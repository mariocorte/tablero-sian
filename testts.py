from fastapi import FastAPI
from pydantic import BaseModel


class Persona(BaseModel):
    nombre: str
    apellido: str
    edad: int


class PersonasRequest(BaseModel):
    personas: list[Persona]

app = FastAPI()


@app.post("/test")
def test(payload: PersonasRequest):
    cantidad_personas = len(payload.personas)
    promedio_edad = (
        sum(persona.edad for persona in payload.personas) / cantidad_personas
        if cantidad_personas
        else 0
    )
    return {
        "cantidad_personas": cantidad_personas,
        "promedio_edad": promedio_edad,
    }
