from fastapi import FastAPI
from pydantic import BaseModel, validator
from typing import List


class Persona(BaseModel):
    nombre: str
    apellido: str
    edad: int


class PersonasRequest(BaseModel):
    personas: List[Persona]

    @validator("personas", pre=True)
    def ensure_list(cls, value):
        if value is None:
            return value
        if isinstance(value, dict):
            return [value]
        return value

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
