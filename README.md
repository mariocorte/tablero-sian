# Tablero SIAN

Aplicación FastAPI que expone un formulario web para ejecutar los procesos de sincronización utilizados por SIAN.

## Requisitos

* Python 3.10+
* Dependencias del proyecto instaladas (`fastapi`, `uvicorn`, `psycopg2-binary`, `jaydebeapi`, etc.).
* Acceso a las bases de datos Informix y PostgreSQL configuradas en el código.

## Ejecución

1. Instala las dependencias (por ejemplo, con `pip install -r requirements.txt` si existe o instalando los paquetes mencionados).
2. Ejecuta el servidor:

   ```bash
   uvicorn app:app --reload
   ```

3. Abre [http://localhost:8000](http://localhost:8000) en tu navegador y completa el formulario con los parámetros necesarios.

### Ejecución desde la línea de comandos

También puedes lanzar el proceso directamente:

```bash
python app.py --tiempo 60 --test 1
```

Si no se proporcionan los parámetros `--tiempo` y `--test`, el script iniciará el servidor web utilizando los valores por defecto (`host=0.0.0.0`, `port=8000`).
