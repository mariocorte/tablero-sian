# Paquete Docker para Tablero SIAN

Este directorio contiene todo lo necesario para que el equipo de Docker construya y publique la imagen contenedora de la aplicación **Tablero SIAN** y programe la ejecución automática del proceso diario a las 20:00 (lunes a domingo).

> **Novedad:** se incluye la carpeta `fuentes_tablero_sian/` con una copia de los archivos esenciales (scripts Python, dependencias, SQL y plantillas) para facilitar el traspaso cuando el destinatario no posee acceso al repositorio original.

## Estructura

- `Dockerfile`: receta de la imagen basada en `python:3.11-slim`, instala dependencias de sistema (OpenJDK para el driver Informix, toolchain para JPype, etc.), dependencias Python y copia el código de la aplicación.
- `entrypoint.sh`: script de entrada que permite lanzar el contenedor en modo servidor (`server`) o modo proceso (`job`).
- `run_job.sh`: script que ejecuta el proceso principal (`python app.py`) utilizando variables de entorno (`TIEMPO`, `TEST_MODE`).
- `env.example`: plantilla de variables de entorno. Debe copiarse a `.env` y ajustarse con los valores definitivos antes de desplegar.
- `docker-compose.yml`: referencia opcional para levantar el servicio (`server`) y para lanzar el proceso como tarea (`job`).
- `lib/.gitkeep`: marcador del directorio donde debe colocarse el driver JDBC de Informix (`ifxjdbc.jar`).

## Preparación previa al build

1. **Driver Informix**: copiar el archivo `ifxjdbc.jar` provisto por infraestructura dentro de `paradocker/lib/`. El Dockerfile lo copiará a `/app/paradocker/lib/ifxjdbc.jar`.
2. **Variables de entorno**:
   - Copiar `paradocker/env.example` a `paradocker/.env`.
   - Revisar y ajustar credenciales, hosts, puertos y parámetros según ambiente (producción o test). Si se dejan en blanco, la aplicación utilizará los valores por defecto definidos en el código.
   - Verificar especialmente `TIEMPO` (segundos entre ciclos cuando se ejecuta el job) y `TEST_MODE` (0 = producción, 1 = test).

## Construcción de la imagen

Ejecutar desde la raíz del repositorio:

```bash
docker build -f paradocker/Dockerfile -t registry.example.com/sian/tablero-sian:latest .
```

> Sustituir `registry.example.com/sian/tablero-sian:latest` por el nombre real de la imagen en el registro corporativo.

Para publicar en el registro:

```bash
docker push registry.example.com/sian/tablero-sian:latest
```

## Ejecución de la imagen

### Modo servidor (interfaz web)

```bash
docker run --rm \
  --env-file paradocker/.env \
  -p 8000:8000 \
  -v $(pwd)/paradocker/lib/ifxjdbc.jar:/app/paradocker/lib/ifxjdbc.jar:ro \
  registry.example.com/sian/tablero-sian:latest server
```

La aplicación quedará disponible en `http://localhost:8000`.

### Modo job (ejecución única)

```bash
docker run --rm \
  --env-file paradocker/.env \
  -v $(pwd)/paradocker/lib/ifxjdbc.jar:/app/paradocker/lib/ifxjdbc.jar:ro \
  registry.example.com/sian/tablero-sian:latest job
```

- Ajustar `TIEMPO` y `TEST_MODE` en el archivo `.env` o pasar parámetros directamente: `docker run ... job --tiempo 300 --test 0`.
- El script `run_job.sh` respeta los parámetros explícitos (`--tiempo`, `--test`) cuando se suministran en la línea de comandos.

### Uso con Docker Compose

```bash
docker compose -f paradocker/docker-compose.yml up --build tablero-sian-web
```

Para ejecutar el job una sola vez utilizando Compose:

```bash
docker compose -f paradocker/docker-compose.yml --profile job run --rm tablero-sian-job
```

## Programación diaria a las 20:00

1. Asegurarse de que la imagen esté disponible en el host donde se ejecutará la tarea programada (pull previo si corresponde).
2. Copiar el archivo `.env` definitivo y `ifxjdbc.jar` al servidor (por ejemplo en `/opt/tablero-sian/`).
3. Crear un script de conveniencia (opcional) o usar directamente `cron`. Ejemplo de entrada en crontab para ejecutar todos los días a las 20:00:

```
0 20 * * * docker run --rm --name tablero-sian-job \
    --env-file /opt/tablero-sian/.env \
    -v /opt/tablero-sian/ifxjdbc.jar:/app/paradocker/lib/ifxjdbc.jar:ro \
    registry.example.com/sian/tablero-sian:latest job \
    >> /var/log/tablero-sian/job.log 2>&1
```

- El comando anterior lanza el contenedor en modo `job`, redirige la salida a `/var/log/tablero-sian/job.log` y elimina el contenedor al finalizar (`--rm`).
- Actualizar el nombre de la imagen y las rutas según la infraestructura real.

### Consideraciones adicionales

- Verificar que el host donde se ejecutará la tarea tenga acceso de red a las bases de datos Informix y PostgreSQL correspondientes.
- El contenedor necesita Java (OpenJDK 17) para que `jaydebeapi` pueda cargar el driver JDBC. El Dockerfile ya instala este requisito.
- Si se requiere un certificado adicional para las conexiones HTTPS, montarlo en el contenedor y ajustar las variables de entorno (`REQUESTS_CA_BUNDLE`, etc.).
- Monitorizar los logs generados por el cron para detectar errores de conexión o credenciales inválidas.

## Variables de entorno clave

| Variable | Descripción |
| --- | --- |
| `TIEMPO` | Intervalo en segundos entre iteraciones cuando se ejecuta el job (por defecto 300). |
| `TEST_MODE` | 0 para producción, 1 para conectarse a los hosts de test definidos en `env.example`. |
| `INFORMIX_JDBC_URL`, `INFORMIX_USERNAME`, `INFORMIX_PASSWORD` | Configuración del driver Informix. |
| `PROD_*` / `TEST_*` | Configuración de las bases PostgreSQL utilizadas por el proceso (principal, panel y IW). |
| `PORT` | Puerto expuesto por uvicorn cuando se ejecuta el modo servidor. |

Con esta información, el equipo de Docker dispone de los archivos y las instrucciones para construir la imagen, publicarla y programar la ejecución diaria del proceso a las 20:00 horas.
