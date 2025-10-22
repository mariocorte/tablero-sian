# Copia de fuentes Tablero SIAN

Esta carpeta contiene una **copia congelada** de los archivos indispensables para ejecutar el Tablero SIAN sin acceder al repositorio privado. Incluye los scripts principales de sincronización, las dependencias Python, plantillas HTML y los SQL de apoyo.

> **Importante:** si se realizan cambios en los archivos originales del repositorio, actualizar también esta copia para mantenerla sincronizada.

## Contenido

- `app.py`, `retornoxmlmp.py`, `historialsian.py`, `resumen.py`: scripts Python principales.
- `requirements.txt`: dependencias de ejecución.
- `templates/index.html`: plantilla del formulario web.
- `analisisfinnotif.sql`, `set_laststate.sql`, `ultimoestado.sql`: consultas SQL auxiliares.
- `resumen.txt`: descripción de alto nivel del proyecto.

Con este paquete, el destinatario puede ejecutar los procesos o empaquetarlos en Docker sin acceder al repositorio original.
