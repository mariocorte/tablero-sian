# Manual de usuario del ejecutable `retornoporestado`

## Descripción

El ejecutable `retornoporestado` actualiza los retornos del Ministerio Público
filtrando por el **último estado** registrado en la tabla
`notpolhistoricomp`. Para cada coincidencia consulta el servicio SOAP del MP,
almacena el XML en `retornomp` y refresca el historial con `historialsian`.

## Requisitos previos

- Acceso a las bases de datos configuradas en el proyecto (PostgreSQL e Informix).
- Conectividad al servicio SOAP del MP (producción o pruebas).
- Permisos para crear archivos en el directorio de ejecución (se escribe
  `codigos_actualizados.txt` si hubo actualizaciones).

## Ubicación del ejecutable

- Windows: `retornoporestado.exe`
- Linux/macOS (si se genera): `retornoporestado`

Si necesitas regenerarlo, consulta el script `build_retornoporestado_exe.py`.

## Parámetros

Uso general:

```bash
retornoporestado --estado "ESTADO" [--codigoseguimientomp CODIGO] [--test]
```

Parámetros disponibles:

- `--estado` (obligatorio): Estado exacto a filtrar. Se compara contra el último
  estado de cada código en `notpolhistoricomp`.
- `--codigoseguimientomp` (opcional): Restringe la consulta a un único código de
  seguimiento.
- `--test` (opcional): Usa el entorno de pruebas del servicio SOAP.

## Ejemplos

### 1) Procesar todos los códigos con un estado específico

```bash
retornoporestado --estado "NOTIFICADO"
```

### 2) Procesar un único código de seguimiento

```bash
retornoporestado --estado "NOTIFICADO" --codigoseguimientomp "MP-123456"
```

### 3) Ejecutar contra el ambiente de pruebas

```bash
retornoporestado --estado "NOTIFICADO" --test
```

### 4) Combinar filtro de estado y código en pruebas

```bash
retornoporestado --estado "NOTIFICADO" --codigoseguimientomp "MP-123456" --test
```

## Salidas y artefactos

- Consola: informa el progreso de cada notificación (por ejemplo, estado
  previo y estado nuevo detectado).
- Archivo `codigos_actualizados.txt`: lista los códigos de seguimiento que
  resultaron actualizados durante la ejecución.

## Solución de problemas

- **No se encuentran registros**: verifica el estado proporcionado y que exista
  como último estado en `notpolhistoricomp`.
- **Fallas de conexión a BD**: confirma credenciales y conectividad a
  PostgreSQL/Informix.
- **Error con el servicio SOAP**: valida que el endpoint esté disponible y que
  el parámetro `--test` coincida con el entorno requerido.
