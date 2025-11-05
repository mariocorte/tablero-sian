# Manual de uso de `retornoxmlmp.py`

El script `retornoxmlmp.py` sincroniza las respuestas XML del servicio SOAP del Ministerio Público con la tabla `retornomp`. Para ejecutarlo se utiliza Python 3 directamente sobre el archivo y dispone de dos parámetros opcionales que permiten seleccionar el entorno del servicio y acotar el rango de fechas a procesar.

## Sintaxis general

```bash
python retornoxmlmp.py [--test] [--dias N]
```

* `--test`: habilita el modo de pruebas. Cuando el flag no se especifica, el script usa el valor configurado en `historialsian.test`, que por defecto está desactivado y, por lo tanto, se conectará al servicio SOAP de producción.
* `--dias N`: limita la consulta a los envíos cuya columna `fechalaststate` es mayor o igual a la fecha actual menos `N` días. El valor debe ser un entero mayor o igual a cero.

Si no se pasa ningún argumento, `retornoxmlmp.py` utiliza el flujo completo de iteraciones definido en el código (estados y ventanas de tiempo específicas para cada grupo) y consulta el servicio de producción.

## Ejemplos de ejecución

### 1. Ejecución en producción (valores por defecto)

Procesa las iteraciones preconfiguradas contra el servicio SOAP de producción:

```bash
python retornoxmlmp.py
```

### 2. Ejecución en modo de pruebas

Fuerza el uso del entorno de pruebas sin alterar la configuración global:

```bash
python retornoxmlmp.py --test
```

### 3. Procesar únicamente los últimos `N` días

Restringe la consulta a los registros con `fechalaststate` dentro de los últimos 7 días:

```bash
python retornoxmlmp.py --dias 7
```

### 4. Modo de pruebas con límite de días

Es posible combinar ambos parámetros. El siguiente comando consulta el entorno de pruebas y solo procesa las notificaciones de los últimos 3 días:

```bash
python retornoxmlmp.py --test --dias 3
```

## Comportamiento cuando no se especifica `--dias`

Al omitir la opción `--dias`, el script recorre todas las iteraciones definidas en `ITERACIONES`, cada una con su propio conjunto de estados y ventana temporal máxima (por ejemplo, "Pendiente/Ingresada" con hasta 10 días de antigüedad). Esto permite cubrir casos habituales sin necesidad de parámetros adicionales.

## Manejo de valores inválidos para `--dias`

Si se proporciona un número negativo, la ejecución se interrumpe con un error indicando que el parámetro debe ser mayor o igual a cero. Ajuste el valor y vuelva a ejecutar el comando.

## Salida y registros

Durante la ejecución se imprimen mensajes que indican el inicio de cada iteración, la cantidad de registros procesados y los errores que puedan producirse. Además, cada corrida se registra en la base de datos del panel (`ejecproc`) para facilitar el seguimiento.

