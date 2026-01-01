import argparse
import psycopg2
from datetime import datetime, date
import xml.etree.ElementTree as ET
from typing import Optional, Union, Iterable, Dict, Any, List, Tuple
from collections import defaultdict


class SummaryCollector:
    """Helper class to collect processing statistics for the execution."""

    _acciones = ("agregados", "modificados", "ignorados")

    def __init__(self) -> None:
        self._datos = defaultdict(lambda: {accion: 0 for accion in self._acciones})
        self._errores: List[str] = []

    def add(self, tabla: str, accion: str, cantidad: int = 1) -> None:
        accion_normalizada = accion.lower()
        if cantidad is None or cantidad <= 0:
            return
        if accion_normalizada not in self._acciones:
            raise ValueError(f"Acción desconocida: {accion}")
        self._datos[tabla][accion_normalizada] += cantidad

    def add_error(self, contexto: str, mensaje: str = "") -> None:
        detalle = contexto if not mensaje else f"{contexto}: {mensaje}"
        self._errores.append(detalle)

    def imprimir(self) -> None:
        lineas = ["Resumen de procesamiento:"]
        if self._datos:
            for tabla in sorted(self._datos):
                estadisticas = self._datos[tabla]
                lineas.append(
                    "- {tabla}: agregados={agregados}, modificados={modificados}, "
                    "ignorados={ignorados}".format(tabla=tabla, **estadisticas)
                )
        else:
            lineas.append("- Sin cambios registrados.")

        if self._errores:
            lineas.append("Errores:")
            for error in self._errores:
                lineas.append(f"  - {error}")
        else:
            lineas.append("Errores: ninguno")

        print("\n".join(lineas))


SUMMARY = SummaryCollector()


def _log_step(func_name: str, status: str, message: str = "") -> None:
    """Collect errors to include them in the final summary."""

    if status.upper() == "ERROR":
        SUMMARY.add_error(func_name, message)

test = False


if test: 

    pgsql_config = {
        "host": "10.18.250.251",
        "port": "5432",
        "database": "iurixPj",
        "user": "cmayuda",
        "password": "power177"
    }



    # Parámetros de conexión PostgreSQL
    panel_config = {
        "host": "10.18.250.250",
        "port": "5432",
        "database": "panelnotificacionesws",
        "user": "usrsian",
        "password": "A8d%4pXq"
    }
else:
    pgsql_config = {
        "host": "10.18.250.250",
        "port": "5432",
        "database": "iurixPj",
        "user": "cmayuda",
        "password": "power177"
    }



    # Parámetros de conexión PostgreSQL
    panel_config = {
        "host": "10.18.250.250",
        "port": "5432",
        "database": "panelnotificacionesws",
        "user": "usrsian",
        "password": "A8d%4pXq"
    }

def lasstage(
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
    fecha_ultima_estado=None,
    xml_respuesta: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], int, int]:
    # Obtiene los estados desde el XML almacenado en retornomp.
    _log_step(
        "lasstage",
        "INICIO",
        f"Procesando seguimiento {CODIGO_SEGUIMIENTO} (mov: {pmovimientoid}, act: {pactuacionid})",
    )

    if not xml_respuesta:
        _log_step("lasstage", "ADVERTENCIA", "XML vacío o inexistente")
        return None, 0, 0

    try:
        root = ET.fromstring(xml_respuesta)
    except ET.ParseError as exc:
        _log_step("lasstage", "ERROR", f"XML inválido: {exc}")
        return None, 0, 0

    namespaces = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "temp": "http://tempuri.org/",
    }

    estados = root.findall(
        ".//temp:HistorialEstados/temp:EstadoNotificacion", namespaces
    )

    estados_normalizados = _normalizar_estados(estados, namespaces)
    total_estados = len(estados_normalizados)
    ultimo_estado = _obtener_estado_mas_reciente(estados_normalizados)

    estados_filtrados = _filtrar_estados_nuevos(
        estados_normalizados, fecha_ultima_estado
    )

    if not estados_filtrados:
        _log_step(
            "lasstage",
            "OK",
            "Sin estados nuevos para registrar",
        )
        return ultimo_estado, total_estados, 0

    insertados = _guardar_historial_notpol(
        estados_filtrados,
        pmovimientoid,
        pactuacionid,
        pdomicilioelectronicopj,
        CODIGO_SEGUIMIENTO,
    )

    _log_step(
        "lasstage",
        "OK",
        f"Estados obtenidos correctamente para {CODIGO_SEGUIMIENTO}",
    )
    return ultimo_estado, total_estados, insertados


def pre_historial(codigodeseguimientomp: Optional[str] = None):
    _log_step("pre_historial", "INICIO", "Preparando actualización de registros")
    codigo_filtrado = (codigodeseguimientomp or "").strip() or None
    try:
        with psycopg2.connect(**pgsql_config) as conexion_pg:
            with conexion_pg.cursor() as cursor_pg:
                cursor_pg.execute(
                    "update enviocedulanotificacionpolicia set finsian = True "
                    "where descartada = True and finsian <> True"
                )
                SUMMARY.add(
                    "enviocedulanotificacionpolicia", "modificados", cursor_pg.rowcount
                )

                cursor_pg.execute(
                    "update enviocedulanotificacionpolicia set descartada= false, "
                    "laststagesian = 'Sin info', fechalaststate = CURRENT_TIMESTAMP "
                    "where penviocedulanotificacionfechahora >= current_date - INTERVAL '1 days' "
                    "and coalesce(descartada,false) = false"
                )
                SUMMARY.add(
                    "enviocedulanotificacionpolicia", "modificados", cursor_pg.rowcount
                )

            conexion_pg.commit()

            _log_step(
                "pre_historial",
                "OK",
                "Se omite el procesamiento de retornomp por configuración.",
            )
            return
    except Exception as e:
        _log_step(
            "pre_historial",
            "ERROR",
            f"Error en la conexión o consulta SQL: {e}",
        )



def llamar_his_mp(
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
    fecha_ultima_estado,
    xml_contenido,
) -> bool:
    _log_step(
        "llamar_his_mp",
        "INICIO",
        f"Obteniendo historial para {CODIGO_SEGUIMIENTO}",
    )
    try:
        ultimo_estado, total_estados, insertados = lasstage(
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            CODIGO_SEGUIMIENTO,
            fecha_ultima_estado,
            xml_contenido,
        )

        if ultimo_estado is None:
            _log_step(
                "llamar_his_mp",
                "OK",
                (
                    f"Sin estados en el XML para {CODIGO_SEGUIMIENTO}; "
                    "se registra 'Sin info' en el envío"
                ),
            )
            return grabar_historico(
                "Sin info",
                None,
                pmovimientoid,
                pactuacionid,
                pdomicilioelectronicopj,
                CODIGO_SEGUIMIENTO,
            )

        estado = (ultimo_estado.get("estado") or "").strip() or "Sin info"
        fecha_estado = ultimo_estado.get("fecha_raw")
        _log_step(
            "llamar_his_mp",
            "OK",
            f"Total estados analizados: {total_estados}, nuevos insertados: {insertados}",
        )

        exito_guardado = grabar_historico(
            estado,
            fecha_estado,
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            CODIGO_SEGUIMIENTO,
        )
    except Exception as e:
        _log_step("llamar_his_mp", "ERROR", f"Error procesando XML: {e}")
        return False
    else:
        if exito_guardado:
            _log_step(
                "llamar_his_mp",
                "OK",
                f"Historial actualizado para {CODIGO_SEGUIMIENTO}",
            )
            return True
        _log_step(
            "llamar_his_mp",
            "ERROR",
            f"Historial no pudo actualizarse para {CODIGO_SEGUIMIENTO}",
        )
        return False



def _obtener_identificadores_por_codigo(
    conexion_pg: psycopg2.extensions.connection,
    codigo_filtrado: str,
) -> List[Tuple[Any, Any, Any]]:
    """Obtiene las claves primarias asociadas a un código de seguimiento."""

    consulta = """
        SELECT pmovimientoid, pactuacionid, pdomicilioelectronicopj
        FROM enviocedulanotificacionpolicia
        WHERE TRIM(codigoseguimientomp) = %s
    """

    with conexion_pg.cursor() as cursor:
        cursor.execute(consulta, (codigo_filtrado,))
        return cursor.fetchall()


def _obtener_datos_envio(
    conexion_pg: psycopg2.extensions.connection,
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
) -> Optional[Tuple[str, Optional[datetime]]]:
    consulta = """
        SELECT codigoseguimientomp, fechalaststate
        FROM enviocedulanotificacionpolicia
        WHERE pmovimientoid = %s
          AND pactuacionid = %s
          AND pdomicilioelectronicopj = %s
          AND codigoseguimientomp IS NOT NULL
          AND TRIM(codigoseguimientomp) <> ''
          AND UPPER(TRIM(codigoseguimientomp)) <> 'NONE'
        LIMIT 1
    """

    with conexion_pg.cursor() as cursor:
        cursor.execute(
            consulta,
            (pmovimientoid, pactuacionid, pdomicilioelectronicopj),
        )
        fila = cursor.fetchone()

    if not fila:
        return None
    return fila[0], fila[1]


def _obtener_fecha_historial(
    conexion_pg: psycopg2.extensions.connection,
    codigo_seguimiento: str,
) -> Optional[datetime]:
    """Obtiene la última fecha registrada en notpolhistoricomp para el código."""

    consulta = """
        SELECT to_timestamp(
            left(replace(notpolhistoricompfecha, 'T', ' '), 19),
            'YYYY-MM-DD HH24:MI:SS'
        )
        FROM notpolhistoricomp
        WHERE codigoseguimientomp IS NOT NULL
          AND TRIM(codigoseguimientomp) = TRIM(%s)
        ORDER BY to_timestamp(
            left(replace(notpolhistoricompfecha, 'T', ' '), 19),
            'YYYY-MM-DD HH24:MI:SS'
        ) DESC NULLS LAST
        LIMIT 1
    """

    with conexion_pg.cursor() as cursor:
        cursor.execute(consulta, (codigo_seguimiento,))
        fila = cursor.fetchone()

    if not fila:
        return None
    return fila[0]


def _marcar_retornomp_procesado(
    conexion_panel: psycopg2.extensions.connection,
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
) -> None:
    with conexion_panel.cursor() as cursor:
        cursor.execute(
            """
            UPDATE retornomp
            SET procesado = TRUE,
                fechaproceso = NOW()
            WHERE pmovimientoid = %s
              AND pactuacionid = %s
              AND pdomicilioelectronicopj = %s
            """,
            (pmovimientoid, pactuacionid, pdomicilioelectronicopj),
        )
        SUMMARY.add("retornomp", "modificados", cursor.rowcount)
    conexion_panel.commit()



def _formatear_fecha_estado(fecha_estado: Optional[Union[str, datetime]]) -> str:
    """Devuelve una representación legible de la fecha del estado."""

    if isinstance(fecha_estado, datetime):
        return fecha_estado.strftime("%d/%m/%Y %H:%M:%S")

    if isinstance(fecha_estado, str):
        fecha = fecha_estado.strip()
        if not fecha:
            return "Sin fecha"

        fecha_normalizada = fecha.replace("Z", "+00:00")
        try:
            fecha_dt = datetime.fromisoformat(fecha_normalizada)
            return fecha_dt.strftime("%d/%m/%Y %H:%M:%S")
        except ValueError:
            formatos = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ]
            for formato in formatos:
                try:
                    fecha_dt = datetime.strptime(fecha, formato)
                    return fecha_dt.strftime("%d/%m/%Y %H:%M:%S")
                except ValueError:
                    continue
            return fecha

    return "Sin fecha"


def _estado_finalizado(estado_texto: str) -> bool:
    return estado_texto.upper() in (
        "ENTREGADA",
        "NO ENTREGADA",
        "DESCARTADA",
        "FINALIZADA",
    )


def _actualizar_envio_por_codigo(
    cursor: psycopg2.extensions.cursor,
    estado_texto: str,
    fecha_estado: Optional[Union[str, datetime]],
    codigo_seguimiento: str,
) -> int:
    update_query = """
        UPDATE enviocedulanotificacionpolicia
        SET laststagesian = %s,
            fechalaststate = %s,
            finsian = %s
        WHERE TRIM(codigoseguimientomp) = TRIM(%s)
    """
    cursor.execute(
        update_query,
        (estado_texto, fecha_estado, _estado_finalizado(estado_texto), codigo_seguimiento),
    )
    SUMMARY.add(
        "enviocedulanotificacionpolicia",
        "modificados",
        cursor.rowcount,
    )
    return cursor.rowcount


def _obtener_ultimo_estado_notpolhistoricomp(
    cursor: psycopg2.extensions.cursor,
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    codigo_seguimiento: str,
) -> Optional[Tuple[str, Optional[datetime]]]:
    consulta = """
        SELECT notpolhistoricompestado,
               to_timestamp(
                   left(replace(notpolhistoricompfecha, 'T', ' '), 19),
                   'YYYY-MM-DD HH24:MI:SS'
               )
        FROM notpolhistoricomp
        WHERE pmovimientoid = %s
          AND pactuacionid = %s
          AND pdomicilioelectronicopj = %s
          AND TRIM(codigoseguimientomp) = TRIM(%s)
        ORDER BY notpolhistoricompestadonid DESC NULLS LAST,
                 to_timestamp(
                     left(replace(notpolhistoricompfecha, 'T', ' '), 19),
                     'YYYY-MM-DD HH24:MI:SS'
                 ) DESC NULLS LAST
        LIMIT 1
    """
    cursor.execute(
        consulta,
        (pmovimientoid, pactuacionid, pdomicilioelectronicopj, codigo_seguimiento),
    )
    fila = cursor.fetchone()
    if not fila:
        return None
    return fila[0], fila[1]


def _actualizar_envio_con_ultimo_estado(
    cursor: psycopg2.extensions.cursor,
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    codigo_seguimiento: str,
) -> int:
    ultimo_estado = _obtener_ultimo_estado_notpolhistoricomp(
        cursor,
        pmovimientoid,
        pactuacionid,
        pdomicilioelectronicopj,
        codigo_seguimiento,
    )
    if not ultimo_estado:
        return 0

    estado_texto, fecha_estado = ultimo_estado
    update_query = """
        UPDATE enviocedulanotificacionpolicia
        SET laststagesian = %s,
            fechalaststate = %s,
            finsian = %s
        WHERE pmovimientoid = %s
          AND pactuacionid = %s
          AND pdomicilioelectronicopj = %s
          AND TRIM(codigoseguimientomp) = TRIM(%s)
    """
    cursor.execute(
        update_query,
        (
            estado_texto,
            fecha_estado,
            _estado_finalizado(estado_texto or ""),
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            codigo_seguimiento,
        ),
    )
    SUMMARY.add(
        "enviocedulanotificacionpolicia",
        "modificados",
        cursor.rowcount,
    )
    return cursor.rowcount


def _obtener_estado_mas_reciente(
    estados: Iterable[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    estados_lista = list(estados)
    if not estados_lista:
        return None

    def _clave_estado(estado: Dict[str, Any]) -> datetime:
        fecha = _normalizar_fecha_para_comparacion(
            estado.get("fecha") or estado.get("fecha_raw")
        )
        return fecha or datetime.min

    return max(estados_lista, key=_clave_estado)


def grabar_historico(
    estado,
    fecha_estado,
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
):
    _log_step(
        "grabar_historico",
        "INICIO",
        f"Actualizando registro {CODIGO_SEGUIMIENTO}",
    )
    try:
        with psycopg2.connect(**pgsql_config) as conexion:
            with conexion.cursor() as cursor:
                estado_texto = estado or ""
                fecha_estado_texto = _formatear_fecha_estado(fecha_estado)
                _actualizar_envio_por_codigo(
                    cursor,
                    estado_texto,
                    fecha_estado,
                    CODIGO_SEGUIMIENTO,
                )
                _actualizar_envio_con_ultimo_estado(
                    cursor,
                    pmovimientoid,
                    pactuacionid,
                    pdomicilioelectronicopj,
                    CODIGO_SEGUIMIENTO,
                )
                conexion.commit()
                _log_step(
                    "grabar_historico",
                    "OK",
                    f"Registro {CODIGO_SEGUIMIENTO} actualizado ({estado_texto})",
                )
                return True

    except psycopg2.Error as e:
        _log_step(
            "grabar_historico",
            "ERROR",
            f"Error al actualizar la base de datos: {e}",
        )
    return False


def _normalizar_estados(
    estados: Iterable[ET.Element], namespaces: Dict[str, str]
) -> List[Dict[str, Any]]:
    _log_step("_normalizar_estados", "INICIO", "Normalizando estados recibidos")
    def obtener_texto(node: ET.Element, tag: str) -> Optional[str]:
        elemento = node.find(f"temp:{tag}", namespaces)
        if elemento is None:
            return None
        if elemento.text is None:
            return None
        texto = elemento.text.strip()
        return texto if texto else None

    estados_normalizados = []

    for estado_node in estados:
        estado_id = obtener_texto(estado_node, "EstadoNotificacionId")
        fecha_raw = obtener_texto(estado_node, "Fecha")
        estados_normalizados.append(
            {
                "estado_id": int(estado_id) if estado_id else None,
                "fecha": _parsear_fecha_estado_bd(fecha_raw),
                "fecha_raw": fecha_raw,
                "estado": obtener_texto(estado_node, "Estado"),
                "observaciones": obtener_texto(estado_node, "Observaciones"),
                "motivo": obtener_texto(estado_node, "Motivo"),
                "responsable": obtener_texto(
                    estado_node, "ResponsableNotificacion"
                ),
                "dependencia": obtener_texto(
                    estado_node, "DependenciaNotificacion"
                ),
                "archivo_id": obtener_texto(estado_node, "ArchivoId"),
                "archivo_nombre": obtener_texto(estado_node, "ArchivoNombre"),
            }
        )

    _log_step(
        "_normalizar_estados",
        "OK",
        f"Total estados normalizados: {len(estados_normalizados)}",
    )
    return estados_normalizados


def _normalizar_fecha_para_comparacion(
    fecha: Optional[Union[datetime, date, str]]
) -> Optional[datetime]:
    if fecha is None:
        return None

    if isinstance(fecha, str):
        fecha_texto = fecha.strip()
        if not fecha_texto:
            return None

        fecha_parseada = _parsear_fecha_estado_bd(fecha_texto)
        if fecha_parseada is None:
            try:
                fecha_parseada = datetime.fromisoformat(
                    fecha_texto.replace("Z", "+00:00")
                )
            except ValueError:
                return None
        fecha = fecha_parseada

    if isinstance(fecha, date) and not isinstance(fecha, datetime):
        fecha = datetime.combine(fecha, datetime.min.time())

    if fecha.tzinfo is not None:
        return fecha.replace(tzinfo=None)

    return fecha


def _filtrar_estados_nuevos(
    estados: Iterable[Dict[str, Any]],
    fecha_ultima_estado: Optional[datetime],
) -> List[Dict[str, Any]]:
    fecha_ultima_normalizada = _normalizar_fecha_para_comparacion(fecha_ultima_estado)
    estados_filtrados: List[Dict[str, Any]] = []

    for estado in estados:
        fecha_estado = estado.get("fecha")
        fecha_estado_normalizada = _normalizar_fecha_para_comparacion(fecha_estado)

        if (
            fecha_ultima_normalizada is None
            or fecha_estado_normalizada is None
            or fecha_estado_normalizada >= fecha_ultima_normalizada
        ):
            estados_filtrados.append(estado)

    return estados_filtrados


def _construir_clave_estado(
    estado_id: Optional[int],
    fecha: Optional[Union[datetime, date]],
    estado_texto: Optional[str],
) -> Tuple[Optional[int], Optional[datetime], str]:
    estado_norm = int(estado_id) if estado_id is not None else None
    fecha_norm = _normalizar_fecha_para_comparacion(fecha)
    texto_norm = (estado_texto or '').strip().upper()
    return estado_norm, fecha_norm, texto_norm


def _obtener_claves_estados_existentes(
    cursor: psycopg2.extensions.cursor,
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
) -> set:
    consulta = """
        SELECT notpolhistoricompestadonid,
               notpolhistoricompfecha,
               notpolhistoricompestado
        FROM notpolhistoricomp
        WHERE pmovimientoid = %s
          AND pactuacionid = %s
          AND pdomicilioelectronicopj = %s
          AND codigoseguimientomp = %s
    """
    cursor.execute(
        consulta,
        (pmovimientoid, pactuacionid, pdomicilioelectronicopj, CODIGO_SEGUIMIENTO),
    )
    existentes = set()
    for estado_id, fecha, estado_texto in cursor.fetchall():
        existentes.add(_construir_clave_estado(estado_id, fecha, estado_texto))
    return existentes


def _guardar_historial_notpol(
    estados: Iterable[Dict[str, Any]],
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
) -> int:
    if not isinstance(estados, list):
        estados = list(estados)

    if not estados:
        _log_step(
            "_guardar_historial_notpol",
            "OK",
            f"Sin estados para registrar en {CODIGO_SEGUIMIENTO}",
        )
        return 0

    insert_query = """INSERT INTO notpolhistoricomp \
        (notpolhistoricomparchivoid, notpolhistoricomparchivonombre, notpolhistoricompestadonid, \
        notpolhistoricomparchcont, notpolhistoricompfecha, notpolhistoricompestado, \
        notpolhistoricompobservaciones, notpolhistoricompmotivo, notpolhistoricompresponsable, \
        notpolhistoricompdependencia, pmovimientoid, pactuacionid, pdomicilioelectronicopj, \
        codigoseguimientomp) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
        ON CONFLICT DO NOTHING;"""

    try:
        with psycopg2.connect(**pgsql_config) as conexion:
            with conexion.cursor() as cursor:
                total_estados = len(estados)
                _log_step(
                    "_guardar_historial_notpol",
                    "INICIO",
                    f"Preparando inserción de {total_estados} estados para {CODIGO_SEGUIMIENTO}",
                )

                claves_existentes = _obtener_claves_estados_existentes(
                    cursor,
                    pmovimientoid,
                    pactuacionid,
                    pdomicilioelectronicopj,
                    CODIGO_SEGUIMIENTO,
                )

                insertados = 0
                ignorados = 0
                for estado in estados:
                    clave = _construir_clave_estado(
                        estado.get('estado_id'),
                        estado.get('fecha'),
                        estado.get('estado'),
                    )
                    if clave in claves_existentes:
                        ignorados += 1
                        continue

                    valores = (
                        estado.get('archivo_id'),
                        estado.get('archivo_nombre'),
                        estado.get('estado_id'),
                        None,
                        estado.get('fecha'),
                        estado.get('estado'),
                        estado.get('observaciones'),
                        estado.get('motivo'),
                        estado.get('responsable'),
                        estado.get('dependencia'),
                        pmovimientoid,
                        pactuacionid,
                        pdomicilioelectronicopj,
                        CODIGO_SEGUIMIENTO,
                    )
                    cursor.execute(insert_query, valores)
                    if cursor.rowcount:
                        insertados += 1
                        claves_existentes.add(clave)

            conexion.commit()
            if insertados:
                SUMMARY.add("notpolhistoricomp", "agregados", insertados)
            if ignorados:
                SUMMARY.add("notpolhistoricomp", "ignorados", ignorados)
            _log_step(
                "_guardar_historial_notpol",
                "OK",
                f"Historial guardado para {CODIGO_SEGUIMIENTO}. Nuevos registros: {insertados}",
            )
            return insertados
    except psycopg2.Error as e:
        _log_step(
            "_guardar_historial_notpol",
            "ERROR",
            f"Error al insertar historial notificación en panel: {e}",
        )
        return 0



def _parsear_fecha_estado_bd(fecha_estado: Optional[str]) -> Optional[datetime]:
    if not fecha_estado:
        return None

    fecha = fecha_estado.strip()
    if not fecha:
        return None

    fecha_normalizada = fecha.replace("Z", "+00:00")

    formatos = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]

    for formato in formatos:
        try:
            return datetime.strptime(fecha_normalizada, formato)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(fecha_normalizada)
    except ValueError:
        return None

def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Procesa los retornos del Ministerio Público y actualiza el historial"
            " en SIAN."
        )
    )
    parser.add_argument(
        "--codigodeseguimientomp",
        type=str,
        help=(
            "Procesa únicamente el retorno correspondiente al código de "
            "seguimiento indicado."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)
    pre_historial(codigodeseguimientomp=args.codigodeseguimientomp)
    SUMMARY.imprimir()


if __name__ == "__main__":
    main()
    #ejecutar_control_cedulas()
    #ejecutar_control_historial()
    #registrarproceso(connpanel,'paso4')
    #pre_historial()


   


 #ejecutar_envio_cedulas(
            #     cantidad=100,  # Ajustar el valor según sea necesario
            #     urlsian='sian.mpublico.gov.ar',
            #     pmovimientoid=datos_grilla['pmovimientoid'],
            #     pactuacionid=datos_grilla['pactuacionid'],
            #     pdomicilioelectronicopj=datos_grilla['pdomicilioelectronicopj'],
            #     urlpj='https://appweb.justiciasalta.gov.ar:8091/policia/api/cnotpolicia/enviarnotificacionsian'
            #)

#sian.mpublico.gov.ar




# Parsear el archivo XML


# Recorrer el XML
