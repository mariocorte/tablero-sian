"""Actualiza retornos según el último estado en ``notpolhistoricomp``.

Este módulo busca las notificaciones cuyo último estado registrado en
``notpolhistoricomp`` coincide con el valor indicado por argumento. Para cada
coincidencia se consulta el servicio SOAP del Ministerio Público, almacena el
XML en ``retornomp`` y ejecuta ``historialsian`` para refrescar los estados.
"""

from __future__ import annotations

import argparse
import contextlib
import io
from dataclasses import dataclass
from typing import Iterable, List, Optional
import xml.etree.ElementTree as ET

import psycopg2
from psycopg2 import extras

import historialsian
from historialsian import _log_step, panel_config, pgsql_config, test as default_test_flag
import retornoxmlmp


@dataclass(frozen=True)
class NotificacionPendiente:
    """Representa una notificación filtrada por estado."""

    codigo_seguimiento: str
    pmovimientoid: int
    pactuacionid: int
    pdomicilioelectronicopj: str
    fecha_ultimo_estado: Optional[object]
    estado_ultimo: str
    estado_envio: str
    tiene_archivo: bool


@contextlib.contextmanager
def _silenciar_salida_consola() -> Iterable[None]:
    """Suprime temporalmente stdout y stderr para evitar ruido en consola."""

    buffer_stdout = io.StringIO()
    buffer_stderr = io.StringIO()
    with contextlib.redirect_stdout(buffer_stdout), contextlib.redirect_stderr(buffer_stderr):
        yield


def _obtener_notificaciones_por_estado(
    conn_pg: psycopg2.extensions.connection, estado_objetivo: Optional[str]
) -> List[NotificacionPendiente]:
    """Devuelve las notificaciones filtradas por el último estado cuando aplica."""

    if estado_objetivo:
        consulta = """
            WITH ultimo_estado AS (
                SELECT DISTINCT ON (TRIM(codigoseguimientomp))
                    TRIM(codigoseguimientomp) AS codigoseguimientomp,
                    notpolhistoricompfecha,
                    notpolhistoricompestado,
                    notpolhistoricompestadonid,
                    notpolhistoricomparchivoid
                FROM notpolhistoricomp
                WHERE codigoseguimientomp IS NOT NULL
                  AND TRIM(codigoseguimientomp) <> ''
                ORDER BY TRIM(codigoseguimientomp),
                         to_timestamp(
                             left(replace(notpolhistoricompfecha, 'T', ' '), 19),
                             'YYYY-MM-DD HH24:MI:SS'
                         ) DESC NULLS LAST,
                         notpolhistoricompestadonid DESC NULLS LAST
            )
            SELECT
                TRIM(env.codigoseguimientomp) AS codigo_seguimiento,
                env.pmovimientoid,
                env.pactuacionid,
                env.pdomicilioelectronicopj,
                ultimo_estado.notpolhistoricompfecha,
                ultimo_estado.notpolhistoricompestado,
                ultimo_estado.notpolhistoricompestadonid,
                ultimo_estado.notpolhistoricomparchivoid,
                env.laststagesian
            FROM enviocedulanotificacionpolicia env
            LEFT JOIN ultimo_estado
              ON TRIM(env.codigoseguimientomp) = ultimo_estado.codigoseguimientomp
            WHERE env.codigoseguimientomp IS NOT NULL
              AND TRIM(env.codigoseguimientomp) <> ''
              AND (
                LOWER(COALESCE(ultimo_estado.notpolhistoricompestado, '')) = LOWER(%s)
                OR LOWER(COALESCE(env.laststagesian, '')) = LOWER(%s)
              )
            ORDER BY TRIM(env.codigoseguimientomp)
        """
    else:
        consulta = """
            WITH ultimo_estado AS (
                SELECT DISTINCT ON (TRIM(codigoseguimientomp))
                    TRIM(codigoseguimientomp) AS codigoseguimientomp,
                    notpolhistoricompfecha,
                    notpolhistoricompestado,
                    notpolhistoricompestadonid,
                    notpolhistoricomparchivoid
                FROM notpolhistoricomp
                WHERE codigoseguimientomp IS NOT NULL
                  AND TRIM(codigoseguimientomp) <> ''
                ORDER BY TRIM(codigoseguimientomp),
                         to_timestamp(
                             left(replace(notpolhistoricompfecha, 'T', ' '), 19),
                             'YYYY-MM-DD HH24:MI:SS'
                         ) DESC NULLS LAST,
                         notpolhistoricompestadonid DESC NULLS LAST
            )
            SELECT
                TRIM(env.codigoseguimientomp) AS codigo_seguimiento,
                env.pmovimientoid,
                env.pactuacionid,
                env.pdomicilioelectronicopj,
                ultimo_estado.notpolhistoricompfecha,
                ultimo_estado.notpolhistoricompestado,
                ultimo_estado.notpolhistoricompestadonid,
                ultimo_estado.notpolhistoricomparchivoid,
                env.laststagesian
            FROM enviocedulanotificacionpolicia env
            LEFT JOIN ultimo_estado
              ON TRIM(env.codigoseguimientomp) = ultimo_estado.codigoseguimientomp
            WHERE env.codigoseguimientomp IS NOT NULL
              AND TRIM(env.codigoseguimientomp) <> ''
              AND (
                ultimo_estado.codigoseguimientomp IS NULL
                OR COALESCE(env.laststagesian, '') <> COALESCE(ultimo_estado.notpolhistoricompestado, '')
              )
            ORDER BY TRIM(env.codigoseguimientomp)
        """

    with conn_pg.cursor(cursor_factory=extras.DictCursor) as cursor:
        estado_normalizado = estado_objetivo.strip() if estado_objetivo else None
        if estado_normalizado:
            cursor.execute(consulta, (estado_normalizado, estado_normalizado))
        else:
            cursor.execute(consulta)
        filas = cursor.fetchall()

    notificaciones: List[NotificacionPendiente] = []
    for fila in filas:
        codigo = (fila["codigo_seguimiento"] or "").strip()
        if not codigo:
            continue

        estado_ultimo = (fila["notpolhistoricompestado"] or "").strip()
        estado_envio = (fila["laststagesian"] or "").strip()
        archivo_id = fila["notpolhistoricomparchivoid"]

        notificaciones.append(
            NotificacionPendiente(
                codigo_seguimiento=codigo,
                pmovimientoid=int(fila["pmovimientoid"]),
                pactuacionid=int(fila["pactuacionid"]),
                pdomicilioelectronicopj=str(fila["pdomicilioelectronicopj"]),
                fecha_ultimo_estado=fila["notpolhistoricompfecha"],
                estado_ultimo=estado_ultimo,
                estado_envio=estado_envio,
                tiene_archivo=archivo_id is not None and int(archivo_id) != 0,
            )
        )

    return notificaciones


def _contar_por_estado(
    conn_pg: psycopg2.extensions.connection, estado_objetivo: Optional[str]
) -> int:
    """Cuenta cuántos registros tienen como último estado el valor indicado."""

    consulta = """
        SELECT COUNT(*) AS total
        FROM (
            SELECT DISTINCT ON (TRIM(codigoseguimientomp))
                TRIM(codigoseguimientomp) AS codigo_seguimiento,
                COALESCE(notpolhistoricompestado, '') AS estado,
                notpolhistoricompfecha,
                notpolhistoricomparchivoid
            FROM notpolhistoricomp
            WHERE codigoseguimientomp IS NOT NULL
              AND TRIM(codigoseguimientomp) <> ''
            ORDER BY TRIM(codigoseguimientomp),
                     to_timestamp(
                         left(replace(notpolhistoricompfecha, 'T', ' '), 19),
                         'YYYY-MM-DD HH24:MI:SS'
                     ) DESC NULLS LAST,
                     notpolhistoricompestadonid DESC NULLS LAST
        ) AS ultimos
        WHERE (%s IS NULL OR LOWER(estado) = LOWER(%s))
          AND notpolhistoricompfecha::date < CURRENT_DATE
          AND notpolhistoricomparchivoid IS NOT NULL
          AND notpolhistoricomparchivoid <> 0;
    """

    with conn_pg.cursor() as cursor:
        estado_normalizado = estado_objetivo.strip() if estado_objetivo else None
        cursor.execute(consulta, (estado_normalizado, estado_normalizado))
        resultado = cursor.fetchone()

    return int(resultado[0]) if resultado else 0


def _obtener_ultimo_estado_desde_xml(xml_respuesta: str) -> Optional[str]:
    """Extrae el último estado desde el XML del servicio SOAP."""

    if not xml_respuesta:
        return None

    try:
        root = ET.fromstring(xml_respuesta)
    except ET.ParseError as exc:
        _log_step(
            "procesar_por_estado",
            "ADVERTENCIA",
            f"No se pudo parsear el XML de respuesta: {exc}",
        )
        return None

    namespaces = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "temp": "http://tempuri.org/",
    }
    estados = root.findall(
        ".//temp:HistorialEstados/temp:EstadoNotificacion", namespaces
    )
    if not estados:
        return None

    estados_normalizados = historialsian._normalizar_estados(estados, namespaces)
    if not estados_normalizados:
        return None

    ultimo_estado = historialsian._obtener_estado_mas_reciente(estados_normalizados)
    if ultimo_estado is None:
        return None

    return (ultimo_estado.get("estado") or "").strip()


def _obtener_archivo_id_ultimo_estado(xml_respuesta: str) -> Optional[str]:
    """Extrae el archivo asociado al último estado desde el XML del servicio."""

    if not xml_respuesta:
        return None

    try:
        root = ET.fromstring(xml_respuesta)
    except ET.ParseError as exc:
        _log_step(
            "procesar_por_estado",
            "ADVERTENCIA",
            f"No se pudo parsear el XML de respuesta: {exc}",
        )
        return None

    namespaces = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "temp": "http://tempuri.org/",
    }
    estados = root.findall(
        ".//temp:HistorialEstados/temp:EstadoNotificacion", namespaces
    )
    if not estados:
        return None

    estados_normalizados = historialsian._normalizar_estados(estados, namespaces)
    if not estados_normalizados:
        return None

    ultimo_estado = historialsian._obtener_estado_mas_reciente(estados_normalizados)
    if ultimo_estado is None:
        return None

    archivo_id = (ultimo_estado.get("archivo_id") or "").strip()
    if not archivo_id or archivo_id == "0":
        return None
    return archivo_id


def _obtener_datos_archivo(
    conn_pg: psycopg2.extensions.connection, codigo_seguimiento: str
) -> Optional[str]:
    """Obtiene los datos de archivo desde las tablas asociadas al código."""

    consulta_envio = """
        SELECT ecedarchivoseguimientodatos
        FROM enviocedulanotificacionpolicia
        WHERE TRIM(codigoseguimientomp) = TRIM(%s)
        LIMIT 1
    """
    consulta_historial = """
        SELECT notpolhistoricomparchcont
        FROM notpolhistoricomp
        WHERE TRIM(codigoseguimientomp) = TRIM(%s)
          AND notpolhistoricomparchivoid IS NOT NULL
          AND notpolhistoricomparchivoid <> 0
        ORDER BY to_timestamp(
            left(replace(notpolhistoricompfecha, 'T', ' '), 19),
            'YYYY-MM-DD HH24:MI:SS'
        ) DESC NULLS LAST
        LIMIT 1
    """

    with conn_pg.cursor() as cursor:
        cursor.execute(consulta_envio, (codigo_seguimiento,))
        resultado_envio = cursor.fetchone()
        if resultado_envio and resultado_envio[0]:
            return str(resultado_envio[0])

        cursor.execute(consulta_historial, (codigo_seguimiento,))
        resultado_historial = cursor.fetchone()
        if resultado_historial and resultado_historial[0]:
            return str(resultado_historial[0])

    return None


def _procesar_notificacion(
    conn_pg: psycopg2.extensions.connection,
    conn_panel: psycopg2.extensions.connection,
    notificacion: NotificacionPendiente,
    estado_objetivo: Optional[str],
    usar_test: bool,
) -> tuple[Optional[retornoxmlmp.ResultadoSOAP], bool, Optional[str], Optional[str]]:
    """Invoca el servicio SOAP y dispara la actualización del historial."""

    _log_step(
        "procesar_por_estado",
        "INICIO",
        (
            f"Consultando {notificacion.codigo_seguimiento} "
            f"(estado actual: {notificacion.fecha_ultimo_estado})"
        ),
    )

    with _silenciar_salida_consola():
        try:
            resultado, mensaje_error = retornoxmlmp._invocar_servicio(
                notificacion.codigo_seguimiento,
                usar_test,
                mostrar_respuesta=False,
            )
        except TypeError:
            _log_step(
                "procesar_por_estado",
                "ADVERTENCIA",
                (
                    "retornoxmlmp._invocar_servicio no acepta el parámetro "
                    "'mostrar_respuesta'; se usa la llamada compatible."
                ),
            )
            resultado, mensaje_error = retornoxmlmp._invocar_servicio(
                notificacion.codigo_seguimiento,
                usar_test,
            )

    if mensaje_error:
        _log_step(
            "procesar_por_estado",
            "ERROR",
            mensaje_error,
        )
        return None, False, None, None

    if resultado is None:
        _log_step(
            "procesar_por_estado",
            "ADVERTENCIA",
            f"Sin resultado para {notificacion.codigo_seguimiento}",
        )
        return None, False, None, None

    ultimo_estado_xml = _obtener_ultimo_estado_desde_xml(resultado.xml_respuesta)
    archivo_id_xml = _obtener_archivo_id_ultimo_estado(resultado.xml_respuesta)
    tiene_archivo_xml = archivo_id_xml is not None

    envio = retornoxmlmp.EnvioNotificacion(
        id_envio=0,
        pmovimientoid=notificacion.pmovimientoid,
        pactuacionid=notificacion.pactuacionid,
        pdomicilioelectronicopj=notificacion.pdomicilioelectronicopj,
        codigoseguimientomp=notificacion.codigo_seguimiento,
    )

    try:
        retornoxmlmp._almacenar_xml(
            conn_panel,
            envio,
            resultado.xml_respuesta,
        )
    except Exception as exc:  # pragma: no cover - dependiente de la base real
        conn_panel.rollback()
        _log_step(
            "procesar_por_estado",
            "ERROR",
            f"{notificacion.codigo_seguimiento}: no se pudo almacenar el XML: {exc}",
        )
        return None, False, None, ultimo_estado_xml

    estado_objetivo_norm = (estado_objetivo or "").strip().lower()
    estado_xml_norm = (ultimo_estado_xml or "").strip().lower()
    estado_hist_norm = (notificacion.estado_ultimo or "").strip().lower()
    estado_env_norm = (notificacion.estado_envio or "").strip().lower()

    requiere_actualizacion = estado_objetivo is None
    if estado_xml_norm:
        if estado_objetivo_norm and estado_xml_norm != estado_objetivo_norm:
            requiere_actualizacion = True
        if estado_xml_norm != estado_hist_norm:
            requiere_actualizacion = True
        if estado_xml_norm != estado_env_norm:
            requiere_actualizacion = True
    if tiene_archivo_xml and not notificacion.tiene_archivo:
        requiere_actualizacion = True

    if requiere_actualizacion:
        with _silenciar_salida_consola():
            historialsian.pre_historial(
                codigodeseguimientomp=notificacion.codigo_seguimiento
            )

    archivo_datos: Optional[str] = None
    archivo_actualizado = False
    if notificacion.tiene_archivo or tiene_archivo_xml:
        try:
            archivo_actualizado = retornoxmlmp._actualizar_datos_archivo(
                conn_pg,
                envio,
                resultado.xml_respuesta,
                usar_test,
            )
        except Exception as exc:  # pragma: no cover - dependiente de la base real
            conn_pg.rollback()
            _log_step(
                "procesar_por_estado",
                "ERROR",
                (
                    f"{notificacion.codigo_seguimiento}: "
                    f"no se pudo actualizar archivo: {exc}"
                ),
            )
            return None, False, None, ultimo_estado_xml
        archivo_datos = _obtener_datos_archivo(conn_pg, notificacion.codigo_seguimiento)
    _log_step(
        "procesar_por_estado",
        "OK",
        f"Actualización completada para {notificacion.codigo_seguimiento}",
    )

    return resultado, requiere_actualizacion or archivo_actualizado, archivo_datos, ultimo_estado_xml


def _imprimir_resultado_en_consola(
    notificacion: NotificacionPendiente,
    datos_archivo: Optional[str],
    ultimo_estado_xml: Optional[str],
) -> None:
    """Muestra los datos clave solicitados en consola."""

    archivo_texto = datos_archivo or ""
    estado_nuevo = (ultimo_estado_xml or "").strip()
    print(
        "codigoseguimientomp={codigo}, notpolhistoricompestado={estado}, "
        "estado_anterior_laststage={laststage}, estado_nuevo={estado_nuevo}, "
        "archivo={archivo}".format(
            codigo=notificacion.codigo_seguimiento,
            estado=notificacion.estado_ultimo,
            laststage=notificacion.estado_envio,
            estado_nuevo=estado_nuevo,
            archivo=archivo_texto,
        )
    )


def procesar_por_estado(
    estado_objetivo: Optional[str],
    usar_test: Optional[bool] = None,
) -> None:
    """Ejecuta el flujo de actualización filtrando por el último estado."""

    estado_normalizado = estado_objetivo.strip() if estado_objetivo else None

    bandera_test = default_test_flag if usar_test is None else usar_test

    with psycopg2.connect(**pgsql_config) as conn_pg, psycopg2.connect(
        **panel_config
    ) as conn_panel:
        conn_pg.autocommit = False
        conn_panel.autocommit = False

        notificaciones = _obtener_notificaciones_por_estado(
            conn_pg,
            estado_normalizado,
        )

        if not notificaciones:
            _log_step(
                "procesar_por_estado",
                "OK",
                (
                    "No se encontraron notificaciones "
                    f"con estado '{estado_normalizado}'"
                    if estado_normalizado
                    else "No se encontraron notificaciones para procesar"
                ),
            )
            return

        codigos_actualizados: List[str] = []
        for notificacion in notificaciones:
            resultado, actualizado, archivo_datos, ultimo_estado_xml = _procesar_notificacion(
                conn_pg,
                conn_panel,
                notificacion,
                estado_normalizado,
                bandera_test,
            )
            if resultado is not None:
                _imprimir_resultado_en_consola(
                    notificacion,
                    archivo_datos,
                    ultimo_estado_xml,
                )
            if actualizado:
                codigos_actualizados.append(notificacion.codigo_seguimiento)

        if codigos_actualizados:
            with open("codigos_actualizados.txt", "w", encoding="utf-8") as archivo:
                archivo.write("\n".join(codigos_actualizados))


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Actualiza los retornos del Ministerio Público filtrando por el "
            "último estado registrado en notpolhistoricomp."
        )
    )
    parser.add_argument(
        "--estado",
        required=False,
        help=(
            "Estado exacto a filtrar (se compara con el último estado de "
            "cada código en notpolhistoricomp). Si se omite, procesa todos "
            "los estados."
        ),
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Usa el entorno de pruebas del servicio SOAP",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)
    procesar_por_estado(args.estado, usar_test=args.test)


if __name__ == "__main__":
    main()
