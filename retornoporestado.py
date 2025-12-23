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


@contextlib.contextmanager
def _silenciar_salida_consola() -> Iterable[None]:
    """Suprime temporalmente stdout y stderr para evitar ruido en consola."""

    buffer_stdout = io.StringIO()
    buffer_stderr = io.StringIO()
    with contextlib.redirect_stdout(buffer_stdout), contextlib.redirect_stderr(buffer_stderr):
        yield


def _obtener_notificaciones_por_estado(
    conn_pg: psycopg2.extensions.connection, estado_objetivo: str
) -> List[NotificacionPendiente]:
    """Devuelve las notificaciones cuyo último estado coincide con el objetivo."""

    consulta = """
        SELECT
            codigo_seguimiento,
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            notpolhistoricompfecha,
            notpolhistoricompestado
        FROM (
            SELECT DISTINCT ON (TRIM(codigoseguimientomp))
                TRIM(codigoseguimientomp) AS codigo_seguimiento,
                pmovimientoid,
                pactuacionid,
                pdomicilioelectronicopj,
                notpolhistoricompfecha,
                notpolhistoricompestado
            FROM notpolhistoricomp
            WHERE codigoseguimientomp IS NOT NULL
              AND TRIM(codigoseguimientomp) <> ''
            ORDER BY TRIM(codigoseguimientomp), notpolhistoricompfecha DESC NULLS LAST
        ) AS ultimos
        WHERE notpolhistoricompfecha::date < CURRENT_DATE
    """

    with conn_pg.cursor(cursor_factory=extras.DictCursor) as cursor:
        cursor.execute(consulta)
        filas = cursor.fetchall()

    notificaciones: List[NotificacionPendiente] = []
    estado_normalizado = estado_objetivo.strip().lower()
    for fila in filas:
        estado_ultimo = (fila["notpolhistoricompestado"] or "").strip().lower()
        if estado_ultimo != estado_normalizado:
            continue

        codigo = (fila["codigo_seguimiento"] or "").strip()
        if not codigo:
            continue

        notificaciones.append(
            NotificacionPendiente(
                codigo_seguimiento=codigo,
                pmovimientoid=int(fila["pmovimientoid"]),
                pactuacionid=int(fila["pactuacionid"]),
                pdomicilioelectronicopj=str(fila["pdomicilioelectronicopj"]),
                fecha_ultimo_estado=fila["notpolhistoricompfecha"],
            )
        )

    return notificaciones


def _contar_por_estado(conn_pg: psycopg2.extensions.connection, estado_objetivo: str) -> int:
    """Cuenta cuántos registros tienen como último estado el valor indicado."""

    consulta = """
        SELECT COUNT(*) AS total
        FROM (
            SELECT DISTINCT ON (TRIM(codigoseguimientomp))
                TRIM(codigoseguimientomp) AS codigo_seguimiento,
                COALESCE(notpolhistoricompestado, '') AS estado,
                notpolhistoricompfecha
            FROM notpolhistoricomp
            WHERE codigoseguimientomp IS NOT NULL
              AND TRIM(codigoseguimientomp) <> ''
            ORDER BY TRIM(codigoseguimientomp), notpolhistoricompfecha DESC NULLS LAST
        ) AS ultimos
        WHERE LOWER(estado) = LOWER(%s)
          AND notpolhistoricompfecha::date < CURRENT_DATE;
    """

    with conn_pg.cursor() as cursor:
        cursor.execute(consulta, (estado_objetivo.strip(),))
        resultado = cursor.fetchone()

    return int(resultado[0]) if resultado else 0


def _procesar_notificacion(
    conn_panel: psycopg2.extensions.connection,
    notificacion: NotificacionPendiente,
    usar_test: bool,
) -> Optional[retornoxmlmp.ResultadoSOAP]:
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
        return

    if resultado is None:
        _log_step(
            "procesar_por_estado",
            "ADVERTENCIA",
            f"Sin resultado para {notificacion.codigo_seguimiento}",
        )
        return

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
        return

    with _silenciar_salida_consola():
        historialsian.pre_historial(codigodeseguimientomp=notificacion.codigo_seguimiento)
    _log_step(
        "procesar_por_estado",
        "OK",
        f"Actualización completada para {notificacion.codigo_seguimiento}",
    )

    return resultado


def _imprimir_resultado_en_consola(
    notificacion: NotificacionPendiente, resultado: Optional[retornoxmlmp.ResultadoSOAP]
) -> None:
    """Muestra únicamente el código de seguimiento y el XML formateado."""

    if resultado is None:
        return

    formateador = getattr(retornoxmlmp, "_formatear_xml_legible", None)
    if formateador is None:
        from xml.dom import minidom

        def formateador(xml_texto: str) -> str:  # type: ignore[redefinition]
            try:
                documento = minidom.parseString(xml_texto)
                pretty = documento.toprettyxml(indent="  ")
                lineas = [linea for linea in pretty.splitlines() if linea.strip()]
                return "\n".join(lineas)
            except Exception:
                return xml_texto

    xml_legible = formateador(resultado.xml_respuesta)
    print(f"CODIGODESEGUIMIENTOMP: {notificacion.codigo_seguimiento}")
    print(f"XML amigable:\n{xml_legible}\n")


def procesar_por_estado(
    estado_objetivo: str,
    usar_test: Optional[bool] = None,
) -> None:
    """Ejecuta el flujo de actualización filtrando por el último estado."""

    estado_normalizado = estado_objetivo.strip()
    if not estado_normalizado:
        raise ValueError("El parámetro 'estado_objetivo' no puede estar vacío")

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

        print(f"Total de registros a procesar: {len(notificaciones)}")

        if not notificaciones:
            _log_step(
                "procesar_por_estado",
                "OK",
                f"No se encontraron notificaciones con estado '{estado_normalizado}'",
            )
            return

        for notificacion in notificaciones:
            resultado = _procesar_notificacion(conn_panel, notificacion, bandera_test)
            _imprimir_resultado_en_consola(notificacion, resultado)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Actualiza los retornos del Ministerio Público filtrando por el "
            "último estado registrado en notpolhistoricomp."
        )
    )
    parser.add_argument(
        "--estado",
        required=True,
        help=(
            "Estado exacto a filtrar (se compara con el último estado de "
            "cada código en notpolhistoricomp)"
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
