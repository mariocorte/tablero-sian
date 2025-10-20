"""Sincroniza en ``retornomp`` las respuestas XML del Ministerio Público.

Este módulo consulta los registros de ``enviocedulanotificacionpolicia`` que no
están descartados, ejecuta la operación ``ObtenerEstadoNotificacion`` del
servicio SOAP del Ministerio y almacena el XML completo obtenido en la tabla
``retornomp``. Si el registro ya existe, se actualiza únicamente cuando el
contenido difiere del almacenado.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple

import psycopg2
from psycopg2 import extras
import requests
import urllib3

from historialsian import _log_step, panel_config, pgsql_config, test as default_test_flag

# Evita advertencias cuando se deshabilita la verificación del certificado.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SOAP_ACTION = "http://tempuri.org/ObtenerEstadoNotificacion"
SOAP_NAMESPACE = "http://tempuri.org/"
SOAP_ENVELOPE = "http://schemas.xmlsoap.org/soap/envelope/"

USUARIO_CLAVE = "NES7u'FR>]e:3)D"
USUARIO_NOMBRE = "wsPoderJudicial"


@dataclass(frozen=True)
class EnvioNotificacion:
    """Representa un envío de ``enviocedulanotificacionpolicia``."""

    id_envio: int
    pmovimientoid: int
    pactuacionid: int
    pdomicilioelectronicopj: str
    codigoseguimientomp: str


@dataclass(frozen=True)
class ResultadoSOAP:
    """Contiene el XML completo devuelto por el servicio SOAP."""

    codigo_seguimiento: str
    xml_respuesta: str


@dataclass(frozen=True)
class IteracionConsulta:
    """Agrupa los parámetros para una iteración de consulta."""

    descripcion: str
    estados: Tuple[str, ...]
    min_dias: Optional[int] = None
    max_dias: Optional[int] = None
    incluir_estados_vacios: bool = False


ITERACIONES: Tuple[IteracionConsulta, ...] = (
    IteracionConsulta(
        descripcion="Pendiente/Ingresada, fechalaststate <= 10 días",
        estados=("Pendiente", "Ingresada"),
        max_dias=10,
        incluir_estados_vacios=True,
    ),
    IteracionConsulta(
        descripcion="En Dep. Policial/Enviada, fechalaststate <= 10 días",
        estados=("En Dep. Policial", "Enviada"),
        max_dias=10,
    ),
    IteracionConsulta(
        descripcion="En Notificaciones/Entregada/No entregada, fechalaststate <= 10 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        max_dias=10,
    ),
    IteracionConsulta(
        descripcion="Rectificación entregada/Rectificación No Entregada, fechalaststate <= 10 días",
        estados=("Rectificación entregada", "Rectificación No Entregada"),
        max_dias=10,
    ),
    IteracionConsulta(
        descripcion="Pendiente/Ingresada, 10 < fechalaststate <= 20 días",
        estados=("Pendiente", "Ingresada"),
        min_dias=10,
        max_dias=20,
        incluir_estados_vacios=True,
    ),
    IteracionConsulta(
        descripcion="En Dep. Policial/Enviada, 10 < fechalaststate <= 20 días",
        estados=("En Dep. Policial", "Enviada"),
        min_dias=10,
        max_dias=20,
    ),
    IteracionConsulta(
        descripcion="En Notificaciones/Entregada/No entregada, 10 < fechalaststate <= 20 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        min_dias=10,
        max_dias=20,
    ),
    IteracionConsulta(
        descripcion="Rectificación entregada/Rectificación No Entregada, 10 < fechalaststate <= 20 días",
        estados=("Rectificación entregada", "Rectificación No Entregada"),
        min_dias=10,
        max_dias=20,
    ),
    IteracionConsulta(
        descripcion="Pendiente/Ingresada, fechalaststate > 20 días",
        estados=("Pendiente", "Ingresada"),
        min_dias=20,
        incluir_estados_vacios=True,
    ),
    IteracionConsulta(
        descripcion="En Dep. Policial/Enviada, fechalaststate > 20 días",
        estados=("En Dep. Policial", "Enviada"),
        min_dias=20,
    ),
    IteracionConsulta(
        descripcion="En Notificaciones/Entregada/No entregada, fechalaststate > 20 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        min_dias=20,
    ),
    IteracionConsulta(
        descripcion="Rectificación entregada/Rectificación No Entregada, fechalaststate > 20 días",
        estados=("Rectificación entregada", "Rectificación No Entregada"),
        min_dias=20,
    ),
)


def _obtener_envios(
    conn_pg: psycopg2.extensions.connection,
    iteracion: IteracionConsulta,
    momento_referencia: datetime,
) -> List[EnvioNotificacion]:
    """Obtiene los envíos a consultar en el servicio SOAP para una iteración."""

    estados_considerados = list(iteracion.estados)
    if iteracion.incluir_estados_vacios:
        estados_considerados.append("")

    comparacion_estados = "laststagesian = ANY(%s)"
    if iteracion.incluir_estados_vacios:
        comparacion_estados = "COALESCE(laststagesian, '') = ANY(%s)"

    params: List[object] = [estados_considerados]
    consulta = f"""
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY penviocedulanotificacionfechahora,
                         pmovimientoid,
                         pactuacionid,
                         pdomicilioelectronicopj
            ) AS id_envio,
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            codigoseguimientomp
        FROM enviocedulanotificacionpolicia
        WHERE COALESCE(descartada, FALSE) = FALSE
          AND COALESCE(laststagesian, '') <> 'Finalizada'
          AND codigoseguimientomp IS NOT NULL
          AND codigoseguimientomp <> ''
          AND {comparacion_estados}
          AND fechalaststate IS NOT NULL
    """

    if iteracion.max_dias is not None:
        fecha_minima = momento_referencia - timedelta(days=iteracion.max_dias)
        consulta += "\n          AND fechalaststate >= %s"
        params.append(fecha_minima)

    if iteracion.min_dias is not None:
        fecha_maxima = momento_referencia - timedelta(days=iteracion.min_dias)
        consulta += "\n          AND fechalaststate < %s"
        params.append(fecha_maxima)

    consulta += """
        ORDER BY penviocedulanotificacionfechahora,
                 pmovimientoid,
                 pactuacionid,
                 pdomicilioelectronicopj
    """

    with conn_pg.cursor(cursor_factory=extras.DictCursor) as cursor:
        cursor.execute(consulta, tuple(params))
        filas = cursor.fetchall()

    envios: List[EnvioNotificacion] = []
    for fila in filas:
        codigo = (fila["codigoseguimientomp"] or "").strip()
        if not codigo:
            continue
        envios.append(
            EnvioNotificacion(
                id_envio=int(fila["id_envio"]),
                pmovimientoid=int(fila["pmovimientoid"]),
                pactuacionid=int(fila["pactuacionid"]),
                pdomicilioelectronicopj=str(fila["pdomicilioelectronicopj"]),
                codigoseguimientomp=codigo,
            )
        )
    return envios


def _construir_xml_peticion(codigo_seguimiento: str) -> str:
    """Genera el envelope SOAP requerido por el servicio."""

    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<soapenv:Envelope xmlns:soapenv=\"{SOAP_ENVELOPE}\" xmlns:tem=\"{SOAP_NAMESPACE}\">
    <soapenv:Header>
        <tem:Authentication>
            <tem:UsuarioClave>{USUARIO_CLAVE}</tem:UsuarioClave>
            <tem:UsuarioNombre>{USUARIO_NOMBRE}</tem:UsuarioNombre>
        </tem:Authentication>
    </soapenv:Header>
    <soapenv:Body>
        <tem:ObtenerEstadoNotificacion>
            <tem:codigoSeguimiento>{codigo_seguimiento}</tem:codigoSeguimiento>
        </tem:ObtenerEstadoNotificacion>
    </soapenv:Body>
</soapenv:Envelope>"""


def _host_soap(usar_test: bool) -> str:
    """Devuelve la URL base del servicio SOAP según el entorno."""

    if usar_test:
        return "https://pruebasian.mpublico.gov.ar"
    return "https://sian.mpublico.gov.ar"


def _invocar_servicio(
    codigo_seguimiento: str,
    usar_test: bool,
    timeout: int = 60,
) -> Optional[ResultadoSOAP]:
    """Invoca el servicio SOAP y retorna el XML completo de la respuesta."""

    url = f"{_host_soap(usar_test)}/services/wsNotificacion.asmx"
    payload = _construir_xml_peticion(codigo_seguimiento)
    headers = {
        "Content-Type": "text/xml; charset=UTF-8",
        "SOAPAction": SOAP_ACTION,
    }

    try:
        respuesta = requests.post(
            url,
            data=payload,
            headers=headers,
            timeout=timeout,
            verify=False,
        )
    except requests.RequestException as exc:
        _log_step(
            "_invocar_servicio",
            "ERROR",
            f"{codigo_seguimiento}: error de red {exc}",
        )
        return None

    if respuesta.status_code != 200:
        _log_step(
            "_invocar_servicio",
            "ERROR",
            f"{codigo_seguimiento}: HTTP {respuesta.status_code} {respuesta.text}",
        )
        return None

    xml_texto = respuesta.text.strip()
    if not xml_texto:
        _log_step(
            "_invocar_servicio",
            "ADVERTENCIA",
            f"{codigo_seguimiento}: respuesta vacía",
        )
        return None

    return ResultadoSOAP(codigo_seguimiento=codigo_seguimiento, xml_respuesta=xml_texto)


def _obtener_xml_actual(
    conn_panel: psycopg2.extensions.connection,
    envio: EnvioNotificacion,
) -> Optional[str]:
    """Recupera el XML almacenado en ``retornomp``."""

    consulta = """
        SELECT contenido_xml
        FROM retornomp
        WHERE pmovimientoid = %s
          AND pactuacionid = %s
          AND pdomicilioelectronicopj = %s
    """

    with conn_panel.cursor() as cursor:
        cursor.execute(
            consulta,
            (
                envio.pmovimientoid,
                envio.pactuacionid,
                envio.pdomicilioelectronicopj,
            ),
        )
        fila = cursor.fetchone()

    if not fila:
        return None

    contenido = fila[0]
    if contenido is None:
        return None
    return str(contenido).strip()


def _almacenar_xml(
    conn_panel: psycopg2.extensions.connection,
    envio: EnvioNotificacion,
    xml_respuesta: str,
) -> str:
    """Inserta o actualiza el XML en ``retornomp`` según corresponda."""

    xml_actual = _obtener_xml_actual(conn_panel, envio)
    xml_nuevo = xml_respuesta.strip()

    if xml_actual is None:
        sentencia = """
            INSERT INTO retornomp (
                pmovimientoid,
                pactuacionid,
                pdomicilioelectronicopj,
                contenido_xml,
                procesado,
                fechaproceso,
                ultactualizacion
            )
            VALUES (%s, %s, %s, %s, FALSE, NULL, NOW())
        """
        with conn_panel.cursor() as cursor:
            cursor.execute(
                sentencia,
                (
                    envio.pmovimientoid,
                    envio.pactuacionid,
                    envio.pdomicilioelectronicopj,
                    xml_nuevo,
                ),
            )
        conn_panel.commit()
        return "insert"

    if xml_actual == xml_nuevo:
        return "sin_cambios"

    sentencia = """
        UPDATE retornomp
        SET contenido_xml = %s,
            ultactualizacion = NOW(),
            procesado = FALSE,
            fechaproceso = NULL
        WHERE pmovimientoid = %s
          AND pactuacionid = %s
          AND pdomicilioelectronicopj = %s
    """
    with conn_panel.cursor() as cursor:
        cursor.execute(
            sentencia,
            (
                xml_nuevo,
                envio.pmovimientoid,
                envio.pactuacionid,
                envio.pdomicilioelectronicopj,
            ),
        )
    conn_panel.commit()
    return "update"


def procesar_envios(usar_test: Optional[bool] = None) -> None:
    """Ejecuta el flujo completo para las iteraciones configuradas."""

    bandera_test = default_test_flag if usar_test is None else usar_test

    momento_referencia = datetime.now()

    with psycopg2.connect(**pgsql_config) as conn_pg, psycopg2.connect(**panel_config) as conn_panel:
        conn_pg.autocommit = False
        conn_panel.autocommit = False

        for iteracion in ITERACIONES:
            envios = _obtener_envios(conn_pg, iteracion, momento_referencia)
            inicio_iteracion = datetime.now()
            cantidad_envios = len(envios)
            mensaje_iteracion = (
                "[procesar_envios] Iteración: {descripcion} | Inicio: {inicio:%Y-%m-%d %H:%M:%S} | "
                "Registros a procesar: {cantidad}"
            ).format(
                descripcion=iteracion.descripcion,
                inicio=inicio_iteracion,
                cantidad=cantidad_envios,
            )
            print(mensaje_iteracion)

            if not envios:
                continue

            for envio in envios:
                resultado = _invocar_servicio(envio.codigoseguimientomp, bandera_test)
                if resultado is None:
                    continue

                _almacenar_xml(conn_panel, envio, resultado.xml_respuesta)


def procesar_periodo(periodo: str, usar_test: Optional[bool] = None) -> None:
    """Compatibilidad hacia atrás: delega en :func:`procesar_envios`."""

    _log_step(
        "procesar_periodo",
        "ADVERTENCIA",
        "El parámetro de período se ignora en la lógica actual",
    )
    procesar_envios(usar_test=usar_test)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    """Analiza los argumentos de línea de comandos."""

    parser = argparse.ArgumentParser(
        description=(
            "Obtiene los estados de notificación desde el servicio SOAP del "
            "Ministerio Público y los almacena en retornomp."
        )
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Utiliza el entorno de pruebas del servicio SOAP",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    """Punto de entrada para ejecución por consola."""

    args = _parse_args(argv)
    procesar_envios(usar_test=args.test)


if __name__ == "__main__":
    main()
