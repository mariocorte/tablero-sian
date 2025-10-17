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
from datetime import datetime
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


def _validar_periodo(periodo: str) -> Tuple[datetime, datetime]:
    """Convierte un período ``YYYYMM`` en el rango datetime correspondiente."""

    if len(periodo) != 6 or not periodo.isdigit():
        raise ValueError("El período debe tener el formato YYYYMM")

    inicio = datetime.strptime(periodo, "%Y%m")
    # Calcula el inicio del mes siguiente para utilizarlo como cota superior.
    if inicio.month == 12:
        fin = datetime(inicio.year + 1, 1, 1)
    else:
        fin = datetime(inicio.year, inicio.month + 1, 1)
    return inicio, fin


def _obtener_envios(
    conn_pg: psycopg2.extensions.connection,
    periodo: str,
) -> List[EnvioNotificacion]:
    """Obtiene los envíos a consultar en el servicio SOAP."""

    fecha_inicio, fecha_fin = _validar_periodo(periodo)
    consulta = """
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
          AND COALESCE(laststage, '') <> 'Finalizada'
          AND codigoseguimientomp IS NOT NULL
          AND codigoseguimientomp <> ''
          AND penviocedulanotificacionfechahora >= %s
          AND penviocedulanotificacionfechahora < %s
        ORDER BY penviocedulanotificacionfechahora,
                 pmovimientoid,
                 pactuacionid,
                 pdomicilioelectronicopj
    """

    with conn_pg.cursor(cursor_factory=extras.DictCursor) as cursor:
        cursor.execute(consulta, (fecha_inicio, fecha_fin))
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
        _log_step(
            "_almacenar_xml",
            "INICIO",
            f"Insertando retorno para {envio.codigoseguimientomp}",
        )
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

    _log_step(
        "_almacenar_xml",
        "INICIO",
        f"Actualizando retorno para {envio.codigoseguimientomp}",
    )
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


def procesar_periodo(periodo: str, usar_test: Optional[bool] = None) -> None:
    """Ejecuta el flujo completo para un período ``YYYYMM``."""

    bandera_test = default_test_flag if usar_test is None else usar_test
    _log_step(
        "procesar_periodo",
        "INICIO",
        f"Procesando período {periodo} (test={bandera_test})",
    )

    with psycopg2.connect(**pgsql_config) as conn_pg, psycopg2.connect(**panel_config) as conn_panel:
        conn_pg.autocommit = False
        conn_panel.autocommit = False

        envios = _obtener_envios(conn_pg, periodo)
        if not envios:
            _log_step("procesar_periodo", "OK", "Sin envíos para procesar")
            return

        _log_step(
            "procesar_periodo",
            "OK",
            f"{len(envios)} envíos a consultar",
        )

        for envio in envios:
            _log_step(
                "procesar_periodo",
                "INICIO",
                (
                    "Consultando servicio para seguimiento "
                    f"{envio.codigoseguimientomp}"
                ),
            )

            resultado = _invocar_servicio(envio.codigoseguimientomp, bandera_test)
            if resultado is None:
                continue

            accion = _almacenar_xml(conn_panel, envio, resultado.xml_respuesta)
            if accion == "insert":
                _log_step(
                    "procesar_periodo",
                    "OK",
                    f"Insertado retorno de {envio.codigoseguimientomp}",
                )
            elif accion == "update":
                _log_step(
                    "procesar_periodo",
                    "OK",
                    f"Actualizado retorno de {envio.codigoseguimientomp}",
                )
            else:
                _log_step(
                    "procesar_periodo",
                    "OK",
                    f"Sin cambios para {envio.codigoseguimientomp}",
                )

    _log_step("procesar_periodo", "FIN", "Proceso completado")


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    """Analiza los argumentos de línea de comandos."""

    parser = argparse.ArgumentParser(
        description=(
            "Obtiene los estados de notificación desde el servicio SOAP del "
            "Ministerio Público y los almacena en retornomp."
        )
    )
    parser.add_argument(
        "periodo",
        help="Período a procesar en formato YYYYMM",
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
    procesar_periodo(args.periodo, usar_test=args.test)


if __name__ == "__main__":
    main()
