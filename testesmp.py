"""Prueba el llamado al servicio SOAP usado en retornoporestado.

Ejemplo pensado para pegar el request en SoapUI.
"""

from __future__ import annotations

import argparse
from typing import Optional
import xml.etree.ElementTree as ET

import retornoxmlmp

SOAP_ACTION_ARCHIVO = "http://tempuri.org/ObtenerArchivoEstadoNotificacion"


def _imprimir_request(codigo: str, usar_test: bool) -> None:
    url = f"{retornoxmlmp._host_soap(usar_test)}/services/wsNotificacion.asmx"
    xml = retornoxmlmp._construir_xml_peticion(codigo)
    print("=== REQUEST SOAP ===")
    print(f"URL: {url}")
    print("Headers:")
    print("  Content-Type: text/xml; charset=UTF-8")
    print(f"  SOAPAction: {retornoxmlmp.SOAP_ACTION}")
    print("\nBody (XML):")
    print(xml)
    print("=== FIN REQUEST SOAP ===\n")


def _construir_xml_archivo(estado_notificacion_id: str) -> str:
    """Genera el envelope SOAP para obtener el archivo asociado."""

    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<soapenv:Envelope xmlns:soapenv=\"{retornoxmlmp.SOAP_ENVELOPE}\" xmlns:tem=\"{retornoxmlmp.SOAP_NAMESPACE}\">
    <soapenv:Header>
        <tem:Authentication>
            <tem:UsuarioClave>{retornoxmlmp.USUARIO_CLAVE}</tem:UsuarioClave>
            <tem:UsuarioNombre>{retornoxmlmp.USUARIO_NOMBRE}</tem:UsuarioNombre>
        </tem:Authentication>
    </soapenv:Header>
    <soapenv:Body>
        <tem:ObtenerArchivoEstadoNotificacion>
            <tem:estadoNotificacionId>{estado_notificacion_id}</tem:estadoNotificacionId>
        </tem:ObtenerArchivoEstadoNotificacion>
    </soapenv:Body>
</soapenv:Envelope>"""


def _imprimir_request_archivo(estado_notificacion_id: str, usar_test: bool) -> None:
    url = f"{retornoxmlmp._host_soap(usar_test)}/services/wsNotificacion.asmx"
    xml = _construir_xml_archivo(estado_notificacion_id)
    print("=== REQUEST SOAP ARCHIVO ===")
    print(f"URL: {url}")
    print("Headers:")
    print("  Content-Type: text/xml; charset=UTF-8")
    print(f"  SOAPAction: {SOAP_ACTION_ARCHIVO}")
    print("\nBody (XML):")
    print(xml)
    print("=== FIN REQUEST SOAP ARCHIVO ===\n")


def _extraer_referencia_archivo(
    xml_respuesta: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    if not xml_respuesta:
        return None, None, None

    try:
        root = ET.fromstring(xml_respuesta)
    except ET.ParseError:
        return None, None, None

    estado_id = None
    archivo_id = None
    archivo_nombre = None
    for estado in root.findall(
        ".//temp:EstadoNotificacion", retornoxmlmp.XML_NAMESPACES
    ):
        estado_id = _texto_xml(estado, "EstadoNotificacionId")
        archivo_id = _texto_xml(estado, "ArchivoId")
        archivo_nombre = _texto_xml(estado, "ArchivoNombre")

    return estado_id, archivo_id, archivo_nombre


def _texto_xml(nodo: ET.Element, tag: str) -> Optional[str]:
    elemento = nodo.find(f"temp:{tag}", retornoxmlmp.XML_NAMESPACES)
    if elemento is None or elemento.text is None:
        return None
    texto = elemento.text.strip()
    return texto if texto else None


def _invocar_servicio_archivo(
    estado_notificacion_id: str,
    usar_test: bool,
    timeout: int,
    max_reintentos: int,
) -> tuple[Optional[str], Optional[str]]:
    url = f"{retornoxmlmp._host_soap(usar_test)}/services/wsNotificacion.asmx"
    payload = _construir_xml_archivo(estado_notificacion_id)
    headers = {
        "Content-Type": "text/xml; charset=UTF-8",
        "SOAPAction": SOAP_ACTION_ARCHIVO,
    }

    sesion = retornoxmlmp._obtener_sesion_soap(max_reintentos)

    try:
        retornoxmlmp._respetar_intervalo_solicitudes()
        respuesta = sesion.post(
            url,
            data=payload,
            headers=headers,
            timeout=timeout,
        )
    except Exception as exc:
        return None, f"Error de red al solicitar archivo: {exc}"

    if respuesta is None:
        return None, "No se obtuvo respuesta del servicio de archivo."

    if respuesta.status_code != 200:
        return (
            None,
            f"HTTP {respuesta.status_code} al solicitar archivo: {respuesta.text}",
        )

    xml_texto = respuesta.text.strip()
    if not xml_texto:
        return None, "Respuesta vacía del servicio de archivo."

    return xml_texto, None


def ejecutar_prueba(
    codigo_seguimiento: str, usar_test: bool, timeout: int, max_reintentos: int
) -> None:
    _imprimir_request(codigo_seguimiento, usar_test)

    resultado, mensaje_error = retornoxmlmp._invocar_servicio(
        codigo_seguimiento,
        usar_test,
        timeout=timeout,
        max_reintentos=max_reintentos,
        mostrar_respuesta=True,
    )

    print("=== RESPUESTA SOAP (RAW) ===")
    if mensaje_error:
        print(f"ERROR: {mensaje_error}")
        return

    if resultado is None:
        print("Sin resultado devuelto por el servicio.")
        return

    print(resultado.xml_respuesta)
    print("=== FIN RESPUESTA SOAP ===")

    estado_id, archivo_id, archivo_nombre = _extraer_referencia_archivo(
        resultado.xml_respuesta
    )
    if not estado_id:
        print(
            "\nNo se encontró EstadoNotificacionId en la respuesta; "
            "no se invoca ObtenerArchivoEstadoNotificacion."
        )
        return

    print(
        "\n=== REFERENCIA ARCHIVO ===\n"
        f"EstadoNotificacionId: {estado_id}\n"
        f"ArchivoId: {archivo_id or 'N/D'}\n"
        f"ArchivoNombre: {archivo_nombre or 'N/D'}\n"
        "=== FIN REFERENCIA ARCHIVO ===\n"
    )

    _imprimir_request_archivo(estado_id, usar_test)

    respuesta_archivo, error_archivo = _invocar_servicio_archivo(
        estado_id,
        usar_test,
        timeout,
        max_reintentos,
    )

    print("=== RESPUESTA SOAP ARCHIVO (RAW) ===")
    if error_archivo:
        print(f"ERROR: {error_archivo}")
        return

    if respuesta_archivo is None:
        print("Sin resultado devuelto por el servicio de archivo.")
        return

    print(respuesta_archivo)
    print("=== FIN RESPUESTA SOAP ARCHIVO ===")


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Testea el servicio SOAP utilizado por retornoporestado.py "
            "imprimiendo el request completo y la respuesta."
        )
    )
    parser.add_argument(
        "--codigo",
        default="6DU2EI",
        help="Código de seguimiento MP (por defecto: 6DU2EI).",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Usa el entorno de pruebas del servicio SOAP.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="Timeout de la llamada SOAP en segundos (default: 60).",
    )
    parser.add_argument(
        "--reintentos",
        type=int,
        default=3,
        help="Cantidad de reintentos de red (default: 3).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)
    ejecutar_prueba(args.codigo, args.test, args.timeout, args.reintentos)


if __name__ == "__main__":
    main()
