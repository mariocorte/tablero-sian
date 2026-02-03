"""Envía la solicitud SOAP ObtenerEstadoNotificacion al servicio del MP.

Uso:
    python soap_notificacion.py --entorno DESA --data '{"UsuarioClave":"...","UsuarioNombre":"...","codigoSeguimiento":"..."}'
    python soap_notificacion.py --entorno PROD --data-file payload.json
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict

import requests


SOAP_ACTION = "http://tempuri.org/ObtenerEstadoNotificacion"
BASE_PATH = "/services/wsNotificacion.asmx"


def _cargar_payload(args: argparse.Namespace) -> Dict[str, Any]:
    if args.data and args.data_file:
        raise ValueError("Solo se puede usar uno de --data o --data-file.")
    if args.data:
        return json.loads(args.data)
    if args.data_file:
        with open(args.data_file, "r", encoding="utf-8") as archivo:
            return json.load(archivo)
    raise ValueError("Debe proveer --data o --data-file.")


def _normalizar_entorno(entorno: str) -> str:
    entorno_normalizado = entorno.strip().upper()
    if entorno_normalizado == "DESA":
        return "pruebasian.mpublico.gov.ar"
    return "sian.mpublico.gov.ar"


def _construir_xml(payload: Dict[str, Any]) -> str:
    usuario_clave = payload.get("UsuarioClave") or payload.get("usuarioclave")
    usuario_nombre = payload.get("UsuarioNombre") or payload.get("usuarioNombre")
    codigo_seguimiento = payload.get("codigoSeguimiento") or payload.get("codseg")

    if not usuario_clave or not usuario_nombre or not codigo_seguimiento:
        raise ValueError(
            "El JSON debe incluir UsuarioClave, UsuarioNombre y codigoSeguimiento."
        )

    return (
        '<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" '
        'xmlns:tem="http://tempuri.org/">'
        "<soapenv:Header>"
        "   <tem:Authentication>"
        "      <tem:UsuarioClave>"
        f"{usuario_clave}"
        "</tem:UsuarioClave>"
        "      <tem:UsuarioNombre>"
        f"{usuario_nombre}"
        "</tem:UsuarioNombre>"
        "   </tem:Authentication>"
        "</soapenv:Header>"
        "<soapenv:Body>"
        "  <tem:ObtenerEstadoNotificacion>"
        "     <tem:codigoSeguimiento>"
        f"{codigo_seguimiento}"
        "</tem:codigoSeguimiento>"
        "  </tem:ObtenerEstadoNotificacion>"
        "</soapenv:Body>"
        "</soapenv:Envelope>"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consulta el estado de una notificación vía SOAP."
    )
    parser.add_argument(
        "--entorno",
        required=True,
        help="Entorno de ejecución. Use DESA para pruebas, cualquier otro valor para producción.",
    )
    parser.add_argument("--data", help="JSON inline con UsuarioClave, UsuarioNombre y codigoSeguimiento.")
    parser.add_argument(
        "--data-file",
        dest="data_file",
        help="Ruta a un archivo JSON con UsuarioClave, UsuarioNombre y codigoSeguimiento.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Timeout en segundos para la solicitud HTTP.",
    )
    args = parser.parse_args()

    try:
        payload = _cargar_payload(args)
        xml_body = _construir_xml(payload)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"Error en los parámetros: {exc}", file=sys.stderr)
        return 2

    host = _normalizar_entorno(args.entorno)
    url = f"https://{host}{BASE_PATH}"

    headers = {
        "Content-Type": "text/xml;charset=UTF-8",
        "SOAPAction": SOAP_ACTION,
    }

    try:
        response = requests.post(url, headers=headers, data=xml_body.strip(), timeout=args.timeout)
    except requests.RequestException as exc:
        print(f"Error HTTP: {exc}", file=sys.stderr)
        return 1

    print(f"Status: {response.status_code}")
    print(response.text)
    return 0 if response.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
