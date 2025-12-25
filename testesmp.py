"""Prueba el llamado al servicio SOAP usado en retornoporestado.

Ejemplo pensado para pegar el request en SoapUI.
"""

from __future__ import annotations

import argparse
from typing import Optional

import retornoxmlmp


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
