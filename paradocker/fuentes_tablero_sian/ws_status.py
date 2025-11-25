"""Herramientas para validar el estado de los web services usados por el tablero SIAN.

Ejemplo de uso desde la línea de comandos:
    python ws_status.py --test
"""

import argparse
import json
from typing import Dict, Iterable, List

import requests


def webservice_endpoints(test: bool) -> Iterable[Dict[str, str]]:
    """Devuelve los endpoints de los web services según entorno.

    Args:
        test: Si es ``True`` devuelve las URLs de prueba; en caso contrario las de producción.
    """

    convertidor_url = (
        "https://appweb.justiciasalta.gov.ar:8091/testnotisian/api/cnotpolicia/convertirNotifPoliciaaPDF"
        if test
        else "https://appweb.justiciasalta.gov.ar:8091/policia/api/cnotpolicia/convertirNotifPoliciaaPDF"
    )

    gestor_url = (
        "http://10.19.240.200:8080/gestor/API/gestiondocumento/InsertarDocumento"
        if test
        else "https://appintra.justiciasalta.gov.ar:8092/gestor/API/gestiondocumento/InsertarDocumento"
    )

    incrustar_qr_url = "https://appweb.justiciasalta.gov.ar:8091/policia/api/cnotpolicia/incrustarqrpdf"

    yield {"nombre": "Convertidor a PDF", "url": convertidor_url}
    yield {"nombre": "InsertarDocumento Gestor", "url": gestor_url}
    yield {"nombre": "Incrustar QR en PDF", "url": incrustar_qr_url}


def verificar_webservice(url: str, method: str = "HEAD", timeout: int = 5) -> Dict[str, str]:
    """Ejecuta una llamada rápida para validar la disponibilidad de un web service."""

    try:
        response = requests.request(method, url, timeout=timeout)
        estado = "up" if response.ok else "error"
        detalle = f"{response.status_code} {response.reason}"
    except Exception as exc:  # pylint: disable=broad-except
        estado = "error"
        detalle = str(exc)

    return {"url": url, "estado": estado, "detalle": detalle}


def chequear_todos_los_webservices(test: bool, method: str = "HEAD", timeout: int = 5) -> List[Dict[str, str]]:
    """Obtiene el estado de todos los web services configurados para el entorno indicado."""

    resultados: List[Dict[str, str]] = []
    for endpoint in webservice_endpoints(test):
        resultado = verificar_webservice(endpoint["url"], method=method, timeout=timeout)
        resultado["nombre"] = endpoint["nombre"]
        resultados.append(resultado)
    return resultados


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verifica el estado de los web services del tablero SIAN"
    )
    parser.add_argument(
        "--test", action="store_true", help="Usar endpoints de test en lugar de producción"
    )
    parser.add_argument(
        "--method",
        default="HEAD",
        help="Método HTTP a usar para la verificación (HEAD por defecto)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=5,
        help="Tiempo de espera en segundos para cada request",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Imprime el resultado en formato JSON (útil para scripts)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    resultados = chequear_todos_los_webservices(
        test=args.test, method=args.method, timeout=args.timeout
    )

    if args.json:
        print(json.dumps(resultados, indent=2, ensure_ascii=False))
    else:
        entorno = "TEST" if args.test else "PRODUCCIÓN"
        print(f"Estado de web services ({entorno}):")
        for resultado in resultados:
            nombre = resultado.get("nombre", "")
            estado = resultado.get("estado", "desconocido")
            detalle = resultado.get("detalle", "")
            print(f"- {nombre}: {estado} ({detalle})")


if __name__ == "__main__":
    main()
