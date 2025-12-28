"""Punto de entrada para generar un ejecutable de retornoporestado."""

from __future__ import annotations

import multiprocessing
import sys

from retornoporestado import main


def run() -> None:
    """Ejecuta la lógica principal, compatible con ejecutables."""

    multiprocessing.freeze_support()
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
