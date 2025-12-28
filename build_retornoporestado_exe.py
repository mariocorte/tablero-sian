"""Genera el ejecutable standalone de retornoporestado."""

from __future__ import annotations

import sys

try:
    import PyInstaller.__main__ as pyinstaller
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "PyInstaller no está instalado. Ejecuta: pip install pyinstaller"
    ) from exc


def main() -> None:
    """Construye el .exe con PyInstaller en modo one-file."""

    pyinstaller.run(
        [
            "--name=retornoporestado",
            "--onefile",
            "--clean",
            "--noconfirm",
            "retornoporestado_exe.py",
        ]
    )


if __name__ == "__main__":
    main()
