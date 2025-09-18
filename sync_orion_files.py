"""Herramientas para sincronizar archivos en Orion.

Este módulo contiene únicamente los fragmentos mínimos necesarios para
demostrar cómo se crea el cliente SFTP respetando la compatibilidad con
versiones antiguas de Paramiko que no soportan el argumento ``encoding``.

El resto del script original no está incluido porque no es relevante para
la corrección solicitada.
"""

from __future__ import annotations

import logging
from typing import Optional

import paramiko

logger = logging.getLogger(__name__)


def _create_sftp_client(
    ssh_client: paramiko.SSHClient, encoding: Optional[str]
) -> paramiko.SFTPClient:
    """Devuelve un ``SFTPClient`` intentando usar ``encoding`` cuando esté disponible.

    Paramiko añadió el parámetro ``encoding`` a ``from_transport`` en
    versiones recientes; las versiones antiguas lanzan ``TypeError`` si se
    les pasa el argumento con nombre.  Para seguir siendo compatibles con
    ambos escenarios, probamos primero con ``encoding`` y, si no está
    soportado, repetimos la llamada sin él.
    """

    transport = ssh_client.get_transport()
    if transport is None:
        raise RuntimeError("La sesión SSH no está conectada.")

    if encoding is None:
        return paramiko.SFTPClient.from_transport(transport)

    try:
        return paramiko.SFTPClient.from_transport(transport, encoding=encoding)
    except TypeError:
        logger.debug(
            "La versión de Paramiko instalada no soporta 'encoding'; se usa "
            "la codificación por defecto."
        )
        sftp = paramiko.SFTPClient.from_transport(transport)
        if encoding.lower() != "utf-8":
            logger.warning(
                "El cliente SFTP no puede utilizar la codificación '%s'; "
                "se continuará con UTF-8.",
                encoding,
            )
        return sftp


__all__ = ["_create_sftp_client"]

