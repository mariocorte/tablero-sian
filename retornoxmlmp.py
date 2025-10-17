"""Procesador incremental de respuestas XML almacenadas en ``retornomp``."""
from __future__ import annotations

import contextlib
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import xml.etree.ElementTree as ET

import psycopg2
from psycopg2 import extras, sql

from historialsian import _log_step, panel_config, pgsql_config


@dataclass
class RegistroRetorno:
    """Representa un registro pendiente en la tabla ``retornomp``."""

    pk_field: str
    pk_value: int
    id_enviocedula: Optional[int]
    codigo_seguimiento: Optional[str]
    xml_payload: str


@dataclass
class DatosEnvio:
    """Datos necesarios de ``enviocedulanotificacionpolicia``."""

    id_enviocedula: int
    pmovimientoid: float
    pactuacionid: float
    pdomicilioelectronicopj: str
    codigoseguimientomp: str


@dataclass
class EstadoNotificacion:
    estado_id: Optional[int]
    estado: Optional[str]
    fecha: Optional[datetime]
    fecha_raw: Optional[str]
    observaciones: Optional[str]
    motivo: Optional[str]
    responsable: Optional[str]
    dependencia: Optional[str]
    archivo_id: Optional[str]
    archivo_nombre: Optional[str]


def _obtener_primer_valor(
    fila: Dict[str, object], candidatos: Sequence[str]
) -> Optional[object]:
    for candidato in candidatos:
        if candidato in fila and fila[candidato] is not None:
            return fila[candidato]
    return None


def _extraer_registro_retorno(row: extras.DictRow) -> RegistroRetorno:
    fila = dict(row)
    pk_field = row.cursor.description[0].name  # campo de la primera columna
    pk_value = _obtener_primer_valor(fila, (pk_field, "id", "idretornomp"))
    if pk_value is None:
        raise ValueError("No se pudo determinar la clave primaria del registro retornomp")

    xml_payload = _obtener_primer_valor(
        fila,
        (
            "xml",
            "xml_respuesta",
            "xmlrespuesta",
            "respuesta",
            "respuesta_xml",
            "payload",
        ),
    )
    if not xml_payload:
        raise ValueError("El registro retornomp no contiene un XML de respuesta")

    id_enviocedula = _obtener_primer_valor(
        fila,
        (
            "id_enviocedula",
            "idenviocedula",
            "idenviocedulanotificacion",
            "idenviocedulanotificacionpolicia",
            "idenviocedulaext",
        ),
    )

    codigo_seguimiento = _obtener_primer_valor(
        fila,
        ("codigoseguimientomp", "codigo_seguimiento", "seguimiento", "codigo"),
    )

    return RegistroRetorno(
        pk_field=str(pk_field),
        pk_value=int(pk_value),
        id_enviocedula=int(id_enviocedula) if id_enviocedula is not None else None,
        codigo_seguimiento=(str(codigo_seguimiento).strip() if codigo_seguimiento else None),
        xml_payload=str(xml_payload),
    )


def _obtener_registros_pendientes(
    conn_panel: psycopg2.extensions.connection,
) -> List[RegistroRetorno]:
    with conn_panel.cursor(cursor_factory=extras.DictCursor) as cursor:
        cursor.execute(
            "SELECT * FROM retornomp WHERE COALESCE(procesado, FALSE) = FALSE ORDER BY 1"
        )
        filas = cursor.fetchall()

    registros: List[RegistroRetorno] = []
    for fila in filas:
        try:
            registros.append(_extraer_registro_retorno(fila))
        except Exception as exc:  # pragma: no cover - logging defensivo
            _log_step("_obtener_registros_pendientes", "ERROR", str(exc))
    return registros


def _obtener_texto(
    node: ET.Element, tag: str, namespaces: Dict[str, str]
) -> Optional[str]:
    elemento = node.find(f"temp:{tag}", namespaces)
    if elemento is None or elemento.text is None:
        return None
    texto = elemento.text.strip()
    return texto or None


def _parsear_fecha(fecha_str: Optional[str]) -> Optional[datetime]:
    if not fecha_str:
        return None
    fecha = fecha_str.strip()
    if not fecha:
        return None
    fecha_normalizada = fecha.replace("Z", "+00:00")
    formatos = (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    )
    for formato in formatos:
        with contextlib.suppress(ValueError):
            return datetime.strptime(fecha_normalizada, formato)
    with contextlib.suppress(ValueError):
        return datetime.fromisoformat(fecha_normalizada)
    return None


def _parsear_estados(xml_payload: str) -> List[EstadoNotificacion]:
    root = ET.fromstring(xml_payload)
    namespaces = {
        "soap": "http://schemas.xmlsoap.org/soap/envelope/",
        "temp": "http://tempuri.org/",
    }
    estados_xml = root.findall(".//temp:HistorialEstados/temp:EstadoNotificacion", namespaces)

    estados: List[EstadoNotificacion] = []
    for estado_xml in estados_xml:
        estado_id = _obtener_texto(estado_xml, "EstadoNotificacionId", namespaces)
        fecha_raw = _obtener_texto(estado_xml, "Fecha", namespaces)
        estados.append(
            EstadoNotificacion(
                estado_id=int(estado_id) if estado_id else None,
                estado=_obtener_texto(estado_xml, "Estado", namespaces),
                fecha=_parsear_fecha(fecha_raw),
                fecha_raw=fecha_raw,
                observaciones=_obtener_texto(estado_xml, "Observaciones", namespaces),
                motivo=_obtener_texto(estado_xml, "Motivo", namespaces),
                responsable=_obtener_texto(estado_xml, "ResponsableNotificacion", namespaces),
                dependencia=_obtener_texto(estado_xml, "DependenciaNotificacion", namespaces),
                archivo_id=_obtener_texto(estado_xml, "ArchivoId", namespaces),
                archivo_nombre=_obtener_texto(estado_xml, "ArchivoNombre", namespaces),
            )
        )
    return estados


def _obtener_datos_envio(
    conn_pg: psycopg2.extensions.connection,
    id_enviocedula: Optional[int],
    codigo_seguimiento: Optional[str],
) -> Optional[DatosEnvio]:
    columnas_id = (
        "idenviocedulanotificacionpolicia",
        "idenviocedulanotificacion",
        "idenviocedula",
    )
    query_base = sql.SQL(
        """
        SELECT
            {id_col} AS id_enviocedula,
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            codigoseguimientomp
        FROM enviocedulanotificacionpolicia
        WHERE {id_col} = %s
        """
    )

    with conn_pg.cursor(cursor_factory=extras.DictCursor) as cursor:
        if id_enviocedula is not None:
            for columna in columnas_id:
                cursor.execute(
                    query_base.format(id_col=sql.Identifier(columna)), (id_enviocedula,)
                )
                fila = cursor.fetchone()
                if fila:
                    return DatosEnvio(
                        id_enviocedula=int(fila["id_enviocedula"]),
                        pmovimientoid=float(fila["pmovimientoid"]),
                        pactuacionid=float(fila["pactuacionid"]),
                        pdomicilioelectronicopj=str(fila["pdomicilioelectronicopj"]),
                        codigoseguimientomp=str(fila["codigoseguimientomp"]),
                    )
        if codigo_seguimiento:
            cursor.execute(
                """
                SELECT
                    idenviocedulanotificacionpolicia AS id_enviocedula,
                    pmovimientoid,
                    pactuacionid,
                    pdomicilioelectronicopj,
                    codigoseguimientomp
                FROM enviocedulanotificacionpolicia
                WHERE codigoseguimientomp = %s
                """,
                (codigo_seguimiento,),
            )
            fila = cursor.fetchone()
            if fila:
                return DatosEnvio(
                    id_enviocedula=int(fila["id_enviocedula"]),
                    pmovimientoid=float(fila["pmovimientoid"]),
                    pactuacionid=float(fila["pactuacionid"]),
                    pdomicilioelectronicopj=str(fila["pdomicilioelectronicopj"]),
                    codigoseguimientomp=str(fila["codigoseguimientomp"]),
                )
    return None


def _obtener_claves_existentes(
    conn_pg: psycopg2.extensions.connection,
    datos_envio: DatosEnvio,
) -> set[Tuple[Optional[int], Optional[datetime]]]:
    with conn_pg.cursor() as cursor:
        cursor.execute(
            """
            SELECT notpolhistoricompestadonid, notpolhistoricompfecha
            FROM notpolhistoricomp
            WHERE pmovimientoid = %s
              AND pactuacionid = %s
              AND pdomicilioelectronicopj = %s
              AND codigoseguimientomp = %s
            """,
            (
                datos_envio.pmovimientoid,
                datos_envio.pactuacionid,
                datos_envio.pdomicilioelectronicopj,
                datos_envio.codigoseguimientomp,
            ),
        )
        return {
            (
                fila[0],
                fila[1].replace(tzinfo=None) if isinstance(fila[1], datetime) else fila[1],
            )
            for fila in cursor.fetchall()
        }


def _insertar_estados_nuevos(
    conn_pg: psycopg2.extensions.connection,
    datos_envio: DatosEnvio,
    estados: Iterable[EstadoNotificacion],
) -> int:
    existentes = _obtener_claves_existentes(conn_pg, datos_envio)
    nuevos = [
        estado
        for estado in estados
        if (
            estado.estado_id,
            estado.fecha.replace(tzinfo=None) if estado.fecha else None,
        )
        not in existentes
    ]

    if not nuevos:
        return 0

    insert_query = """
        INSERT INTO notpolhistoricomp (
            notpolhistoricomparchivoid,
            notpolhistoricompestadonid,
            notpolhistoricompfecha,
            notpolhistoricompestado,
            notpolhistoricompobservaciones,
            notpolhistoricompmotivo,
            notpolhistoricompresponsable,
            notpolhistoricompdependencia,
            notpolhistoricomparchivonombre,
            notpolhistoricomparchcont,
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            codigoseguimientomp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with conn_pg.cursor() as cursor:
        for estado in nuevos:
            cursor.execute(
                insert_query,
                (
                    estado.archivo_id,
                    estado.estado_id,
                    estado.fecha,
                    estado.estado,
                    estado.observaciones,
                    estado.motivo,
                    estado.responsable,
                    estado.dependencia,
                    estado.archivo_nombre,
                    None,
                    datos_envio.pmovimientoid,
                    datos_envio.pactuacionid,
                    datos_envio.pdomicilioelectronicopj,
                    datos_envio.codigoseguimientomp,
                ),
            )
    return len(nuevos)


_ESTADOS_FINALES = {"ENTREGADA", "NO ENTREGADA", "DESCARTADA", "FINALIZADA"}


def _seleccionar_ultimo_estado(
    estados: Sequence[EstadoNotificacion],
) -> Optional[EstadoNotificacion]:
    if not estados:
        return None
    return max(
        enumerate(estados),
        key=lambda item: (
            item[1].fecha or datetime.min,
            item[0],
        ),
    )[1]


def _actualizar_enviocedula(
    conn_pg: psycopg2.extensions.connection,
    datos_envio: DatosEnvio,
    ultimo_estado: EstadoNotificacion,
) -> None:
    estado_texto = (ultimo_estado.estado or "").strip()
    fecha_estado = ultimo_estado.fecha
    finsian = estado_texto.upper() in _ESTADOS_FINALES if estado_texto else False

    with conn_pg.cursor() as cursor:
        cursor.execute(
            """
            UPDATE enviocedulanotificacionpolicia
            SET laststagesian = %s,
                fechalaststate = %s,
                finsian = %s
            WHERE codigoseguimientomp = %s
            """,
            (
                estado_texto,
                fecha_estado,
                finsian,
                datos_envio.codigoseguimientomp,
            ),
        )


def _marcar_procesado(
    conn_panel: psycopg2.extensions.connection,
    registro: RegistroRetorno,
) -> None:
    with conn_panel.cursor() as cursor:
        cursor.execute(
            sql.SQL("UPDATE retornomp SET procesado = TRUE WHERE {pk} = %s").format(
                pk=sql.Identifier(registro.pk_field)
            ),
            (registro.pk_value,),
        )


def procesar_incremental() -> None:
    _log_step("procesar_incremental", "INICIO", "Buscando registros pendientes en retornomp")
    with psycopg2.connect(**panel_config) as conn_panel, psycopg2.connect(**pgsql_config) as conn_pg:
        conn_panel.autocommit = False
        conn_pg.autocommit = False

        registros = _obtener_registros_pendientes(conn_panel)
        if not registros:
            _log_step("procesar_incremental", "OK", "No hay registros pendientes")
            return

        for registro in registros:
            _log_step(
                "procesar_incremental",
                "INICIO",
                f"Procesando retorno {registro.pk_value}",
            )
            try:
                estados = _parsear_estados(registro.xml_payload)
                if not estados:
                    _log_step(
                        "procesar_incremental",
                        "OK",
                        f"Registro {registro.pk_value} sin estados en el XML",
                    )
                    _marcar_procesado(conn_panel, registro)
                    conn_panel.commit()
                    continue

                datos_envio = _obtener_datos_envio(
                    conn_pg,
                    registro.id_enviocedula,
                    registro.codigo_seguimiento,
                )
                if datos_envio is None:
                    raise ValueError(
                        "No se encontraron datos en enviocedulanotificacionpolicia para el retorno",
                    )

                insertados = _insertar_estados_nuevos(conn_pg, datos_envio, estados)
                _log_step(
                    "procesar_incremental",
                    "OK",
                    f"{insertados} estados nuevos insertados para {registro.pk_value}",
                )

                ultimo_estado = _seleccionar_ultimo_estado(estados)
                if ultimo_estado:
                    _actualizar_enviocedula(conn_pg, datos_envio, ultimo_estado)

                _marcar_procesado(conn_panel, registro)
                conn_pg.commit()
                conn_panel.commit()
                _log_step(
                    "procesar_incremental",
                    "OK",
                    f"Registro {registro.pk_value} procesado correctamente",
                )
            except Exception as exc:
                conn_pg.rollback()
                conn_panel.rollback()
                _log_step(
                    "procesar_incremental",
                    "ERROR",
                    f"Error procesando retorno {registro.pk_value}: {exc}",
                )


if __name__ == "__main__":
    procesar_incremental()
