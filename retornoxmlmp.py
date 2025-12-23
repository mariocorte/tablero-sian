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
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
import subprocess
import sys
import time
from typing import Iterable, List, Optional, Tuple
from xml.dom import minidom

import psycopg2
from psycopg2 import extras
import requests
from requests.adapters import HTTPAdapter
import urllib3
from urllib3.util.retry import Retry

from historialsian import (
    _log_step,
    panel_config,
    pgsql_config,
    test as default_test_flag,
)

# Evita advertencias cuando se deshabilita la verificación del certificado.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SOAP_ACTION = "http://tempuri.org/ObtenerEstadoNotificacion"
SOAP_NAMESPACE = "http://tempuri.org/"
SOAP_ENVELOPE = "http://schemas.xmlsoap.org/soap/envelope/"

USUARIO_CLAVE = "NES7u'FR>]e:3)D"
USUARIO_NOMBRE = "wsPoderJudicial"

PROCESO_RETORNOMP_ID = 3
MAX_OBSERVACION_LEN = 400

# Intervalo mínimo entre invocaciones consecutivas al servicio SOAP para reducir
# la probabilidad de recibir respuestas ``HTTP 429`` cuando se ejecutan muchos
# requerimientos de manera seguida.
MIN_INTERVALO_SOAP_SEGUNDOS = 1.5


class _SesionSOAP(requests.Session):
    """Sesión configurada con reintentos para el servicio SOAP."""

    def __init__(self, max_reintentos: int) -> None:
        super().__init__()
        reintentos_retry = max(0, max_reintentos - 1)
        retry = Retry(
            total=reintentos_retry,
            read=reintentos_retry,
            connect=reintentos_retry,
            status=reintentos_retry,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods={"POST"},
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.mount("http://", adapter)
        self.mount("https://", adapter)
        # La verificación del certificado ya se controla en cada petición, pero
        # lo dejamos en ``False`` para mantener la compatibilidad con el
        # comportamiento previo del script.
        self.verify = False
        # Se almacena el número lógico de reintentos solicitados para reutilizar
        # la sesión cuando coincidan las configuraciones.
        self._max_reintentos = max_reintentos  # type: ignore[attr-defined]


_SESION_SOAP: Optional[_SesionSOAP] = None
_ULTIMA_INVOCACION_SOAP: float = 0.0


def _obtener_sesion_soap(max_reintentos: int) -> _SesionSOAP:
    """Obtiene una sesión HTTP reutilizable con la política de reintentos."""

    global _SESION_SOAP
    if _SESION_SOAP is None or getattr(_SESION_SOAP, "_max_reintentos", None) != max_reintentos:
        _SESION_SOAP = _SesionSOAP(max_reintentos)
    return _SESION_SOAP


def _respetar_intervalo_solicitudes() -> None:
    """Garantiza un descanso mínimo entre llamadas al servicio SOAP."""

    if MIN_INTERVALO_SOAP_SEGUNDOS <= 0:
        return

    global _ULTIMA_INVOCACION_SOAP
    momento_actual = time.monotonic()
    restante = MIN_INTERVALO_SOAP_SEGUNDOS - (momento_actual - _ULTIMA_INVOCACION_SOAP)
    if restante > 0:
        time.sleep(restante)
        momento_actual = time.monotonic()
    _ULTIMA_INVOCACION_SOAP = momento_actual


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
    omitir_filtro_estados: bool = False


def _asegurar_tablas_panel(conn_panel: psycopg2.extensions.connection) -> None:
    """Crea las tablas e índices requeridos para registrar ejecuciones."""

    sentencia_crear_procesos = """
        CREATE TABLE IF NOT EXISTS public.procesosat (
            procesosatid int8 DEFAULT nextval('procesosatid'::regclass) NOT NULL,
            procesosatnombre varchar(40) NOT NULL,
            procesosatdescripcion varchar(400) NULL,
            procesosatultiej timestamp NULL,
            procesosatprxej timestamp NULL,
            CONSTRAINT procesosat_pkey PRIMARY KEY (procesosatid)
        )
    """

    sentencia_crear_ejecproc = """
        CREATE TABLE IF NOT EXISTS public.ejecproc (
            ejecprocid int8 DEFAULT nextval('ejecprocid'::regclass) NOT NULL,
            procesosatid int8 NOT NULL,
            ejecprocfecha timestamp NOT NULL,
            ejecprocresultado int2 NOT NULL,
            ejecprocobservaciones varchar(400) NULL,
            CONSTRAINT ejecproc_pkey PRIMARY KEY (ejecprocid)
        )
    """

    sentencia_crear_indice = """
        CREATE INDEX IF NOT EXISTS iejecproc1
            ON public.ejecproc USING btree (procesosatid)
    """

    with conn_panel.cursor() as cursor:
        cursor.execute("CREATE SEQUENCE IF NOT EXISTS procesosatid")
        cursor.execute("CREATE SEQUENCE IF NOT EXISTS ejecprocid")
        cursor.execute(sentencia_crear_procesos)
        cursor.execute(sentencia_crear_ejecproc)
        cursor.execute(sentencia_crear_indice)
    conn_panel.commit()


def _actualizar_inicio_proceso(
    conn_panel: psycopg2.extensions.connection, inicio: datetime
) -> None:
    """Actualiza la hora de última ejecución del proceso en ``procesosat``."""

    sentencia = """
        UPDATE public.procesosat
        SET procesosatultiej = %s
        WHERE procesosatid = %s
    """

    with conn_panel.cursor() as cursor:
        cursor.execute(sentencia, (inicio, PROCESO_RETORNOMP_ID))
        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO public.procesosat (
                    procesosatid,
                    procesosatnombre,
                    procesosatdescripcion,
                    procesosatultiej
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (procesosatid) DO UPDATE
                SET procesosatultiej = EXCLUDED.procesosatultiej
                """,
                (
                    PROCESO_RETORNOMP_ID,
                    "retornoxmlmp",
                    "Sincronización de XML Ministerio Público",
                    inicio,
                ),
            )
    conn_panel.commit()


def _registrar_evento_ejecucion(
    conn_panel: psycopg2.extensions.connection,
    fecha: datetime,
    resultado: int,
    observaciones: str,
) -> None:
    """Inserta un registro en ``ejecproc`` con la información indicada."""

    sentencia = """
        INSERT INTO public.ejecproc (
            procesosatid,
            ejecprocfecha,
            ejecprocresultado,
            ejecprocobservaciones
        )
        VALUES (%s, %s, %s, %s)
    """

    texto_observaciones = (observaciones or "")[:MAX_OBSERVACION_LEN]

    with conn_panel.cursor() as cursor:
        cursor.execute(
            sentencia,
            (
                PROCESO_RETORNOMP_ID,
                fecha,
                resultado,
                texto_observaciones,
            ),
        )
    conn_panel.commit()


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
        descripcion="En Notificaciones/Entregada/No entregada, fechalaststate <= 5 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        max_dias=5,
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
        descripcion="En Notificaciones/Entregada/No entregada, 5 < fechalaststate <= 10 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        min_dias=5,
        max_dias=10,
    ),
    IteracionConsulta(
        descripcion="Rectificación entregada/Rectificación No Entregada, 10 < fechalaststate <= 20 días",
        estados=("Rectificación entregada", "Rectificación No Entregada"),
        min_dias=10,
        max_dias=20,
    ),
    IteracionConsulta(
        descripcion="Pendiente/Ingresada, 20 < fechalaststate <= 45 días",
        estados=("Pendiente", "Ingresada"),
        min_dias=20,
        max_dias=45,
        incluir_estados_vacios=True,
    ),
    IteracionConsulta(
        descripcion="En Dep. Policial/Enviada, 20 < fechalaststate <= 45 días",
        estados=("En Dep. Policial", "Enviada"),
        min_dias=20,
        max_dias=45,
    ),
    IteracionConsulta(
        descripcion="En Notificaciones/Entregada/No entregada, 10 < fechalaststate <= 15 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        min_dias=10,
        max_dias=15,
    ),
    IteracionConsulta(
        descripcion="Rectificación entregada/Rectificación No Entregada, 20 < fechalaststate <= 45 días",
        estados=("Rectificación entregada", "Rectificación No Entregada"),
        min_dias=20,
        max_dias=45,
    ),
    IteracionConsulta(
        descripcion="En Notificaciones/Entregada/No entregada, 15 < fechalaststate <= 20 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        min_dias=15,
        max_dias=20,
    ),
    IteracionConsulta(
        descripcion="En Notificaciones/Entregada/No entregada, 20 < fechalaststate <= 25 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        min_dias=20,
        max_dias=25,
    ),
    IteracionConsulta(
        descripcion="En Notificaciones/Entregada/No entregada, 25 < fechalaststate <= 45 días",
        estados=("En Notificaciones", "Entregada", "No entregada"),
        min_dias=25,
        max_dias=45,
    ),
)


def _ejecutar_historial_sian(
    codigo_seguimiento: Optional[str] = None,
) -> None:
    """Ejecuta ``historialsian.py`` y espera a que finalice.

    Parameters
    ----------
    codigo_seguimiento:
        Código específico que se desea procesar. Cuando es ``None`` se
        ejecuta el comportamiento tradicional que procesa todos los
        retornos pendientes.
    """

    script_path = Path(__file__).resolve().parent / "historialsian.py"
    if not script_path.exists():
        _log_step(
            "procesar_envios",
            "ERROR",
            f"No se encontró historialsian.py en {script_path}",
        )
        return

    _log_step(
        "procesar_envios",
        "INICIO",
        f"Ejecutando {script_path.name}",
    )

    try:
        comando = [sys.executable, str(script_path)]
        if codigo_seguimiento:
            comando.extend(["--codigodeseguimientomp", codigo_seguimiento])
        subprocess.run(comando, check=True)
    except (subprocess.CalledProcessError, OSError) as exc:
        print(
            f"[retornoxmlmp] historialsian.py finalizó con error: {exc}",
            flush=True,
        )
        _log_step(
            "procesar_envios",
            "ERROR",
            f"historialsian.py finalizó con error: {exc}",
        )
    else:
        mensaje = "historialsian.py finalizó correctamente"
        if codigo_seguimiento:
            mensaje = (
                f"historialsian.py finalizó correctamente para "
                f"{codigo_seguimiento}"
            )
        _log_step("procesar_envios", "OK", mensaje)


def _obtener_envios(
    conn_pg: psycopg2.extensions.connection,
    iteracion: IteracionConsulta,
    momento_referencia: datetime,
    codigo_especifico: Optional[str] = None,
) -> List[EnvioNotificacion]:
    """Obtiene los envíos a consultar en el servicio SOAP para una iteración."""

    params: List[object] = []
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
          AND feiw = 'NO'
    """

    if not iteracion.omitir_filtro_estados:
        estados_considerados = list(iteracion.estados)
        if iteracion.incluir_estados_vacios:
            estados_considerados.append("")

        comparacion_estados = "laststagesian = ANY(%s)"
        if iteracion.incluir_estados_vacios:
            comparacion_estados = "COALESCE(laststagesian, '') = ANY(%s)"

        consulta += f"\n          AND {comparacion_estados}"
        params.append(estados_considerados)

    if codigo_especifico is None:
        consulta += "\n          AND fechalaststate IS NOT NULL"

    if iteracion.max_dias is not None:
        fecha_minima = momento_referencia - timedelta(days=iteracion.max_dias)
        consulta += "\n          AND fechalaststate >= %s"
        params.append(fecha_minima)

    if iteracion.min_dias is not None:
        fecha_maxima = momento_referencia - timedelta(days=iteracion.min_dias)
        consulta += "\n          AND fechalaststate < %s"
        params.append(fecha_maxima)

    if codigo_especifico is not None:
        consulta += "\n          AND TRIM(codigoseguimientomp) = %s"
        params.append(codigo_especifico.strip())

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
    max_reintentos: int = 3,
    mostrar_respuesta: bool = True,
) -> Tuple[Optional[ResultadoSOAP], Optional[str]]:
    """Invoca el servicio SOAP y retorna el XML completo de la respuesta."""

    url = f"{_host_soap(usar_test)}/services/wsNotificacion.asmx"
    payload = _construir_xml_peticion(codigo_seguimiento)
    headers = {
        "Content-Type": "text/xml; charset=UTF-8",
        "SOAPAction": SOAP_ACTION,
    }

    sesion = _obtener_sesion_soap(max_reintentos)

    try:
        _respetar_intervalo_solicitudes()
        respuesta = sesion.post(
            url,
            data=payload,
            headers=headers,
            timeout=timeout,
        )
    except requests.RequestException as exc:
        mensaje_error = f"{codigo_seguimiento}: error de red {exc}"
        _log_step(
            "_invocar_servicio",
            "ERROR",
            mensaje_error,
        )
        return None, mensaje_error

    if respuesta is None:
        mensaje_error = f"{codigo_seguimiento}: no se obtuvo respuesta del servicio"
        _log_step(
            "_invocar_servicio",
            "ERROR",
            mensaje_error,
        )
        return None, mensaje_error

    if respuesta.status_code == 429:
        retry_after = respuesta.headers.get("Retry-After")
        segundos_espera = _segundos_retry_after(
            retry_after,
            referencia=datetime.now(timezone.utc),
        )
        mensaje_error = (
            f"{codigo_seguimiento}: HTTP 429 Too Many Requests"
            + (
                f" (Retry-After: {retry_after}, {segundos_espera}s)"
                if retry_after and segundos_espera is not None
                else ""
            )
        )
        _log_step(
            "_invocar_servicio",
            "ADVERTENCIA",
            mensaje_error,
        )
        if segundos_espera:
            time.sleep(segundos_espera)
        return None, mensaje_error

    if respuesta.status_code != 200:
        mensaje_error = (
            f"{codigo_seguimiento}: HTTP {respuesta.status_code} {respuesta.text}"
        )
        _log_step(
            "_invocar_servicio",
            "ERROR",
            mensaje_error,
        )
        return None, mensaje_error

    xml_texto = respuesta.text.strip()
    if not xml_texto:
        mensaje_error = f"{codigo_seguimiento}: respuesta vacía"
        _log_step(
            "_invocar_servicio",
            "ADVERTENCIA",
            mensaje_error,
        )
        return None, mensaje_error

    if mostrar_respuesta:
        xml_legible = _formatear_xml_legible(xml_texto)
        print(f"CODIGODESEGUIMIENTOMP: {codigo_seguimiento}")
        print(f"XML amigable:\n{xml_legible}\n")

    return ResultadoSOAP(codigo_seguimiento=codigo_seguimiento, xml_respuesta=xml_texto), None


def _formatear_xml_legible(xml_texto: str) -> str:
    """Devuelve el XML con indentación legible, o el original si falla."""

    try:
        documento = minidom.parseString(xml_texto)
        pretty = documento.toprettyxml(indent="  ")
        lineas = [linea for linea in pretty.splitlines() if linea.strip()]
        return "\n".join(lineas)
    except Exception:
        return xml_texto


def _segundos_retry_after(
    valor_header: Optional[str],
    *,
    referencia: Optional[datetime] = None,
) -> Optional[int]:
    """Convierte el encabezado ``Retry-After`` a segundos si es posible."""

    if not valor_header:
        return None

    valor_normalizado = valor_header.strip()
    if not valor_normalizado:
        return None

    if valor_normalizado.isdigit():
        try:
            segundos = int(valor_normalizado)
        except ValueError:
            return None
        return max(0, segundos)

    try:
        fecha_reintento = parsedate_to_datetime(valor_normalizado)
    except (TypeError, ValueError, IndexError):
        return None

    if fecha_reintento is None:
        return None

    if fecha_reintento.tzinfo is None:
        fecha_reintento = fecha_reintento.replace(tzinfo=timezone.utc)

    momento_referencia = referencia or datetime.now(timezone.utc)
    diferencia = (fecha_reintento - momento_referencia).total_seconds()
    if diferencia <= 0:
        return None
    return int(diferencia)


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


def procesar_envios(
    usar_test: Optional[bool] = None,
    dias: Optional[int] = None,
    codigodeseguimientomp: Optional[str] = None,
) -> None:
    """Ejecuta el flujo completo para las iteraciones configuradas."""

    bandera_test = default_test_flag if usar_test is None else usar_test

    inicio_proceso = datetime.now()
    momento_referencia = inicio_proceso

    if dias is not None and dias < 0:
        raise ValueError("El parámetro 'dias' debe ser mayor o igual a cero")

    codigo_filtrado = (codigodeseguimientomp or "").strip() or None

    with psycopg2.connect(**pgsql_config) as conn_pg, psycopg2.connect(**panel_config) as conn_panel:
        conn_pg.autocommit = False
        conn_panel.autocommit = False

        _asegurar_tablas_panel(conn_panel)
        _actualizar_inicio_proceso(conn_panel, inicio_proceso)

        if codigo_filtrado is not None:
            iteraciones = (
                IteracionConsulta(
                    descripcion=(
                        f"Código seguimiento {codigo_filtrado}"
                    ),
                    estados=(),
                    omitir_filtro_estados=True,
                ),
            )
        elif dias is not None:
            iteraciones = (
                IteracionConsulta(
                    descripcion=(
                        "fechalaststate >= {fecha:%Y-%m-%d}".format(
                            fecha=momento_referencia.date() - timedelta(days=dias)
                        )
                    ),
                    estados=(),
                    max_dias=dias,
                    omitir_filtro_estados=True,
                ),
            )
        else:
            iteraciones = ITERACIONES

        se_procesaron_envios_codigo = False
        iteraciones_preparadas: List[Tuple[IteracionConsulta, datetime, List[EnvioNotificacion], str, bool, bool]] = []
        total_envios = 0

        for iteracion in iteraciones:
            inicio_iteracion = datetime.now()

            try:
                envios = _obtener_envios(
                    conn_pg,
                    iteracion,
                    momento_referencia,
                    codigo_especifico=codigo_filtrado,
                )
            except Exception as exc:
                mensaje_error = (
                    f"[procesar_envios] Iteración: {iteracion.descripcion} | "
                    f"Error al obtener envíos: {exc}"
                )
                print(mensaje_error)
                conn_pg.rollback()
                conn_panel.rollback()
                _registrar_evento_ejecucion(
                    conn_panel, inicio_iteracion, 0, mensaje_error
                )
                continue

            cantidad_envios = len(envios)
            total_envios += cantidad_envios
            mensaje_iteracion = (
                "[procesar_envios] Iteración: {descripcion} | Inicio: {inicio:%Y-%m-%d %H:%M:%S} | "
                "Registros a procesar: {cantidad}"
            ).format(
                descripcion=iteracion.descripcion,
                inicio=inicio_iteracion,
                cantidad=cantidad_envios,
            )

            es_iteracion_dependencia = iteracion.estados == (
                "En Dep. Policial",
                "Enviada",
            )
            es_iteracion_notificaciones_25_45 = (
                iteracion.estados
                == (
                    "En Notificaciones",
                    "Entregada",
                    "No entregada",
                )
                and iteracion.min_dias == 25
                and iteracion.max_dias == 45
            )

            iteraciones_preparadas.append(
                (
                    iteracion,
                    inicio_iteracion,
                    envios,
                    mensaje_iteracion,
                    es_iteracion_dependencia,
                    es_iteracion_notificaciones_25_45,
                )
            )

        if not iteraciones_preparadas:
            print("Total de registros a procesar: 0")

        total_impreso = False

        for (
            iteracion,
            inicio_iteracion,
            envios,
            mensaje_iteracion,
            es_iteracion_dependencia,
            es_iteracion_notificaciones_25_45,
        ) in iteraciones_preparadas:
            if not total_impreso:
                print(f"Total de registros a procesar: {total_envios}")
                total_impreso = True

            ejecutar_historial_general = (
                codigo_filtrado is None
                and (es_iteracion_dependencia or es_iteracion_notificaciones_25_45)
            )

            ejecucion_exitosa = True

            try:
                if not envios:
                    if ejecutar_historial_general:
                        _ejecutar_historial_sian()
                else:
                    if codigo_filtrado is not None:
                        se_procesaron_envios_codigo = True
                    for envio in envios:
                        resultado, mensaje_error = _invocar_servicio(
                            envio.codigoseguimientomp, bandera_test
                        )
                        if mensaje_error:
                            observacion_error = (
                                f"[procesar_envios] Iteración: {iteracion.descripcion} | "
                                f"{mensaje_error}"
                            )
                            _registrar_evento_ejecucion(
                                conn_panel,
                                datetime.now(),
                                0,
                                observacion_error,
                            )
                        if resultado is None:
                            continue

                        try:
                            _almacenar_xml(
                                conn_panel,
                                envio,
                                resultado.xml_respuesta,
                            )
                        except Exception as exc:
                            conn_panel.rollback()
                            observacion_error = (
                                f"[procesar_envios] Iteración: {iteracion.descripcion} | "
                                f"{envio.codigoseguimientomp}: error al almacenar XML: {exc}"
                            )
                            _registrar_evento_ejecucion(
                                conn_panel,
                                datetime.now(),
                                0,
                                observacion_error,
                            )

                    if ejecutar_historial_general:
                        _ejecutar_historial_sian()
            except Exception as exc:
                ejecucion_exitosa = False
                conn_pg.rollback()
                conn_panel.rollback()
                observacion_error = f"{mensaje_iteracion} | Error inesperado: {exc}"
                print(observacion_error)
                _registrar_evento_ejecucion(
                    conn_panel,
                    inicio_iteracion,
                    0,
                    observacion_error,
                )

            if ejecucion_exitosa:
                _registrar_evento_ejecucion(
                    conn_panel,
                    inicio_iteracion,
                    1,
                    mensaje_iteracion,
                )

        if dias is not None and codigo_filtrado is None:
            _ejecutar_historial_sian()

        if codigo_filtrado is not None and se_procesaron_envios_codigo:
            _ejecutar_historial_sian(codigo_filtrado)


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
    parser.add_argument(
        "--dias",
        type=int,
        help=(
            "Limita la consulta a registros con fechalaststate mayor o igual a la fecha "
            "actual menos la cantidad de días indicada"
        ),
    )
    parser.add_argument(
        "--codigodeseguimientomp",
        "--codigoseguimientomp",
        dest="codigodeseguimientomp",
        type=str,
        help=(
            "Limita la ejecución al código de seguimiento especificado. "
            "Solo se consulta ese registro y se sincroniza su historial."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> None:
    """Punto de entrada para ejecución por consola."""

    args = _parse_args(argv)
    procesar_envios(
        usar_test=args.test,
        dias=args.dias,
        codigodeseguimientomp=args.codigodeseguimientomp,
    )


if __name__ == "__main__":
    main()
