import psycopg2
from datetime import datetime, date
import requests
import xml.etree.ElementTree as ET
from typing import Optional, Union, Iterable, Dict, Any, List


def _log_step(func_name: str, status: str, message: str = "") -> None:
    """Helper to print debug information with unified format."""

    base = f"[{func_name}] {status}"
    if message:
        base = f"{base}: {message}"
    print(base)

test = False


if test: 

    pgsql_config = {
        "host": "10.18.250.251",
        "port": "5432",
        "database": "iurixPj",
        "user": "cmayuda",
        "password": "power177"
    }



    # Parámetros de conexión PostgreSQL
    panel_config = {
        "host": "10.18.250.250",
        "port": "5432",
        "database": "panelnotificacionesws",
        "user": "usrsian",
        "password": "A8d%4pXq"
    }
else:
    pgsql_config = {
        "host": "10.18.250.250",
        "port": "5432",
        "database": "iurixPj",
        "user": "cmayuda",
        "password": "power177"
    }



    # Parámetros de conexión PostgreSQL
    panel_config = {
        "host": "10.18.250.250",
        "port": "5432",
        "database": "panelnotificacionesws",
        "user": "usrsian",
        "password": "A8d%4pXq"
    }

def lasstage(
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
    fecha_ultima_estado=None,
):
    # Configuración del WebService
    _log_step(
        "lasstage",
        "INICIO",
        f"Procesando seguimiento {CODIGO_SEGUIMIENTO} (mov: {pmovimientoid}, act: {pactuacionid})",
    )
    if test:
        HOST_WS_SIAN = "https://pruebasian.mpublico.gov.ar"  # Reemplaza con la URL real
    else:
        HOST_WS_SIAN = "https://sian.mpublico.gov.ar"
    BASE_URL = "/services/wsNotificacion.asmx"
    URL = f"{HOST_WS_SIAN}{BASE_URL}"

    # Credenciales
    USUARIO_CLAVE = "NES7u'FR>]e:3)D"
    USUARIO_NOMBRE = "wsPoderJudicial"

    # Código de seguimiento
    # CODIGO_SEGUIMIENTO = "C0D7BA"  # Ejemplo, reemplaza con el real

    # XML de la petición
    xml_data = f"""<?xml version="1.0" encoding="UTF-8"?>
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
        <soapenv:Header>
            <tem:Authentication>
                <tem:UsuarioClave>{USUARIO_CLAVE}</tem:UsuarioClave>
                <tem:UsuarioNombre>{USUARIO_NOMBRE}</tem:UsuarioNombre>
            </tem:Authentication>
        </soapenv:Header>
        <soapenv:Body>
            <tem:ObtenerEstadoNotificacion>
                <tem:codigoSeguimiento>{CODIGO_SEGUIMIENTO}</tem:codigoSeguimiento>
            </tem:ObtenerEstadoNotificacion>
        </soapenv:Body>
    </soapenv:Envelope>"""

    # print(xml_data)

    # Encabezados HTTP
    headers = {
        "Content-Type": "text/xml; charset=UTF-8",
        "SOAPAction": "http://tempuri.org/ObtenerEstadoNotificacion",  # A veces es obligatorio
    }

    try:
        # Realizar la petición
        response = requests.post(URL, data=xml_data, headers=headers, verify=False)  # verify=False si el certificado no es válido

        # Verificar respuesta
        if response.status_code == 200:
            # print("✅ Respuesta recibida con éxito")
            # print(response.text)  # Mostrar XML de respuesta
            root = ET.fromstring(response.text)

            namespaces = {
                "soap": "http://schemas.xmlsoap.org/soap/envelope/",
                "temp": "http://tempuri.org/",
            }

            estados = root.findall(
                ".//temp:HistorialEstados/temp:EstadoNotificacion", namespaces
            )

            estados_normalizados = _normalizar_estados(estados, namespaces)

            estados_filtrados = _filtrar_estados_nuevos(
                estados_normalizados, fecha_ultima_estado
            )

            if not estados_filtrados:
                _log_step(
                    "lasstage",
                    "OK",
                    "Sin estados nuevos para registrar",
                )
                return None, None

            if estados_filtrados:
                _guardar_historial_notpol(
                    estados_filtrados,
                    pmovimientoid,
                    pactuacionid,
                    pdomicilioelectronicopj,
                    CODIGO_SEGUIMIENTO,
                )

                ultimo_estado = estados_filtrados[-1]
                print(
                    "******Tag: {tag}, Atributos: {attrs}, valor: {valor}".format(
                        tag="{http://tempuri.org/}Estado",
                        attrs={},
                        valor=ultimo_estado.get("estado") or "",
                    )
                )
                _log_step(
                    "lasstage",
                    "OK",
                    f"Estados obtenidos correctamente para {CODIGO_SEGUIMIENTO}",
                )
                return ultimo_estado.get("estado"), ultimo_estado.get("fecha_raw")
            _log_step("lasstage", "OK", "Sin estados disponibles en la respuesta")
            return None, None

        else:
            _log_step(
                "lasstage",
                "ERROR",
                f"Código de estado HTTP {response.status_code}. Respuesta: {response.text}",
            )

    except requests.exceptions.RequestException as e:
        _log_step("lasstage", "ERROR", f"Error en la solicitud: {e}")

    return None, None


def pre_historial():
    _log_step("pre_historial", "INICIO", "Preparando actualización de registros")
    try:
        with psycopg2.connect(**pgsql_config) as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(
                    "update enviocedulanotificacionpolicia set finsian = True "
                    "where descartada = True and finsian <> True"
                )
                _log_step("pre_historial", "OK", "Registros descartados marcados como finalizados")

                cursor.execute(
                    "update enviocedulanotificacionpolicia set descartada= false, "
                    "laststagesian = 'Sin info', fechalaststate = null "
                    "where penviocedulanotificacionfechahora >= current_date - INTERVAL '1 days' "
                    "and coalesce(descartada,false) = false"
                )
                _log_step(
                    "pre_historial",
                    "OK",
                    "Registros reiniciados para seguimiento reciente",
                )

                conexion.commit()

                consultas = [
                    "AND  e.pdac_codigo = 'CEDURG'",
                    "AND  e.pdac_codigo <> 'CEDURG'",
                ]

                base_query = (
                    "SELECT e.pmovimientoid, e.pactuacionid, e.pdomicilioelectronicopj, "
                    "e.codigoseguimientomp, e.fechalaststate "
                    "FROM enviocedulanotificacionpolicia e "
                    "WHERE e.codigoseguimientomp IS NOT NULL "
                    "AND upper(trim(e.codigoseguimientomp)) <> 'NONE' "
                    "{extra_condition}"
                )

                for condicion_extra in consultas:
                    cursor.execute(base_query.format(extra_condition=condicion_extra))
                    for notas in cursor:
                        try:
                            _log_step(
                                "pre_historial",
                                "INICIO",
                                f"Procesando registro {notas}",
                            )
                            llamar_his_mp(
                                notas[0],
                                notas[1],
                                notas[2],
                                notas[3],
                                notas[4],
                            )
                            _log_step(
                                "pre_historial",
                                "OK",
                                f"Registro {notas[3]} procesado correctamente",
                            )
                        except Exception as e:
                            _log_step(
                                "pre_historial",
                                "ERROR",
                                f"Error procesando registro {notas}: {e}",
                            )

                _log_step("pre_historial", "OK", "Proceso de historial finalizado")
    except Exception as e:
        _log_step(
            "pre_historial",
            "ERROR",
            f"Error en la conexión o consulta SQL: {e}",
        )
                
    

def llamar_his_mp(
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
    fecha_ultima_estado,
):
    _log_step(
        "llamar_his_mp",
        "INICIO",
        f"Obteniendo historial para {CODIGO_SEGUIMIENTO}",
    )
    try:
        estado, fecha_estado = lasstage(
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            CODIGO_SEGUIMIENTO,
            fecha_ultima_estado,
        )

        if estado is None and fecha_estado is None:
            _log_step(
                "llamar_his_mp",
                "OK",
                f"Sin estados nuevos para {CODIGO_SEGUIMIENTO}",
            )
            return
        exito_guardado = grabar_historico(
            estado,
            fecha_estado,
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            CODIGO_SEGUIMIENTO,
        )
    except requests.exceptions.RequestException as e:
        _log_step("llamar_his_mp", "ERROR", f"Error en la solicitud: {e}")
    except Exception as e:
        _log_step("llamar_his_mp", "ERROR", f"Error inesperado: {e}")
    else:
        if exito_guardado:
            _log_step(
                "llamar_his_mp",
                "OK",
                f"Historial actualizado para {CODIGO_SEGUIMIENTO}",
            )
        else:
            _log_step(
                "llamar_his_mp",
                "ERROR",
                f"Historial no pudo actualizarse para {CODIGO_SEGUIMIENTO}",
            )

def _formatear_fecha_estado(fecha_estado: Optional[Union[str, datetime]]) -> str:
    """Devuelve una representación legible de la fecha del estado."""

    if isinstance(fecha_estado, datetime):
        return fecha_estado.strftime("%d/%m/%Y %H:%M:%S")

    if isinstance(fecha_estado, str):
        fecha = fecha_estado.strip()
        if not fecha:
            return "Sin fecha"

        fecha_normalizada = fecha.replace("Z", "+00:00")
        try:
            fecha_dt = datetime.fromisoformat(fecha_normalizada)
            return fecha_dt.strftime("%d/%m/%Y %H:%M:%S")
        except ValueError:
            formatos = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%d/%m/%Y %H:%M:%S",
                "%d/%m/%Y",
            ]
            for formato in formatos:
                try:
                    fecha_dt = datetime.strptime(fecha, formato)
                    return fecha_dt.strftime("%d/%m/%Y %H:%M:%S")
                except ValueError:
                    continue
            return fecha

    return "Sin fecha"


def grabar_historico(
    estado,
    fecha_estado,
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
):
    _log_step(
        "grabar_historico",
        "INICIO",
        f"Actualizando registro {CODIGO_SEGUIMIENTO}",
    )
    try:
        with psycopg2.connect(**pgsql_config) as conexion:
            with conexion.cursor() as cursor:
                estado_texto = estado or ""
                fecha_estado_texto = _formatear_fecha_estado(fecha_estado)
                print(
                    f"{CODIGO_SEGUIMIENTO} -> Estado: {estado_texto} | Fecha: {fecha_estado_texto}"
                )

                if estado_texto.upper() in (
                    "ENTREGADA",
                    "NO ENTREGADA",
                    "DESCARTADA",
                    "FINALIZADA",
                ):
                    finsian = True
                    print(f"{CODIGO_SEGUIMIENTO} -> Finalizada")
                else:
                    finsian = False

                update_query = """
                    UPDATE enviocedulanotificacionpolicia
                    SET laststagesian = %s,
                        fechalaststate = %s,
                        finsian = %s
                    WHERE codigoseguimientomp = %s
                """
                cursor.execute(
                    update_query,
                    (estado_texto, fecha_estado, finsian, CODIGO_SEGUIMIENTO),
                )
                conexion.commit()
                _log_step(
                    "grabar_historico",
                    "OK",
                    f"Registro {CODIGO_SEGUIMIENTO} actualizado ({estado_texto})",
                )
                return True

    except psycopg2.Error as e:
        _log_step(
            "grabar_historico",
            "ERROR",
            f"Error al actualizar la base de datos: {e}",
        )
    return False


def _normalizar_estados(
    estados: Iterable[ET.Element], namespaces: Dict[str, str]
) -> List[Dict[str, Any]]:
    _log_step("_normalizar_estados", "INICIO", "Normalizando estados recibidos")
    def obtener_texto(node: ET.Element, tag: str) -> Optional[str]:
        elemento = node.find(f"temp:{tag}", namespaces)
        if elemento is None:
            return None
        if elemento.text is None:
            return None
        texto = elemento.text.strip()
        return texto if texto else None

    estados_normalizados = []

    for estado_node in estados:
        estado_id = obtener_texto(estado_node, "EstadoNotificacionId")
        fecha_raw = obtener_texto(estado_node, "Fecha")
        estados_normalizados.append(
            {
                "estado_id": int(estado_id) if estado_id else None,
                "fecha": _parsear_fecha_estado_bd(fecha_raw),
                "fecha_raw": fecha_raw,
                "estado": obtener_texto(estado_node, "Estado"),
                "observaciones": obtener_texto(estado_node, "Observaciones"),
                "motivo": obtener_texto(estado_node, "Motivo"),
                "responsable": obtener_texto(
                    estado_node, "ResponsableNotificacion"
                ),
                "dependencia": obtener_texto(
                    estado_node, "DependenciaNotificacion"
                ),
                "archivo_id": obtener_texto(estado_node, "ArchivoId"),
                "archivo_nombre": obtener_texto(estado_node, "ArchivoNombre"),
            }
        )

    _log_step(
        "_normalizar_estados",
        "OK",
        f"Total estados normalizados: {len(estados_normalizados)}",
    )
    return estados_normalizados


def _normalizar_fecha_para_comparacion(
    fecha: Optional[Union[datetime, date]]
) -> Optional[datetime]:
    if fecha is None:
        return None

    if isinstance(fecha, date) and not isinstance(fecha, datetime):
        fecha = datetime.combine(fecha, datetime.min.time())

    if fecha.tzinfo is not None:
        return fecha.replace(tzinfo=None)

    return fecha


def _filtrar_estados_nuevos(
    estados: Iterable[Dict[str, Any]],
    fecha_ultima_estado: Optional[datetime],
) -> List[Dict[str, Any]]:
    fecha_ultima_normalizada = _normalizar_fecha_para_comparacion(fecha_ultima_estado)
    estados_filtrados: List[Dict[str, Any]] = []

    for estado in estados:
        fecha_estado = estado.get("fecha")
        fecha_estado_normalizada = _normalizar_fecha_para_comparacion(fecha_estado)

        if (
            fecha_ultima_normalizada is None
            or fecha_estado_normalizada is None
            or fecha_estado_normalizada >= fecha_ultima_normalizada
        ):
            estados_filtrados.append(estado)

    return estados_filtrados


def _guardar_historial_notpol(
    estados: Iterable[Dict[str, Any]],
    pmovimientoid,
    pactuacionid,
    pdomicilioelectronicopj,
    CODIGO_SEGUIMIENTO,
):
    if not estados:
        _log_step(
            "_guardar_historial_notpol",
            "OK",
            f"Sin estados para registrar en {CODIGO_SEGUIMIENTO}",
        )
        return

    insert_query = """INSERT INTO notpolhistoricomp \
        (notpolhistoricomparchivoid, notpolhistoricomparchivonombre, notpolhistoricompestadonid, \
        notpolhistoricomparchcont, notpolhistoricompfecha, notpolhistoricompestado, \
        notpolhistoricompobservaciones, notpolhistoricompmotivo, notpolhistoricompresponsable, \
        notpolhistoricompdependencia, pmovimientoid, pactuacionid, pdomicilioelectronicopj, \
        codigoseguimientomp) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (notpolhistoricompestadonid) DO NOTHING;"""

    try:
        with psycopg2.connect(**pgsql_config) as conexion:
            with conexion.cursor() as cursor:
                if not isinstance(estados, list):
                    estados = list(estados)
                total_estados = len(estados)
                _log_step(
                    "_guardar_historial_notpol",
                    "INICIO",
                    f"Preparando inserción de {total_estados} estados para {CODIGO_SEGUIMIENTO}",
                )
                for estado in estados:
                    valores = (
                        estado.get("archivo_id"),
                        estado.get("archivo_nombre"),
                        estado.get("estado_id"),
                        None,
                        estado.get("fecha"),
                        estado.get("estado"),
                        estado.get("observaciones"),
                        estado.get("motivo"),
                        estado.get("responsable"),
                        estado.get("dependencia"),
                        pmovimientoid,
                        pactuacionid,
                        pdomicilioelectronicopj,
                        CODIGO_SEGUIMIENTO,
                    )
                    cursor.execute(insert_query, valores)
            conexion.commit()
            _log_step(
                "_guardar_historial_notpol",
                "OK",
                f"Historial guardado para {CODIGO_SEGUIMIENTO}",
            )
    except psycopg2.Error as e:
        _log_step(
            "_guardar_historial_notpol",
            "ERROR",
            f"Error al insertar historial notificación en panel: {e}",
        )


def _parsear_fecha_estado_bd(fecha_estado: Optional[str]) -> Optional[datetime]:
    if not fecha_estado:
        return None

    fecha = fecha_estado.strip()
    if not fecha:
        return None

    fecha_normalizada = fecha.replace("Z", "+00:00")

    formatos = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]

    for formato in formatos:
        try:
            return datetime.strptime(fecha_normalizada, formato)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(fecha_normalizada)
    except ValueError:
        return None

if __name__ == "__main__":
    pre_historial()
    #ejecutar_control_cedulas()
    #ejecutar_control_historial()
    #registrarproceso(connpanel,'paso4')
    print(datetime.now())
    #pre_historial()


   


 #ejecutar_envio_cedulas(
            #     cantidad=100,  # Ajustar el valor según sea necesario
            #     urlsian='sian.mpublico.gov.ar',
            #     pmovimientoid=datos_grilla['pmovimientoid'],
            #     pactuacionid=datos_grilla['pactuacionid'],
            #     pdomicilioelectronicopj=datos_grilla['pdomicilioelectronicopj'],
            #     urlpj='https://appweb.justiciasalta.gov.ar:8091/policia/api/cnotpolicia/enviarnotificacionsian'
            #)

#sian.mpublico.gov.ar




# Parsear el archivo XML


# Recorrer el XML

