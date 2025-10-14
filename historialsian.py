import psycopg2
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
from typing import Optional, Union

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

def lasstage(pmovimientoid, pactuacionid, pdomicilioelectronicopj, CODIGO_SEGUIMIENTO):
    # Configuración del WebService
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

            last_index = len(estados) - 1

            for index, estado_node in enumerate(estados):
                estado_text = None
                fecha_estado = None

                estado_element = estado_node.find("temp:Estado", namespaces)
                if estado_element is not None:
                    estado_text = estado_element.text

                fecha_element = estado_node.find("temp:Fecha", namespaces)
                if fecha_element is not None:
                    fecha_estado = fecha_element.text

                if not estado_text:
                    continue

                if index == last_index:
                    print(
                        f"******Tag: {{http://tempuri.org/}}Estado, Atributos: {estado_element.attrib if estado_element is not None else {}}, valor: {estado_text}"
                    )
                    return estado_text, fecha_estado
                # registrarnotpolhst(estado_text, fecha_estado)
            return None, None

        else:
            print(f"⚠️ Error en la respuesta: Código {response.status_code}")
            print(response.text)

    except requests.exceptions.RequestException as e:
        print(f"❌ Error en la solicitud: {e}")

    return None, None


def pre_historial():
    try:
        with psycopg2.connect(**pgsql_config) as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(
                    "update enviocedulanotificacionpolicia set finsian = True "
                    "where descartada = True and finsian <> True"
                )

                cursor.execute(
                    "update enviocedulanotificacionpolicia set descartada= false, "
                    "laststagesian = 'Sin info', fechalaststate = null "
                    "where penviocedulanotificacionfechahora >= current_date - INTERVAL '1 days' "
                    "and coalesce(descartada,false) = false"
                )

                conexion.commit()

                consultas = [
                    ("AND  e.pdac_codigo = 'CEDURG'", "15 days"),
                    ("AND  e.pdac_codigo <> 'CEDURG'", "5 days"),
                ]

                base_query = (
                    "SELECT e.pmovimientoid, e.pactuacionid, e.pdomicilioelectronicopj, e.codigoseguimientomp "
                    "FROM enviocedulanotificacionpolicia e "
                    "WHERE e.codigoseguimientomp IS NOT NULL "
                    "AND upper(trim(e.codigoseguimientomp)) <> 'NONE' "
                    "AND e.finsian = false "
                    "{extra_condition} "
                    "AND e.penviocedulanotificacionfechahora >= current_date - INTERVAL %s"
                )

                for condicion_extra, intervalo in consultas:
                    cursor.execute(
                        base_query.format(extra_condition=condicion_extra), (intervalo,)
                    )
                    for notas in cursor:
                        try:
                            llamar_his_mp(notas[0], notas[1], notas[2], notas[3])
                        except Exception as e:
                            print(f"Error procesando registro {notas}: {e}")

    except Exception as e:
        print("Error en la conexión o consulta SQL")
        print(f"Error: {e}")
                
    

def llamar_his_mp(pmovimientoid, pactuacionid, pdomicilioelectronicopj, CODIGO_SEGUIMIENTO):
    try:
        estado, fecha_estado = lasstage(
            pmovimientoid, pactuacionid, pdomicilioelectronicopj, CODIGO_SEGUIMIENTO
        )
        grabar_historico(
            estado,
            fecha_estado,
            pmovimientoid,
            pactuacionid,
            pdomicilioelectronicopj,
            CODIGO_SEGUIMIENTO,
        )
    except requests.exceptions.RequestException as e:
        print(f"❌ Error en la solicitud: {e}")

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

    except psycopg2.Error as e:
        print(f"Error al insertar en la base de datos: {e}")

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

