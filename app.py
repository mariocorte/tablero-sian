from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi import BackgroundTasks
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import psycopg2
import os
import base64
import time
from datetime import datetime, timedelta
import jaydebeapi
import requests
import json
import xml.etree.ElementTree as ET
from typing import Tuple, Optional  # ✅ agregado

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Variables globales para cachear las consultas
query_sql_cache: Optional[str] = None
queryvl_cache: Optional[str] = None


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Página de inicio con el formulario web."""
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
        },
    )

# Modelo para recibir parámetros en la API
class QueryParams(BaseModel):
    testservicios: int
    hostpgsql: str
    portpgsql: str
    databasepgsql: str
    user: str
    password: str
    userpanel: str
    passwordpanel: str
    hostpanel: str
    portpanel: str
    databasepanel: str
    hostiw: str
    portiw: str
    databaseiw: str


classpath = "/code/app/ifxjdbc.jar"

database_url = (
    "jdbc:informix-sqli://scpenala:1526/cpenal:"
    "INFORMIXSERVER=scpenala;LOBCACHE=-1;"
    "JDBCTEMP=/tmp;ifxJDBCTEMP=/tmp;FREECLOBWITHRS=true"
)

username = "cmayuda"
password = "power177"
driver_class = os.environ.get("DRIVER_CLASS", "com.informix.jdbc.IfxDriver")


def ensure_queries_loaded() -> Tuple[Optional[str], Optional[str]]:
    """Carga las consultas SQL solo cuando son necesarias."""
    global query_sql_cache, queryvl_cache
    if query_sql_cache is None:
        query_sql_cache = cargar_consulta("SQL-ACT-GAR-SIAN")
    if queryvl_cache is None:
        queryvl_cache = cargar_consulta("SQL-ACT-VIO-SIAN")
    return query_sql_cache, queryvl_cache


@app.post("/enviosian")
async def root(params: QueryParams, background_tasks: BackgroundTasks):
    # Función principal
    query_sql, queryvl = ensure_queries_loaded()

    if not query_sql or not queryvl:
        return {"mensaje": "No se pudieron cargar las consultas SQL requeridas.", "errores": 1}

    test = params.testservicios
    pgsql_config = {
        "host": params.hostpgsql,
        "port": params.portpgsql,
        "database": params.databasepgsql,
        "user": params.user,
        "password": params.password,
    }
    panel_config = {
        "host": params.hostpanel,
        "port": params.portpanel,
        "database": params.databasepanel,
        "user": params.userpanel,
        "password": params.passwordpanel,
    }

    pgsql_iw = {
        "host": params.hostiw,
        "port": params.portiw,
        "database": params.databaseiw,
        "user": params.user,
        "password": params.password,
    }

    print(f"Iniciando proceso Penal a las {datetime.now()}")
    procesar_e_insertar(pgsql_config, panel_config, test, query_sql)  # ✅ corregido
    print("Proceso completado Penal. Esperando próximo ciclo...")
    print(f"Iniciando proceso Violencia a las {datetime.now()}")
    #print(f"queryvl: {queryvl}")
    procesar_e_insertar_iw(pgsql_config, pgsql_iw, panel_config, test, queryvl)  # ✅ corregido

    # 🔹 Lanza las tareas en segundo plano
    #background_tasks.add_task(procesar_e_insertar_iw, pgsql_config, pgsql_iw, panel_config, test, queryvl)
    print("Proceso completado Violencia. Esperando próximo ciclo...")

    try:
        return {"mensaje": "Proceso lanzado en segundo plano correctamente", "errores": 0}
    except Exception as e:
        return {"error": str(e)}


# Función para cargar consulta SQL desde parametros


def cargar_consulta(archivo):
    pgsql_config = {
        "host": "10.18.250.250",
        "port": "5432",
        "database": "iurixPj",
        "user": "cmayuda",
        "password": "power177"
    }
    query = f"""SELECT parametroblobfile FROM parametro \
             WHERE parametronombre = '{archivo}';"""
    print(query)
    connlp = None
    try:
        connlp = psycopg2.connect(**pgsql_config)
        respuesta = connlp.cursor()
        respuesta.execute(query)
        rows = respuesta.fetchall()
        retorno = None
        for fila in rows:
            retorno = fila[0]
            if isinstance(retorno, memoryview):
                retorno = retorno.tobytes().decode('utf-8', errors='ignore')
            elif isinstance(retorno, bytes):
                retorno = retorno.decode('utf-8', errors='ignore')
            elif not isinstance(retorno, str):
                retorno = str(retorno)
        return retorno
    except Exception as e:
        print(f"Error al cargar consulta {archivo}: {e}")
        return None



def ejecutar_sqlix(query_sql):
    if not query_sql:
        return []
    try:
        conn = jaydebeapi.connect(
            driver_class,
            database_url,
            [username, password],
            classpath
        )
        cursor = conn.cursor()
        cursor.execute(query_sql)
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"Error al ejecutar Informix: {e}")
        return []


def ejecutar_iw(pgsql_iw, queryvl):
    print("ejecutar_iw")
    print(f"queryvl: {queryvl}")
    if not queryvl:
        print("No encontro nada")
        return []
    try:
        conn = psycopg2.connect(**pgsql_iw)
        cursor = conn.cursor()
        cursor.execute(queryvl)
        rows = cursor.fetchall()
        conn.close()
        print("si encontro filas")
        return rows
    except Exception as e:
        print(f"Error al ejecutar IW: {e}")
        return []


def es_base64(cadena):
    try:
        if isinstance(cadena, str):
            base64.b64decode(cadena, validate=True)
            return True
    except Exception:
        return False
    return False


def insertar_datos_enviocedula(conn, datos):
    try:
        with conn.cursor() as cursor:
            try:
                query = """
                INSERT INTO public.enviocedulanotificacionpolicia (
                    pmovimientoid, pactuacionid, pdomicilioelectronicopj, pfechayhora, pfechahora,
                    pdocumentotipoabreviatura, pnumero, panio, pdescripcion, pexpedienteid,
                    porganismoid, ptipoexpedienteid, pdependenciaenviopj, pdependenciaenvionombre,
                    pdac_codigo, pdac_descr, pdocumento, pdestinatario, pdirecciondestinatario,
                    pactuacionarchivo, ecednpoliciatitulo, ecednpoliciaobservaciones,
                    ecednpoliciadomiciliodep, ecednpoliciaidcentronot, ecednpoliciaidtiponot,
                    ecednpoliciaidexterno, ecednpolicianombdeppol, fechacreacion,
                    ecednpoliciadesccausa, parchivoactnombre, pactuacioniurix, irx_tcc_codigo,
                    irx_hca_numero, irx_hca_anio, irx_dac_codigo, irx_hac_numero,
                    penviocedulanotificacionexito,magistradofirma,denundiaid
                ) VALUES (
                    %(pmovimientoid)s, %(pactuacionid)s, %(pdomicilioelectronicopj)s, %(pfechayhora)s, %(pfechahora)s,
                    %(pdocumentotipoabreviatura)s, %(pnumero)s, %(panio)s, %(pdescripcion)s, %(pexpedienteid)s,
                    %(porganismoid)s, %(ptipoexpedienteid)s, %(pdependenciaenviopj)s, %(pdependenciaenvionombre)s,
                    %(pdac_codigo)s, %(pdac_descr)s, %(pdocumento)s, %(pdestinatario)s, %(pdirecciondestinatario)s,
                    %(pactuacionarchivo)s, %(ecednpoliciatitulo)s, %(ecednpoliciaobservaciones)s,
                    %(ecednpoliciadomiciliodep)s, %(ecednpoliciaidcentronot)s, %(ecednpoliciaidtiponot)s,
                    %(ecednpoliciaidexterno)s, %(ecednpolicianombredeppol)s, %(fechacreacion)s,
                    %(ecednpoliciadesccausa)s, %(parchivoactnombre)s, %(pactuacioniurix)s, %(irx_tcc_codigo)s,
                    %(irx_hca_numero)s, %(irx_hca_anio)s, %(irx_dac_codigo)s, %(irx_hac_numero)s,
                    %(penviocedulanotificacionexito)s,%(fte_resolucion)s,%(denuncia_id)s
                ) ON CONFLICT (pmovimientoid, pactuacionid, pdomicilioelectronicopj) DO NOTHING;
                """
                cursor.execute(query, datos)
                if cursor.rowcount > 0:
                    conn.commit()
                    print(f"Datos insertados para pmovimientoid: {datos['pactuacionid']}")
                    return True
                else:
                    return False
            except Exception as er:
                print(er)
    except Exception as e:
        conn.rollback()
        print(f"Error al insertar datos: {e}")
        return False
    


def ejecutar_convertidor_pdf(pmovimientoid, pactuacionid, pdomicilioelectronicopj, path, test):
    if test:
        url = 'https://appweb.justiciasalta.gov.ar:8091/testnotisian/api/cnotpolicia/convertirNotifPoliciaaPDF'
    else:
        url = 'https://appweb.justiciasalta.gov.ar:8091/policia/api/cnotpolicia/convertirNotifPoliciaaPDF'

    headers = {'Content-Type': 'application/json'}
    payload = {
        "SDTEntradaConvertidor": {
            "pmovimientoid": str(pmovimientoid).strip(),
            "pactuacionid": str(pactuacionid).strip(),
            "pdomicilioelectronicopj": pdomicilioelectronicopj.strip(),
            "path": path
        }
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            print("✅ Conversión a PDF exitosa.")
            return True
        else:
            print(f"Error en conversión a PDF: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        print(f"Error al convertir a PDF: {e}")
        return False


def ejecutar_envio_cedulas(pgsql_config, cantidad, urlsian, pmovimientoid, pactuacionid, pdomicilioelectronicopj, urlpj):
    payload = {
        "entradaCedula": {
            "cantidad": str(cantidad),
            "urlsian": urlsian,
            "pmovimientoid": str(pmovimientoid),
            "pactuacionid": str(pactuacionid),
            "pdomicilioelectronicopj": pdomicilioelectronicopj
        }
    }
    headers = {'Content-Type': 'application/json'}
    try:
        pass
    except Exception as e:
        print(f"Error al enviar cédulas: {e}")
        return False


def ejecutarpaso(proceso, panel_config):
    query = f"""SELECT procesosatid,procesosatnombre,procesosatdescripcion,procesosatultiej,procesosatprxej FROM procesosat \
        where procesosatnombre = '{proceso}' ;"""
    print(f"query: {query}")
    try:
        ejecutarpaso = False
        connlp = psycopg2.connect(**panel_config)
        horaaut = connlp.cursor()
        horaaut.execute(query)
        rows = horaaut.fetchall()
        for fila in rows:
            try:
                ultima = fila[3]
            except Exception:
                ultima = datetime.strptime("1900-01-01 00:00:00.0", "%Y-%m-%d %H:%M:%S.%f")
            try:
                proxima = fila[4]
            except Exception:
                proxima = datetime.strptime("2100-01-01 00:00:00.0", "%Y-%m-%d %H:%M:%S.%f")
            try:
                if datetime.now() > proxima:
                    ejecutarpaso = True
            except Exception:
                print("Error en comparacion 1")
                ejecutarpaso = False
    except Exception as e:
        print(f"Error en comparacion 2 {e}")
        ejecutarpaso = False

    return ejecutarpaso


def registrar_paso(proceso, codigo_proceso, panel_config):
    connrp = psycopg2.connect(**panel_config)
    print("Entro a registrar paso")
    try:
        with connrp.cursor() as cursor:
            ahora = datetime.now()
            proximo = ahora + timedelta(minutes=10)
            query = f"""
            UPDATE procesosat set procesosatultiej = '{ahora}',procesosatprxej = '{proximo}' where trim(procesosatnombre) = trim('{proceso}') ;
            """
            cursor.execute(query)
            query = f"""
            insert into ejecproc (procesosatid,ejecprocfecha,ejecprocresultado) values ({codigo_proceso},'{ahora}',0) ;
            """
            cursor.execute(query)
            connrp.commit()
            print(f"Datos de procesos actualizados {proceso} a hrs: {ahora}")
    except Exception as e:
        print(f"Error al registrar paso {e}")




def procesar_e_insertar(pgsql_config, panel_config, test, query_sql):
    print("Procesar e insertar IURIX")
    impix = 0
    try:
        rows = ejecutar_sqlix(query_sql)
        if not rows:
            print("No se obtuvieron datos de Informix.")
            return
        conn = psycopg2.connect(**pgsql_config)
        errores = []
        actualizar = True
        for fila in rows:
            try:
                hora_audiencia = fila[20].strip().replace('.0', '').replace(' HS:00', ":00").replace('.', ':')
                valorarchivo = fila[26]
                if not es_base64(valorarchivo):
                    if isinstance(valorarchivo, bytes):
                        valorarchivo = base64.b64encode(valorarchivo).decode('utf-8')
                    else:
                        valorarchivo = base64.b64encode(str(valorarchivo).encode('utf-8')).decode('utf-8')
                datos_insertar = {
                    'pmovimientoid': int(fila[0]),
                    'pactuacionid': int(fila[2]),
                    'pdomicilioelectronicopj': fila[22].strip(),
                    'penviocedulanotificacionfechahora': '0001-01-01 00:00:00.000',
                    'pfechayhora': hora_audiencia,
                    'pfechahora': fila[6].strip() + " 00:00:00.0",
                    'pdocumentotipoabreviatura': fila[3].strip(),
                    'pnumero': int(fila[5]),
                    'panio': int(fila[4]),
                    'pdescripcion': fila[16].strip(),
                    'pexpedienteid': 0,
                    'porganismoid': 0,
                    'ptipoexpedienteid': 0,
                    'pdependenciaenviopj': fila[9],
                    'pdependenciaenvionombre': fila[11].strip(),
                    'pdac_codigo': fila[24].strip(),
                    'pdac_descr': fila[25].strip(),
                    'pdocumento': fila[17],
                    'pdestinatario': fila[18].strip(),
                    'pdirecciondestinatario': fila[19].strip(),
                    'pactuacionarchivo': valorarchivo,
                    'ecednpoliciatitulo': "CEDULA DE NOTIFICACION",
                    'ecednpoliciaobservaciones': fila[8],
                    'ecednpoliciadomiciliodep': 'N/A',
                    'ecednpoliciaidcentronot': fila[12],
                    'ecednpoliciaidtiponot': fila[13],
                    'ecednpoliciaidexterno': f"901{str(fila[21]).zfill(9)}",
                    'ecednpolicianombredeppol': fila[24],
                    'fechacreacion': datetime.now(),
                    'ecednpoliciadesccausa': f"{fila[3].strip()} {fila[5]}/{fila[4]}",
                    'parchivoactnombre': f"901{str(fila[21]).zfill(9)}.pdf",
                    'pactuacioniurix': int(fila[1]),
                    'irx_tcc_codigo': fila[27].strip(),
                    'irx_hca_numero': int(fila[28]),
                    'irx_hca_anio': int(fila[29]),
                    'irx_dac_codigo': fila[30].strip(),
                    'irx_hac_numero': fila[31],
                    'penviocedulanotificacionexito': False,
                    'fte_resolucion': fila[34].strip(),
                    'denuncia_id': fila[35]
                }
                if ejecutarpaso("paso1", panel_config) or True:
                    actualizar = True
                    if insertar_datos_enviocedula(conn, datos_insertar):
                        pmovimientoid = datos_insertar['pmovimientoid']
                        pactuacionid = datos_insertar['pactuacionid']
                        pdomicilioelectronicopj = datos_insertar['pdomicilioelectronicopj']
                        if ejecutar_convertidor_pdf(pmovimientoid, pactuacionid, pdomicilioelectronicopj, './static/apiconsumo/cnotpolicia', test):
                            if test:
                                print("test")
                            else:
                                print("prod")
            except Exception as e:
                errores.append(f"Error al procesar fila {fila[0]}: {e}")
                print(f"Error al procesar fila {fila[0]}: {e}")

        if actualizar:
            print(f"conexion: {panel_config}")
            registrar_paso("paso1", 1, panel_config)
        conn.close()
        if errores:
            print("Algunos registros no pudieron procesarse:")
            for error in errores:
                print(error)
        else:
            print("Todos los registros procesados correctamente.")
    except Exception as e:
        print(f"Error general: {e}")


def procesar_e_insertar_iw(pgsql_config, pgsql_iw, panel_config, test, queryvl):
    print("procesar e insertar iw")
    try:
        print(f"conexion: {pgsql_iw}")
        rows = ejecutar_iw(pgsql_iw, queryvl)
        if not rows:
            print("No se obtuvieron datos de Iurix Web.")
            return
        print("Si hay filas")
        conn = psycopg2.connect(**pgsql_config)
        errores = []
        actualizar = False
        for fila in rows:
            try:
                hora_audiencia = fila[20]
                datos_hex = fila[26]
                datos_bytes = bytes(datos_hex)
                datos_base64 = base64.b64encode(datos_bytes).decode('utf-8')
                valorarchivo = datos_base64
                datos_base64 = base64.b64encode(datos_bytes)
                datos_base64_str = datos_base64.decode('utf-8')
                valorarchivo = datos_base64_str

                datos_hex = fila[35]
                datos_bytes = bytes(datos_hex)
                datos_base64 = base64.b64encode(datos_bytes)
                datos_base64_str = datos_base64.decode('utf-8')
                valorarchivoactuacion = datos_base64_str
                datos_insertar = {
                    'pmovimientoid': int(fila[0]),
                    'pactuacionid': int(fila[2]),
                    'pdomicilioelectronicopj': str(fila[22]).strip(),
                    'penviocedulanotificacionfechahora': '0001-01-01 00:00:00.000',
                    'pfechayhora': hora_audiencia,
                    'pfechahora': fila[6],
                    'pdocumentotipoabreviatura': fila[3].strip(),
                    'pnumero': int(fila[4]),
                    'panio': int(fila[5]),
                    'pdescripcion': fila[16].strip(),
                    'pexpedienteid': 0,
                    'porganismoid': 0,
                    'ptipoexpedienteid': 0,
                    'pdependenciaenviopj': fila[9],
                    'pdependenciaenvionombre': fila[11].strip(),
                    'pdac_codigo': fila[24].strip(),
                    'pdac_descr': fila[25].strip(),
                    'pdocumento': fila[17],
                    'pdestinatario': fila[18].strip(),
                    'pdirecciondestinatario': fila[19].strip(),
                    'pactuacionarchivo': valorarchivo,
                    'ecednpoliciatitulo': "CEDULA DE NOTIFICACION",
                    'ecednpoliciaobservaciones': fila[8],
                    'ecednpoliciadomiciliodep': 'N/A',
                    'ecednpoliciaidcentronot': fila[12],
                    'ecednpoliciaidtiponot': fila[13],
                    'ecednpoliciaidexterno': f"900{str(fila[21]).zfill(9)}",
                    'ecednpolicianombredeppol': fila[24],
                    'fechacreacion': datetime.now(),
                    'ecednpoliciadesccausa': f"{fila[3].strip()} {fila[5]}/{fila[4]}",
                    'parchivoactnombre': f"900{str(fila[21]).zfill(9)}.pdf",
                    'pactuacioniurix': int(fila[1]),
                    'irx_tcc_codigo': fila[27].strip(),
                    'irx_hca_numero': int(fila[28]),
                    'irx_hca_anio': int(fila[29]),
                    'irx_dac_codigo': fila[30].strip(),
                    'irx_hac_numero': fila[31],
                    'penviocedulanotificacionexito': False,
                    'archivoactuacion': valorarchivoactuacion,
                    'archivoactuacionid': f"{str(fila[34])}.pdf",
                    'fte_resolucion': fila[36].strip(),
                    'denuncia_id': fila[37]

                }
                if ejecutarpaso("paso21", panel_config) or True:
                    actualizar = True
                    if insertar_datos_enviocedula(conn, datos_insertar):
                        resultado = insertar_documento(valorarchivoactuacion, f"900{str(fila[21]).zfill(9)}.pdf", 8880, test)
                        if resultado:
                            print("Respuesta del webservice:", json.dumps(resultado, indent=2))
                            rs = json.dumps(resultado, indent=2)
                            datadelws = json.loads(rs)

                            if datadelws.get('resultado', False):
                                sgdocid = datadelws.get('sgdDocId')
                                if sgdocid is not None:
                                    grabarcedulasconqr(int(fila[0]), int(fila[2]), str(fila[22]).strip(), sgdocid, pgsql_config)

                            resultado = insertar_documento(valorarchivo, f"{str(fila[34])}.pdf", 8880, test)
                            if resultado:
                                print("Respuesta del webservice:", json.dumps(resultado, indent=2))
                                rs = json.dumps(resultado, indent=2)
                                datadelws = json.loads(rs)
                                print(f"Previa:{datadelws}")
                                if datadelws.get('resultado', False):
                                    sgdocidc = datadelws.get('sgdDocId')
                                    print(f"sgdod:{sgdocid}")
                                    if sgdocidc is not None:
                                        grabarcedencedulasconqr(int(fila[0]), int(fila[2]), str(fila[22]).strip(), sgdocidc, pgsql_config)
                                        formularioqr = obtener_formulario_qr(int(fila[0]), int(fila[2]), str(fila[22]).strip(), sgdocid, pgsql_config, urlpj='https://appweb.justiciasalta.gov.ar:8091/policia/api/cnotpolicia/incrustarqrpdf')
                        else:
                            print("No se pudo completar la solicitud.")
                        pmovimientoid = datos_insertar['pmovimientoid']
                        pactuacionid = datos_insertar['pactuacionid']
                        pdomicilioelectronicopj = datos_insertar['pdomicilioelectronicopj']
            except Exception as e:
                errores.append(f"Error al procesar fila {fila[1]}: {e}")
                print(f"Error al procesar fila {fila[1]}: {e}")

        if actualizar:
            print(f"conexion: {panel_config}")
            registrar_paso("paso21", 21, panel_config)
        conn.close()
        if errores:
            print("Algunos registros no pudieron procesarse:")
            for error in errores:
                print(error)
        else:
            print("Todos los registros procesados correctamente.")
    except Exception as e:
        print(f"Error general: {e}")


def insertar_documento(base64_data, nombre_archivo, numero_legajo, test=True):
    #print("algo")
    sdt_gestion_documento = {
        "sgdDocNombre": f"SIAN_VL_{nombre_archivo}.strip()",
        "sgdDocTipo": "pdf",
        "sgdDocUsuarioAlta": str(numero_legajo).strip(),
        "sgdDocDocumentoBase64": base64_data,
        "sgdDocPublico": "true",
        "sgdDocFisico": "true",
        "sgdDocAppOrigen": "SIAN"
    }
    data = {"sdtGestionDocumento": sdt_gestion_documento}
    if test:
        url = "http://10.19.240.200:8080/gestor/API/gestiondocumento/InsertarDocumento"
    else:
        url = "https://appintra.justiciasalta.gov.ar:8092/gestor/API/gestiondocumento/InsertarDocumento"
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error al llamar al webservice: {e}")
        return None


def grabarcedencedulasconqr(pmovimientoid, pactuacionid, pdomicilioelectronicopj, sgdocid, pgsql_config):

    connrp = psycopg2.connect(**pgsql_config)
    cursor = connrp.cursor()
    try:
        with connrp.cursor() as cursor:
            ahora = datetime.now()
            query = f"""
            update cedulasconcodigoqr 
            set uidgestorcedula = '{sgdocid}'
            where pmovimientoid = {pmovimientoid} and
            pactuacionid = {pactuacionid} and
            pdomicilioelectronicopj = '{pdomicilioelectronicopj}'         ;
            """
            cursor.execute(query)
            print(f"Datos generados en tabla de qrs")
            connrp.commit()
    except Exception as e:
        print(f"Error al registrar paso {e}")


def grabarcedulasconqr(pmovimientoid, pactuacionid, pdomicilioelectronicopj, sgdocid, pgsql_config):

    connrp = psycopg2.connect(**pgsql_config)
    cursor = connrp.cursor()
    try:
        with connrp.cursor() as cursor:
            ahora = datetime.now()
            query = f"""
            insert into cedulasconcodigoqr (pmovimientoid,pactuacionid,pdomicilioelectronicopj,uidgestor) values ({pmovimientoid},{pactuacionid},'{pdomicilioelectronicopj}','{sgdocid}')         ;
            """
            cursor.execute(query)
            print(f"Datos generados en tabla de qrs")
            connrp.commit()
    except Exception as e:
        print(f"Error al registrar paso {e}")


def obtener_formulario_qr(pmovimientoid, pactuacionid, pdomicilioelectronicopj, sgdocid, pgsql_config, urlpj):
    print("Ingrese a obtener_formulario_qr")
    connrp = psycopg2.connect(**pgsql_config)
    cursor = connrp.cursor()
    payload = {
        "cedulas": {
            "pMovimientoId": str(pmovimientoid),
            "pActuacionId": str(pactuacionid),
            "pDomicilioElectronicoPj": pdomicilioelectronicopj,
            "uidgestor": sgdocid
        }
    }
    headers = {'Content-Type': 'application/json'}
    try:
        print("Enviando solicitud QR...")
        response = requests.post(urlpj, headers=headers, json=payload)

        print(f"Código de estado: {response.status_code}")
        print(f"Contenido de la respuesta: {response.text}")

        if response.status_code == 200:
            json_response = response.json()

            errores = json_response.get("errores")

            if errores is not None:
                raise Exception(f"Errores reportados: {errores}")

            print("✅ La tarea de envío de cédulas se completó con éxito.")

            fqr = json.dumps(json_response, indent=2)

            datosconqr = json.loads(fqr)

            formularioqr = datosconqr.get("base64")

            try:
                updateqr = f"""
                update cedulasconcodigoqr set pdfgenerado = '{formularioqr}' where pmovimientoid = {pmovimientoid} and pactuacionid = {pactuacionid} and pdomicilioelectronicopj = '{pdomicilioelectronicopj}'         ;
                """
                connrp = psycopg2.connect(**pgsql_config)
                cursor = connrp.cursor()
                cursor.execute(updateqr)
                connrp.commit()
                print("paso1")
                insertadjuntos = f"""
                insert into adjuntospolicia  (pmovimientoid, pactuacionid, pdomicilioelectronicopj ,adjuntospolicianombre,adjuntospoliciabase64) 
                values({pmovimientoid},{pactuacionid},'{pdomicilioelectronicopj}',(select parchivoactnombre from enviocedulanotificacionpolicia en where 
                                en.pmovimientoid = {pmovimientoid} and
                                en.pactuacionid = {pactuacionid} and
                                en.pdomicilioelectronicopj = '{pdomicilioelectronicopj}'), '{formularioqr}')"""

                connrp = psycopg2.connect(**pgsql_config)
                cursor = connrp.cursor()
                cursor.execute(insertadjuntos)
                connrp.commit()

                print("paso2")
                updateec = f"""
                update enviocedulanotificacionpolicia set pactuacionarchivo = '{formularioqr}' where pmovimientoid = {pmovimientoid} and pactuacionid = {pactuacionid} and pdomicilioelectronicopj = '{pdomicilioelectronicopj}'         ;
                """

                connrp = psycopg2.connect(**pgsql_config)
                cursor = connrp.cursor()
                cursor.execute(updateec)

                print("paso 3")

                connrp.commit()

            except Exception as e:
                print(f"Error al crear Formulario QR: {e}")

        else:
            raise Exception(f"Error de conexión: Código HTTP {response.status_code}, Respuesta: {response.text}")

    except Exception as e:
        print(f"Error al llamar al servicio web de envío de cédulas: {e}")


def registrar_error(dbgusername, dbguservalor, dbguserprograma):
    print(f"Registrar  Error")
    panel_test = {
            "host": "10.18.250.251",
            "port": "5432",
            "database": "panelnotificacionesws",
            "user": "cmayuda",
            "password": "power177"
            }
    connpanel = psycopg2.connect(**panel_test)
    try:
        query=f"insert into dbguser (dbguservalor,dbguserprograma) values ('{dbguservalor}','{dbguserprograma}')"
        print(f"Error {query}")
        logserror = connpanel.cursor()
        logserror.execute(query)
    except requests.exceptions.RequestException as e:
        print(f"Error al insertar error '{e}'")
        return None


def ejecutar_control_cedulas(pgsql_config, pmovimientoid, pactuacionid, pdomicilioelectronicopj):
    try:
        conexion = psycopg2.connect(**pgsql_config)
        cursor = conexion.cursor()
        cursor.execute(f"""SELECT * FROM enviocedulanotificacionpolicia where penviocedulanotificacionexito = true and pmovimientoid = {pmovimientoid} and pactuacionid = {pactuacionid} and pdomicilioelectronicopj = '{pdomicilioelectronicopj}'""")
        registros = cursor.fetchall()

        for registro in registros:
            pmovimientoid = registro[0]
            pactuacionid = registro[1]
            pdomicilioelectronicopj = registro[2]
            root = ET.fromstring(registro[5])
            for child in root:
                for subchild in child:
                    if subchild.tag == "{http://tempuri.org/}CodigosSeguimiento":
                        for codigo in subchild:
                            print(f"{registro[26]}  Codigo: {codigo.text}")
                            update_query = f"UPDATE enviocedulanotificacionpolicia SET codigoseguimientomp = '{codigo.text}' WHERE pmovimientoid = {pmovimientoid} and pactuacionid = {pactuacionid} and pdomicilioelectronicopj = '{pdomicilioelectronicopj}'"
                            print(update_query)
                            cursor.execute(update_query)
                            conexion.commit()

        print("control 1 ok")

    except Exception as e:
        print("tabla con errores")
        print(f"Error: {e} / {registro[1]} ")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conexion' in locals():
            conexion.close()
