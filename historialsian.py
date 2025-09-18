import threading
import time
import tkinter as tk
from tkinter import ttk
import psycopg2
import jaydebeapi

import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import json
import requests
import base64
import sys
from datetime import datetime
import xml.etree.ElementTree as ET

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

def lasstage(pmovimientoid,pactuacionid,pdomicilioelectronicopj,CODIGO_SEGUIMIENTO):

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
	#CODIGO_SEGUIMIENTO = "C0D7BA"  # Ejemplo, reemplaza con el real

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


	#print(xml_data)

	
	# Encabezados HTTP
	headers = {
	    "Content-Type": "text/xml; charset=UTF-8",
	    "SOAPAction": "http://tempuri.org/ObtenerEstadoNotificacion"  # A veces es obligatorio
	}

	try:
	    # Realizar la petición
		response = requests.post(URL, data=xml_data, headers=headers, verify=False)  # verify=False si el certificado no es válido

	    # Verificar respuesta
		if response.status_code == 200:
			#print("✅ Respuesta recibida con éxito")
			#print(response.text)  # Mostrar XML de respuesta
			root = ET.fromstring(response.text)


			for n1 in root:
				#print(f'n1-------------')
				#print(f'*Tag: {n1.tag}, Atributos: {n1.attrib}')
				
				for n2 in n1:
					#print(f'n2-------------')
					#print(f'**Tag: {n2.tag}, Atributos: {n2.attrib}, valor: {n2.text}')

					for n3 in n2:
						#print(f'n3-------------')
						#print(f'***Tag: {n3.tag}, Atributos: {n3.attrib}, valor: {n3.text}')
						for n4 in n3:
							#print(f'n4-------------')
							#print(f'****Tag: {n4.tag}, Atributos: {n4.attrib}, valor: {n4.text}')
							for n5 in reversed(n4):
								#print(f'n5-------------')
								#print(f'*****Tag: {n5.tag}, Atributos: {n5.attrib}, valor: {n5.text}')
								archivoid = 0
								archivonombre = ""
								estadonotificacionid = 0
								archivocontenido = ""
								fecha = ""
								estado = ""
								observaciones = ""
								motivo = ""
								responsablenotificacion = ""
								dependencianotificacion = ""
								archivoid = 0
								archivonombre = ""
								archivocontenido = ""
								for n6 in n5:
									
									
									if n6.tag == '{http://tempuri.org/}Estado':	
										print(f'******Tag: {n6.tag}, Atributos: {n6.attrib}, valor: {n6.text}')
										estado = n6.text	
										return estado
								break	
											

		else:
			print(f"⚠️ Error en la respuesta: Código {response.status_code}")
			print(response.text)

	except requests.exceptions.RequestException as e:
	    print(f"❌ Error en la solicitud: {e}")




def pre_historial():
    try:
        # Conectar a la base de datos
        conexion = psycopg2.connect(**pgsql_config)
        cursor = conexion.cursor()


        update_query = f"update enviocedulanotificacionpolicia set finsian = True where descartada = True and finsian <> True"
        cursor.execute(update_query)
        conexion.commit()

        update_query = f"update enviocedulanotificacionpolicia set descartada= false, laststagesian = 'Sin info' where penviocedulanotificacionfechahora >= current_date - INTERVAL '1 days' and coalesce(descartada,false) = false"
        cursor.execute(update_query)
        conexion.commit()
        cursor.execute("""
            SELECT e.pmovimientoid, e.pactuacionid, e.pdomicilioelectronicopj, e.codigoseguimientomp  
            FROM enviocedulanotificacionpolicia e 
            WHERE e.codigoseguimientomp IS NOT NULL and upper(trim(e.codigoseguimientomp)) <> 'NONE'
            AND  e.finsian = false
            AND  e.pdac_codigo = 'CEDURG'
            AND  e.penviocedulanotificacionfechahora >= current_date - INTERVAL '15 days'
        """)
        registros = cursor.fetchall()
        #print(registros)
        for notas in registros:
            try:
                llamar_his_mp(notas[0], notas[1], notas[2], notas[3])
            except Exception as e:
                print(f"Error procesando registro {notas}: {e}")

        conexion.commit()        
        cursor.execute("""
            SELECT e.pmovimientoid, e.pactuacionid, e.pdomicilioelectronicopj, e.codigoseguimientomp  
            FROM enviocedulanotificacionpolicia e 
            WHERE e.codigoseguimientomp IS NOT NULL and upper(trim(e.codigoseguimientomp)) <> 'NONE'
            AND  e.finsian = false
            AND  e.pdac_codigo <> 'CEDURG'
            AND  e.penviocedulanotificacionfechahora >= current_date - INTERVAL '5 days'
        """)
        registros = cursor.fetchall()
        #print(registros)
        for notas in registros:
            try:
                llamar_his_mp(notas[0], notas[1], notas[2], notas[3])
            except Exception as e:
                print(f"Error procesando registro {notas}: {e}")
        conexion.commit()        

    except Exception as e:
        print("Error en la conexión o consulta SQL")
        print(f"Error: {e}")

    finally:
        # Cerrar la conexión
        if 'cursor' in locals():
            cursor.close()
        if 'conexion' in locals():
            conexion.close()
				
	

def llamar_his_mp(pmovimientoid,pactuacionid,pdomicilioelectronicopj,CODIGO_SEGUIMIENTO):
	
	try:
		estado = lasstage(pmovimientoid,pactuacionid,pdomicilioelectronicopj,CODIGO_SEGUIMIENTO)
		grabar_historico(estado,pmovimientoid,pactuacionid,pdomicilioelectronicopj,CODIGO_SEGUIMIENTO)
	except requests.exceptions.RequestException as e:
	    print(f"❌ Error en la solicitud: {e}")




def completar_archivo(CODIGO_SEGUIMIENTO,archivocontenido):		    
	conexion = psycopg2.connect(**pgsql_config)
	cursor = conexion.cursor()
	#print(f"update enviocedulanotificacionpolicia set ecedarchivoseguimientoid = {archivoid},ecedarchivoseguimientonombre = '{archivonombre}' ,ecedarchivosegnotid = {estadonotificacionid}		where codigoseguimientomp = '{CODIGO_SEGUIMIENTO}'")
	insert_query = f"update enviocedulanotificacionpolicia set ecedarchivoseguimientodatos = '{archivocontenido}'	where codigoseguimientomp = '{CODIGO_SEGUIMIENTO}'"
	#print(insert_query)
	cursor.execute(insert_query)
	conexion.commit()


def completar_envio(archivoid,archivonombre,CODIGO_SEGUIMIENTO,estadonotificacionid):		    
	conexion = psycopg2.connect(**pgsql_config)
	cursor = conexion.cursor()
	#print(f"update enviocedulanotificacionpolicia set ecedarchivoseguimientoid = {archivoid},ecedarchivoseguimientonombre = '{archivonombre}' ,ecedarchivosegnotid = {estadonotificacionid}		where codigoseguimientomp = '{CODIGO_SEGUIMIENTO}'")
	insert_query = f"update enviocedulanotificacionpolicia set ecedarchivoseguimientoid = {archivoid},ecedarchivoseguimientonombre = '{archivonombre}' ,ecedarchivosegnotid = {estadonotificacionid}		where codigoseguimientomp = '{CODIGO_SEGUIMIENTO}'"
	cursor.execute(insert_query)
	conexion.commit()



def grabar_historico(estado, pmovimientoid, pactuacionid, pdomicilioelectronicopj, CODIGO_SEGUIMIENTO):
    try:
        conexion = psycopg2.connect(**pgsql_config)
        cursor = conexion.cursor()
        
        print(f"{CODIGO_SEGUIMIENTO} -> {estado}")

        if estado.upper() in ('ENTREGADA','NO ENTREGADA','DESCARTADA','FINALIZADA'):
            finsian = True
            print(f"{CODIGO_SEGUIMIENTO} -> Finalizada")
        else:
            finsian = False
	
        update_query = f"update enviocedulanotificacionpolicia set laststagesian = '{estado}', finsian = {finsian} where codigoseguimientomp = '{CODIGO_SEGUIMIENTO}'"
        cursor.execute(update_query)
        conexion.commit()


    
    except psycopg2.Error as e:
        print(f"Error al insertar en la base de datos: {e} - {valores}")
    
    finally:
        if cursor:
            cursor.close()
        if conexion:
            conexion.close()
if __name__ == "__main__":
	connpanel = psycopg2.connect(**panel_config)
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

