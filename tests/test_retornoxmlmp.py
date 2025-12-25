import unittest
from unittest import mock

import retornoxmlmp


XML_CON_ESTADO = """<?xml version="1.0"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
  <soapenv:Body>
    <tem:ObtenerEstadoNotificacionResponse>
      <tem:ObtenerEstadoNotificacionResult>
        <tem:EstadoNotificacion>
          <tem:EstadoNotificacionId>123</tem:EstadoNotificacionId>
        </tem:EstadoNotificacion>
      </tem:ObtenerEstadoNotificacionResult>
    </tem:ObtenerEstadoNotificacionResponse>
  </soapenv:Body>
</soapenv:Envelope>
"""

XML_ARCHIVO = """<?xml version="1.0"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:tem="http://tempuri.org/">
  <soapenv:Body>
    <tem:ObtenerArchivoEstadoNotificacionResponse>
      <tem:ObtenerArchivoEstadoNotificacionResult>
        <tem:ArchivoId>456</tem:ArchivoId>
        <tem:ArchivoNombre>archivo.pdf</tem:ArchivoNombre>
        <tem:ArchivoContenido>ABCDEF</tem:ArchivoContenido>
      </tem:ObtenerArchivoEstadoNotificacionResult>
    </tem:ObtenerArchivoEstadoNotificacionResponse>
  </soapenv:Body>
</soapenv:Envelope>
"""


class RetornoXmlMpTests(unittest.TestCase):
    def test_invocar_servicio_devuelve_xml_y_datos_archivo(self):
        respuesta = mock.Mock(status_code=200, text=XML_CON_ESTADO, headers={})
        sesion = mock.Mock()
        sesion.post.return_value = respuesta

        with mock.patch.object(retornoxmlmp, "_obtener_sesion_soap", return_value=sesion), \
            mock.patch.object(retornoxmlmp, "_respetar_intervalo_solicitudes"):
            resultado, error = retornoxmlmp._invocar_servicio(
                "ABC123",
                usar_test=True,
                timeout=5,
                max_reintentos=1,
                mostrar_respuesta=False,
            )

        self.assertIsNone(error)
        self.assertIsNotNone(resultado)
        self.assertEqual(resultado.xml_respuesta, XML_CON_ESTADO.strip())
        sesion.post.assert_called_once()
        called_url = sesion.post.call_args.args[0]
        self.assertEqual(
            called_url,
            "https://pruebasian.mpublico.gov.ar/services/wsNotificacion.asmx",
        )
        self.assertIn("ABC123", sesion.post.call_args.kwargs["data"])
        self.assertEqual(
            sesion.post.call_args.kwargs["headers"]["SOAPAction"],
            retornoxmlmp.SOAP_ACTION,
        )

        estado_id = retornoxmlmp._extraer_estado_notificacion_id(resultado.xml_respuesta)
        self.assertEqual(estado_id, "123")

        datos_archivo = retornoxmlmp._extraer_datos_archivo(XML_ARCHIVO)
        self.assertEqual(
            datos_archivo,
            {
                "archivo_id": "456",
                "archivo_nombre": "archivo.pdf",
                "archivo_contenido": "ABCDEF",
            },
        )

    def test_actualizar_datos_archivo_actualiza_bd(self):
        envio = retornoxmlmp.EnvioNotificacion(
            id_envio=1,
            pmovimientoid=10,
            pactuacionid=20,
            pdomicilioelectronicopj="correo@test.com",
            codigoseguimientomp="ABC123",
        )
        conn_pg = mock.MagicMock()
        cursor = conn_pg.cursor.return_value.__enter__.return_value

        with mock.patch.object(
            retornoxmlmp,
            "_invocar_servicio_archivo",
            return_value=(XML_ARCHIVO, None),
        ):
            actualizado = retornoxmlmp._actualizar_datos_archivo(
                conn_pg,
                envio,
                XML_CON_ESTADO,
                usar_test=True,
            )

        self.assertTrue(actualizado)
        cursor.execute.assert_called_once()
        _, params = cursor.execute.call_args.args
        self.assertEqual(
            params,
            (
                123,
                456,
                "archivo.pdf",
                "ABCDEF",
                10,
                20,
                "correo@test.com",
            ),
        )
        conn_pg.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
