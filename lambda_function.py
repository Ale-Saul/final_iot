import os
import json
import logging
import boto3
import time
from botocore.exceptions import ClientError
from decimal import Decimal  # Importar Decimal para manejar números decimales
from ask_sdk_core.skill_builder import SkillBuilder
from ask_sdk_core.dispatch_components import AbstractRequestHandler, AbstractExceptionHandler
from ask_sdk_core.handler_input import HandlerInput
from ask_sdk_core.utils import get_request_type, get_intent_name
from ask_sdk_model import Response

# Configuración de logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Crear el cliente de SSM (AWS Systems Manager) antes de usarlo
ssm_client = boto3.client('ssm')

# Función para obtener parámetros del Parameter Store
def get_parameter(name):
    try:
        response = ssm_client.get_parameter(Name=name, WithDecryption=True)
        return response['Parameter']['Value']
    except ClientError as e:
        logger.error(f"Error al obtener el parámetro {name}: {e}")
        raise Exception(f"Error al obtener el parámetro {name}: {e}")

# Obtener el nombre del thing y el endpoint desde el Parameter Store
thing_name = get_parameter('/sistema_riego/thing_name')
endpoint_url = get_parameter('/sistema_riego/endpoint_url')
db = get_parameter('/sistema_riego/database')

# Crear cliente IoT con el endpoint dinámico
iot_client = boto3.client('iot-data', endpoint_url=endpoint_url)

# Configura el cliente DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(db)  

# Tasa de flujo de agua en litros por minuto
FLOW_RATE = 5

# Función para insertar datos en DynamoDB
def insertar_datos_riego(evento_id, start_time, end_time, duration, water_volume, soil_moisture, thing_id, system_status, request_time, user_id):
    try:
        # Convertir los valores flotantes a Decimal
        item = {
            'ID': evento_id,
            'StartTime': start_time,
            'EndTime': end_time,
            'Duration': Decimal(str(duration)) if duration is not None else None,  # Convertir a Decimal
            'WaterVolume': Decimal(str(water_volume)) if water_volume is not None else None,  # Convertir a Decimal
            'SoilMoisture': soil_moisture if soil_moisture is not None else None, 
            'ThingID': thing_id,
            'SystemStatus': system_status,
            'RequestTime': request_time,
            'UserID': user_id
        }
        table.put_item(Item=item)
        logger.info(f"Datos insertados en DynamoDB: {item}")
    except Exception as e:
        logger.error(f"Error al insertar datos en DynamoDB: {e}")

# Variables globales para registrar el inicio del riego
start_time_global = None

# Manejador de inicio de la skill
class LaunchRequestHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return get_request_type(handler_input) == "LaunchRequest"

    def handle(self, handler_input):
        speak_output = 'Bienvenido al sistema de riego. Puedes decir "activar el regador", "desactivar el regador", o "consultar humedad".'
        return handler_input.response_builder.speak(speak_output).reprompt(speak_output).response

# Manejador para activar el regador
class ActivarRegadorIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return get_intent_name(handler_input) == "ActivarRegadorIntent"

    def handle(self, handler_input):
        global start_time_global
        payload = {"state": {"desired": {"bomba": "ON"}}}
        start_time_global = time.time()  # Registra el tiempo de inicio en segundos
        try:
            iot_client.update_thing_shadow(thingName=thing_name, payload=json.dumps(payload))
            speak_output = 'El regador ha sido activado.'
        except Exception as e:
            logger.error(f"Error al activar el regador: {e}")
            speak_output = 'Hubo un problema al activar el regador, por favor intenta nuevamente.'
        return handler_input.response_builder.speak(speak_output).response

# Manejador para desactivar el regador
class DesactivarRegadorIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return get_intent_name(handler_input) == "DesactivarRegadorIntent"

    def handle(self, handler_input):
        global start_time_global
        payload = {"state": {"desired": {"bomba": "OFF"}}}
        end_time = time.time()  # Tiempo actual en segundos
        user_id = handler_input.request_envelope.context.system.user.user_id
        try:
            iot_client.update_thing_shadow(thingName=thing_name, payload=json.dumps(payload))
            if start_time_global:
                duration = (end_time - start_time_global) / 60  # Duración en minutos
                water_volume = duration * FLOW_RATE  # Volumen de agua
                evento_id = f"{int(end_time)}-{thing_name}"  # ID único basado en el tiempo y el dispositivo
                time.sleep(1)
                response = iot_client.get_thing_shadow(thingName=thing_name)
                json_state = json.loads(response["payload"].read())
                humedad = json_state["state"]["reported"].get("humedad", None)

                # Insertar datos en DynamoDB
                insertar_datos_riego(
                    evento_id=evento_id,
                    start_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time_global)),
                    end_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end_time)),
                    duration=round(duration, 2),
                    water_volume=round(water_volume, 2),
                    soil_moisture=humedad,  # Solo guardamos humedad explícita cuando se consulta
                    thing_id=thing_name,
                    system_status="Desactivado",
                    request_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end_time)),
                    user_id=user_id
                )
                speak_output = f'El regador ha sido desactivado. Se utilizaron aproximadamente {round(water_volume, 2)} litros de agua.'
                start_time_global = None  # Reiniciar el tiempo de inicio
            else:
                speak_output = 'El regador no estaba activado.'
        except Exception as e:
            logger.error(f"Error al desactivar el regador: {e}")
            speak_output = 'Hubo un problema al desactivar el regador, por favor intenta nuevamente.'
        return handler_input.response_builder.speak(speak_output).response

# Manejador para consultar la humedad
class ConsultarHumedadIntentHandler(AbstractRequestHandler):
    def can_handle(self, handler_input):
        return get_intent_name(handler_input) == "ConsultarHumedadIntent"

    def handle(self, handler_input):
        global start_time_global
        user_id = handler_input.request_envelope.context.system.user.user_id
        try:
            iot_client.publish(
                topic="sistema_riego/solicitud_humedad",
                qos=1,
                payload=json.dumps({"message": "SOLICITAR_HUMEDAD"})
            )
            time.sleep(1)  
            response = iot_client.get_thing_shadow(thingName=thing_name)
            json_state = json.loads(response["payload"].read())
            humedad = json_state["state"]["reported"].get("humedad", None)
            
            if humedad is None:
                speak_output = "No puedo obtener la humedad en este momento."
            else:
                estado = "húmedo" if humedad < 1300 else "seco"
                speak_output = f"El nivel de humedad es {humedad}. El suelo está {estado}."
                 # Insertar datos en DynamoDB
                evento_id = f"{time.time()}-{thing_name}"
                request_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time()))  # Hora de la solicitud
                
                if humedad > 1300: # Si la humedad es mayor, guardamos el inicio y la humedad 
                    evento_id = f"{time.time()}-{thing_name}"
                    start_time_global = time.time()  # Registramos la hora de la solicitud
                    insertar_datos_riego(
                        evento_id=evento_id,
                        start_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time_global)),
                        end_time=None,
                        duration=None,
                        water_volume=None,
                        soil_moisture=humedad,
                        thing_id=thing_name,
                        system_status="Consulta Humedad",
                        request_time=request_time,
                        user_id=user_id
                    )
                else:  # Si la humedad es menor
                    if start_time_global:
                        end_time = time.time()  # Hora actual (cuando se consulta la humedad)
                        duration = (end_time - start_time_global) / 60  # Duración en minutos
                        water_volume = duration * FLOW_RATE  # Volumen de agua en litros
                        # Insertar datos con la duración y el volumen de agua
                        insertar_datos_riego(
                            evento_id=evento_id,
                            start_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time_global)),
                            end_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(end_time)),
                            duration=round(duration, 2),
                            water_volume=round(water_volume, 2),
                            soil_moisture=humedad,
                            thing_id=thing_name,
                            system_status="Consulta Humedad",
                            request_time=request_time,
                            user_id=user_id
                        )
                    else:
                        # Si no hay start_time_global, solo guardamos el inicio y la humedad
                        start_time_global = time.time()
                        insertar_datos_riego(
                            evento_id=evento_id,
                            start_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_time_global)),
                            end_time=None,
                            duration=None,
                            water_volume=None,
                            soil_moisture=humedad,
                            thing_id=thing_name,
                            system_status="Consulta Humedad",
                            request_time=request_time,
                            user_id=user_id
                        )

        except Exception as e:
            logger.error(f"Error al obtener la humedad: {e}")
            speak_output = "Hubo un problema al obtener la humedad. Por favor, intenta nuevamente."

        return handler_input.response_builder.speak(speak_output).response

# Manejador de errores
class ErrorHandler(AbstractExceptionHandler):
    def can_handle(self, handler_input, exception):
        return True

    def handle(self, handler_input, exception):
        logger.error(f"Error handled: {exception}")
        speak_output = 'Lo siento, hubo un problema. Por favor intenta nuevamente.'
        return handler_input.response_builder.speak(speak_output).response

# Construcción de la skill
sb = SkillBuilder()
sb.add_request_handler(LaunchRequestHandler())
sb.add_request_handler(ActivarRegadorIntentHandler())
sb.add_request_handler(DesactivarRegadorIntentHandler())
sb.add_request_handler(ConsultarHumedadIntentHandler())
sb.add_exception_handler(ErrorHandler())

lambda_handler = sb.lambda_handler()
