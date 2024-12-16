import json
import boto3

# IoT Client
iot_client = boto3.client('iot-data', endpoint_url='https://aleas6fbrinez-ats.iot.us-east-1.amazonaws.com')

class IrrigationControl:
    def __init__(self, thing_name):
        self.thing_name = thing_name

    def get_humidity_and_limit(self, event):
        humidity = event.get('state', {}).get('reported', {}).get('humidity', None)
        humidity_limit = event.get('state', {}).get('reported', {}).get('humidity_limit', None)
        return humidity, humidity_limit

    def validate_data(self, humidity, humidity_limit):
        if humidity is None or humidity_limit is None:
            return False
        return True

    def control_pump(self, humidity, humidity_limit):
        if humidity >= humidity_limit:
            payload = {"state": {"desired": {"pump": "ON"}}}
            iot_client.update_thing_shadow(thingName=self.thing_name, payload=json.dumps(payload))
            return 'Pump activated'
        else:
            payload = {"state": {"desired": {"pump": "OFF"}}}
            iot_client.update_thing_shadow(thingName=self.thing_name, payload=json.dumps(payload))
            return 'Pump deactivated'

def lambda_handler(event, context):

    # Create an instance of the IrrigationControl class
    irrigation_control = IrrigationControl(thing_name="proyecto_03")

    # Print the received payload
    print("Received payload:", json.dumps(event))

    # Get humidity and humidity limit from the event
    humidity, humidity_limit = irrigation_control.get_humidity_and_limit(event)

    # Validate the data
    if not irrigation_control.validate_data(humidity, humidity_limit):
        return {
            'statusCode': 400,
            'body': json.dumps('Missing humidity or humidity limit data')
        }

    # Control the pump based on humidity and humidity limit
    result = irrigation_control.control_pump(humidity, humidity_limit)
    
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
