#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <WiFiManager.h> 

// Configuración de AWS IoT Core
const char* MQTT_SERVER = "aleas6fbrinez-ats.iot.us-east-1.amazonaws.com";
const int MQTT_PORT = 8883;

// Certificados y claves
const char* rootCA = R"EOF(
-----BEGIN CERTIFICATE-----
MIIDQTCCAimgAwIBAgITBmyfz5m/jAo54vB4ikPmljZbyjANBgkqhkiG9w0BAQsF
ADA5MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6
b24gUm9vdCBDQSAxMB4XDTE1MDUyNjAwMDAwMFoXDTM4MDExNzAwMDAwMFowOTEL
MAkGA1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEZMBcGA1UEAxMQQW1hem9uIFJv
b3QgQ0EgMTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALJ4gHHKeNXj
ca9HgFB0fW7Y14h29Jlo91ghYPl0hAEvrAIthtOgQ3pOsqTQNroBvo3bSMgHFzZM
9O6II8c+6zf1tRn4SWiw3te5djgdYZ6k/oI2peVKVuRF4fn9tBb6dNqcmzU5L/qw
IFAGbHrQgLKm+a/sRxmPUDgH3KKHOVj4utWp+UhnMJbulHheb4mjUcAwhmahRWa6
VOujw5H5SNz/0egwLX0tdHA114gk957EWW67c4cX8jJGKLhD+rcdqsq08p8kDi1L
93FcXmn/6pUCyziKrlA4b9v7LWIbxcceVOF34GfID5yHI9Y/QCB/IIDEgEw+OyQm
jgSubJrIqg0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMC
AYYwHQYDVR0OBBYEFIQYzIU07LwMlJQuCFmcx7IQTgoIMA0GCSqGSIb3DQEBCwUA
A4IBAQCY8jdaQZChGsV2USggNiMOruYou6r4lK5IpDB/G/wkjUu0yKGX9rbxenDI
U5PMCCjjmCXPI6T53iHTfIUJrU6adTrCC2qJeHZERxhlbI1Bjjt/msv0tadQ1wUs
N+gDS63pYaACbvXy8MWy7Vu33PqUXHeeE6V/Uq2V8viTO96LXFvKWlJbYK8U90vv
o/ufQJVtMVT8QtPHRh8jrdkPSHCa2XV4cdFyQzR1bldZwgJcJmApzyMZFo6IQ6XU
5MsI+yMRQ+hDKXJioaldXgjUkK642M4UwtBV8ob2xJNDd2ZhwLnoQdeXeGADbkpy
rqXRfboQnoZsG4q5WTP468SQvvG5
-----END CERTIFICATE-----
)EOF";

const char* certificate = R"KEY(
-----BEGIN CERTIFICATE-----
MIIDWTCCAkGgAwIBAgIUPfGkG1fMQgEfGUS++2Rop3HoWlcwDQYJKoZIhvcNAQEL
BQAwTTFLMEkGA1UECwxCQW1hem9uIFdlYiBTZXJ2aWNlcyBPPUFtYXpvbi5jb20g
SW5jLiBMPVNlYXR0bGUgU1Q9V2FzaGluZ3RvbiBDPVVTMB4XDTI0MTEwNDIyNTYx
MVoXDTQ5MTIzMTIzNTk1OVowHjEcMBoGA1UEAwwTQVdTIElvVCBDZXJ0aWZpY2F0
ZTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANBK5yeDD4wfObfoaJWf
Qwh89AboHOFSsdd7TabFZh9ZAugcwJevOKFJixrteVqqs7+8SYGRNtc+eNxeg4Se
yFy/GRgr0n6FIY17ov2EY0Nl6QScp5GDXa3Wh/QdrexGFNc0TZrdswra4qHzXNXd
hIBE0+nzPOujlvgo9eRDWQb47J4XtCEHZ9MIELhe1CUWgddwl8sHGIdeaJuURfTb
aClqjkY8YFAfv+V2xK/gczMkMHcWWOS3ssHDrxPM1IDtvnd1FpP/Q+N1EcxXw57S
n7DAXYTR97u04tBD5fbIeiyEKtKUdPE9pqiw0HgbTj1CEZZP/D5UFVJed0Nbwsuj
r9MCAwEAAaNgMF4wHwYDVR0jBBgwFoAUFFKQuXzQWbDydenlL0ZQAB1s8C4wHQYD
VR0OBBYEFLyWxl69DOQhQW4k8s9H46l7R1d0MAwGA1UdEwEB/wQCMAAwDgYDVR0P
AQH/BAQDAgeAMA0GCSqGSIb3DQEBCwUAA4IBAQAxyEJ9edSk6UxmSMh/I3cIvKC1
3K1ri9zNdj5ezXdaXtRlDwObOz6eEC0bpuCIw31FSpTQUOj5Vs1x7cT3JeFVhBIi
z2gIgIkkBJWDKyFl5uxjo8Gc6GJPY3Al1/z8U/C+lui8YTA5n0KoxqZ3DLABFlTX
YUdXC0GdKpRkF/rHJbcJx2R5qndIyTtGdQjeQKKbPzVnuVlugrcl/5/wEjedbEsM
d9MoIkG/d1N67WKNP5LmJDCD57NKDMm71XT0pCTX1uTFlJ/1P9n/C/flv99g5PUo
hkpS+kRzc4C+yUrOpmHxjgQxUuylg04R9C/9juFBDNZnt3SG+fVBIuY9ojbn
-----END CERTIFICATE-----
)KEY";

const char* privateKey = R"KEY(
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA0ErnJ4MPjB85t+holZ9DCHz0Bugc4VKx13tNpsVmH1kC6BzA
l684oUmLGu15Wqqzv7xJgZE21z543F6DhJ7IXL8ZGCvSfoUhjXui/YRjQ2XpBJyn
kYNdrdaH9B2t7EYU1zRNmt2zCtriofNc1d2EgETT6fM866OW+Cj15ENZBvjsnhe0
IQdn0wgQuF7UJRaB13CXywcYh15om5RF9NtoKWqORjxgUB+/5XbEr+BzMyQwdxZY
5LeywcOvE8zUgO2+d3UWk/9D43URzFfDntKfsMBdhNH3u7Ti0EPl9sh6LIQq0pR0
8T2mqLDQeBtOPUIRlk/8PlQVUl53Q1vCy6Ov0wIDAQABAoIBAGHLaTBB9qDBYGYa
dlAZkG2BzUkY3cZk/x6+w7yTXdlF/3lfVoVhPWNsliU0bg/FqdNR4ek1dtwkdrZw
oas4qbXx+yRAHvpMI268J9CDwd85D+icbIiDdw5RdU+GR8xxMwO/iNg3UwK7tkB3
dJTtBnL63cwv6eBw1Eb7ZEeh30DH8MXsxLgmutmDBIODtz1Rgf6aXXjWFnWYQ230
Kf6S4lJjqqjKvIWMJVD46uf3egk0vDIYVrSPr26Z17BePHpIGpImYT9yHjwMemYb
sGSVvc+dzgfsozdBdtRnXntGN72QdoQTCptjKpB+vxDz/6lyACZJ+2lkt6rDwPGu
ioPCI0ECgYEA/9nAlJVIuSl/hzcUBELcQxDj8kUWL19CkKGoD8QAG0NolAggZw+Q
zplZlOwEF6Sq5wh4j6PJ+h6oTeWhQtwOzSXTHVVnTpv2mbHwsstRaWWAonIthy47
fjU/pZP4vePILmr9wm1OCcxMpPtf8hgSPtZM/PdjbBvOdwY6wS1UdlECgYEA0GoK
hp7VxQpcNYVIqMWmsOwV1t3AdlAatCH+hAIPDwWbPGoXvHtoMsxNrOMsXFl8pywv
yrXeS+DhDjJuOFXHMN0DvHYojwtWA7zWZaLIbRwtw0/JiVRx1oEsdka30OK/pWAJ
uJoVzQ1M/pze5aBKDPPxWFZdKK05ZPI9CtnS5uMCgYB0cyLxPeVEybuOyqXkrp3c
NKLbkVBgrWX5uGprCpXV0t+ViQFehzosnqWkX6wZKszSrQtarXzvx+Zo1hyI9uoR
u6aUUlvb7qbWG6RnbJ0YcKeUyI0qWwOfFRNsBKaRn0xsvCvVw7RiR0eXTAbGhOhB
C38tIFhzS1C04fP6Guy3kQKBgDr82G/TwuNbFFAdojwKfSx0FZZT+yZBc91qBbRt
NL/msVI/IOq67vn5sz5sqeCVf199dSVlpj5Jrskq4uFU/eTJmUYdF0utRLIYH4Jy
uVGQeS9fhMY2vWWd9+yeBWa81stCzF2QVv1Ld3BVDA1n+a2C2dtLzmA17xmwKjDJ
CBVLAoGBAL142A1+mYqyKhB96eNnweKRRctEDh4sxX+hGRIAxGiMuNbzWpVXMIw1
PVjYqob1DGrp12StIWqvQkxn+PtuGXtzFmXxcdCmjZUSlSBRgzavOC55R3ngg4YH
mwj7EryYtPLdHDIH/TJrYFpHur7sv9L0UY+jUwhMv4WFgp/ULZFD
-----END RSA PRIVATE KEY-----
)KEY";


#define SHADOW_UPDATE_TOPIC "$aws/things/proyecto_03/shadow/update"
#define SHADOW_UPDATE_ACCEPTED_TOPIC "$aws/things/proyecto_03/shadow/update/accepted"
#define SHADOW_GET_TOPIC "$aws/things/proyecto_03/shadow/get"
#define SHADOW_GET_ACCEPTED_TOPIC "$aws/things/proyecto_03/shadow/get/accepted"

class IrrigationSystem {
  private:
    const int humidity_sensor_pin = 32;  // Pin del sensor de humedad
    const int pump_pin = 5;              // Pin de la bomba
    const int humidity_threshold = 1300; // Umbral de humedad
    unsigned long last_check_time = 0;   // Última vez que se verificó la memoria
    const int check_interval = 5000;     // Intervalo de verificación en milisegundos

    WiFiClientSecure wifi_client;
    PubSubClient mqtt_client;

  public:
    // Constructor para inicializar el cliente MQTT con el cliente WiFi
    IrrigationSystem() : mqtt_client(wifi_client) {}

    // Configura la conexión Wi-Fi utilizando WiFiManager
    void setupWiFi() {
      WiFiManager wifiManager;

      // Configurar un timeout para el portal de configuración
      wifiManager.setConfigPortalTimeout(180); // 180 segundos

      // Intentar conectar a una red conocida o lanzar el portal de configuración
      if (!wifiManager.autoConnect("ESP32_ConfigAP")) {
        Serial.println("No se pudo conectar a WiFi. Reiniciando...");
        ESP.restart();
      }

      Serial.println("Conexión WiFi establecida.");
    }

    // Conectar a AWS IoT Core
    void connectAWS() {
      wifi_client.setCACert(rootCA);
      wifi_client.setCertificate(certificate);
      wifi_client.setPrivateKey(privateKey);

      mqtt_client.setServer(MQTT_SERVER, MQTT_PORT);
      mqtt_client.setCallback([this](char* topic, byte* payload, unsigned int length) { 
        this->messageCallback(topic, payload, length); 
      });

      Serial.print("Conectando a AWS IoT Core...");
      while (!mqtt_client.connected()) {
        if (mqtt_client.connect("ESP32Client")) {
          Serial.println("Conectado a AWS IoT Core.");
          subscribeTopics();
        } else {
          Serial.print("Conexión fallida, rc=");
          Serial.print(mqtt_client.state());
          Serial.println(" Intentando de nuevo en 5 segundos.");
          delay(5000);
        }
      }
      mqtt_client.publish(SHADOW_GET_TOPIC, ""); // Solicita el estado actual del shadow
    }

    // Suscribirse a los tópicos de AWS IoT Core
    void subscribeTopics() {
      mqtt_client.subscribe(SHADOW_UPDATE_ACCEPTED_TOPIC);
      mqtt_client.subscribe(SHADOW_GET_ACCEPTED_TOPIC);
    }

    // Callback para manejar los mensajes MQTT
    void messageCallback(char* topic, byte* payload, unsigned int length) {
      String message;
      for (int i = 0; i < length; i++) {
        message += (char)payload[i];
      }

      DynamicJsonDocument doc(512);
      DeserializationError error = deserializeJson(doc, message);
      if (error) {
        Serial.print("Error al deserializar JSON: ");
        Serial.println(error.c_str());
        return;
      }

      if (String(topic) == SHADOW_UPDATE_ACCEPTED_TOPIC) {
        const char* desired_state = doc["state"]["desired"]["bomba"];
        if (desired_state) {
          controlPump(desired_state);
        }
      }
    }

    // Controlar el estado de la bomba
    void controlPump(const char* state) {
      if (String(state) == "ON") {
        digitalWrite(pump_pin, HIGH);
        Serial.println("Bomba activada.");
        updateShadow("ON");
      } else if (String(state) == "OFF") {
        digitalWrite(pump_pin, LOW);
        Serial.println("Bomba desactivada.");
        updateShadow("OFF");
      }
    }

    // Actualizar el estado de la bomba en el shadow de AWS
    void updateShadow(const char* pump_state) {
      DynamicJsonDocument shadow_doc(256);
      shadow_doc["state"]["reported"]["bomba"] = pump_state;
      String output;
      serializeJson(shadow_doc, output);
      mqtt_client.publish(SHADOW_UPDATE_TOPIC, output.c_str());
    }

    // Reportar el nivel de humedad al shadow de AWS
    void reportHumidity() {
      int humidity = analogRead(humidity_sensor_pin);
      DynamicJsonDocument doc(256);
      doc["state"]["reported"]["humedad"] = humidity;
      doc["state"]["reported"]["limite_humedad"] = humidity_threshold;
      String output;
      serializeJson(doc, output);
      mqtt_client.publish(SHADOW_UPDATE_TOPIC, output.c_str());
      Serial.print("Enviando nivel de humedad: ");
      Serial.println(humidity);
    }

    // Ciclo principal
    void loop() {
      if (!mqtt_client.connected()) {
        connectAWS();
      }
      mqtt_client.loop();

      if (millis() - last_check_time > check_interval) {
        last_check_time = millis();
        int free_heap = ESP.getFreeHeap();
        Serial.print("Memoria libre (heap): ");
        Serial.println(free_heap);
      }
    }
};

IrrigationSystem irrigation_system;

void setup() {
  Serial.begin(115200);
  pinMode(irrigation_system.humidity_sensor_pin, INPUT);
  pinMode(irrigation_system.pump_pin, OUTPUT);
  digitalWrite(irrigation_system.pump_pin, LOW); // Asegurarse que la bomba esté apagada
  irrigation_system.setupWiFi();
  irrigation_system.connectAWS();
}

void loop() {
  irrigation_system.loop();
}