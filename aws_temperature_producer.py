import time
import random
import json
import boto3
from botocore.exceptions import ClientError

# Configurar las credenciales directamente
AWS_ACCESS_KEY_ID = ''  # Reemplaza con tu Access Key ID
AWS_SECRET_ACCESS_KEY = ''  # Reemplaza con tu Secret Access Key
AWS_REGION = 'us-east-1'  # Reemplaza con la región de tu stream

# Configurar el cliente de Kinesis con credenciales
kinesis_client = boto3.client(
    'kinesis',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Nombre del stream
stream_name = 'TemperaturaStream'

# Lista de ciudades y unidad de medida
ciudades = ["Aguascalientes", "Guanajuato", "Jalisco", "Zacatecas"]
unidad_medida = "Celsius"

try:
    while True:
        # Obtener la hora actual del sistema en formato HH:MM:SS
        hora_actual = time.strftime("%H:%M:%S")
        
        # Generar una temperatura aleatoria entre 27 y 33
        temperatura = random.randint(27, 33)
        
        # Seleccionar una ciudad aleatoria
        ciudad = random.choice(ciudades)
        
        # Crear el objeto de datos
        data = {
            "ciudad": ciudad,
            "hora": hora_actual,
            "temperatura": temperatura,
            "unidad_medida": unidad_medida
        }
        
        # Convertir a JSON y codificar a bytes
        data_bytes = json.dumps(data).encode('utf-8')
        
        # Enviar el registro a Kinesis
        try:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=data_bytes,
                PartitionKey=ciudad
            )
            print(f"Registro enviado: {data}, ShardId={response['ShardId']}, SequenceNumber={response['SequenceNumber']}")
        except ClientError as e:
            print(f"Error al enviar el registro: {e}")
        
        # Esperar 1 segundo
        time.sleep(1)

except KeyboardInterrupt:
    print("\nScript detenido por el usuario.")