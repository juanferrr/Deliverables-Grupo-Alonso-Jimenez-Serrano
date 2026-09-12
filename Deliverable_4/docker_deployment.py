import os

import boto3
from dotenv import load_dotenv

# Carga las credenciales de la sesión de AWS Academy desde el archivo .env
load_dotenv()

session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
)

ec2 = session.resource("ec2")

# Script bash para configurar Amazon Linux 2023 (string multilínea de Python)
user_data_script = """#!/bin/bash
# 1. Actualizar el sistema e instalar Docker
dnf update -y
dnf install docker -y

# 2. Iniciar el servicio de Docker y habilitarlo en el arranque
systemctl start docker
systemctl enable docker
usermod -aG docker ec2-user

# 3. Descargar y ejecutar el contenedor de Grafana
docker run -d \
    --name=grafana_server \
    --restart unless-stopped \
    -p 3000:3000 \
    grafana/grafana:latest
"""

print("Aprovisionando infraestructura EC2 y contenedor Docker...")

# >>> COMPLETA ESTOS DOS VALORES ANTES DE EJECUTAR <
KEYPAIR_NAME = "llave-cloud-docker"          # Nombre de tu KeyPair (sin .pem)
SG_IDS = ["sg-00d5ce044356a2ded"]  # ID del Security Group del Paso 1

# Resolve the current AMI and replace the literal
ssm = session.client('ssm')
AMI_ID = ssm.get_parameter(Name='/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64')['Parameter']['Value']

instances = ec2.create_instances(
    ImageId=AMI_ID,
    MinCount=1,
    MaxCount=1,
    InstanceType="t2.micro",
    KeyName=KEYPAIR_NAME,
    UserData=user_data_script,
    SecurityGroupIds=SG_IDS,
)

# create_instances devuelve una lista; se accede al primer elemento
print(f"✅ Instancia creada correctamente. ID: {instances[0].id}")

