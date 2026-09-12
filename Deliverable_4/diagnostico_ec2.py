"""Diagnostico rapido: estado de la instancia y reglas del Security Group.
Uso:  python diagnostico_ec2.py
Si falta el puerto 22 o 3000, los abre automaticamente."""
import os
import boto3
from dotenv import load_dotenv

load_dotenv()
session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
)
ec2 = session.client("ec2")

SG_ID = "sg-00d5ce044356a2ded"

print("=== INSTANCIAS ACTIVAS ===")
for res in ec2.describe_instances()["Reservations"]:
    for i in res["Instances"]:
        if i["State"]["Name"] in ("terminated", "shutting-down"):
            continue
        print(f"{i['InstanceId']}  estado={i['State']['Name']}  "
              f"IP_publica={i.get('PublicIpAddress')}  keypair={i.get('KeyName')}  "
              f"SG={[g['GroupId'] for g in i['SecurityGroups']]}")

print("\n=== REGLAS DE ENTRADA DEL SECURITY GROUP ===")
sg = ec2.describe_security_groups(GroupIds=[SG_ID])["SecurityGroups"][0]
abiertos = set()
for p in sg["IpPermissions"]:
    cidrs = [c["CidrIp"] for c in p.get("IpRanges", [])]
    print(f"  {p.get('IpProtocol')}  {p.get('FromPort')}-{p.get('ToPort')}  {cidrs}")
    if p.get("FromPort") is not None:
        for port in range(p["FromPort"], p["ToPort"] + 1):
            abiertos.add(port)

faltantes = [p for p in (22, 3000) if p not in abiertos]
if faltantes:
    print(f"\nFaltan los puertos {faltantes}. Abriendolos...")
    ec2.authorize_security_group_ingress(
        GroupId=SG_ID,
        IpPermissions=[
            {"IpProtocol": "tcp", "FromPort": p, "ToPort": p,
             "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": f"puerto {p}"}]}
            for p in faltantes
        ],
    )
    print("Listo. Reintenta la conexion SSH.")
else:
    print("\nLos puertos 22 y 3000 ya estan abiertos.")
