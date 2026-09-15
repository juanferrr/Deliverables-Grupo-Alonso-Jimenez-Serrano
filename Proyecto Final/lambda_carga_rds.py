"""
Lambda de carga: consumption (Parquet en S3) -> Amazon RDS PostgreSQL

Reemplaza al cargador anterior, que leia CSV. Ahora lee Parquet directamente,
de modo que la zona de consumo publica un unico archivo columnar y no se
duplica el almacenamiento con una copia en CSV.

    s3://<bucket>/consumption_zone/analisis_oportunidad.parquet
                    |
                    v
            tabla analisis_oportunidad en RDS
                    |
                    v
                 Grafana

La tabla se crea si no existe y la carga es idempotente: el UPSERT va por
fsq_place_id, el identificador unico de Foursquare. Usar 'name' como clave
colapsaria establecimientos distintos de la misma marca (dos Terpel en
ubicaciones diferentes), error que se detecto en la ingesta anterior.


DEPENDENCIAS
------------
  pandas + pyarrow -> capa gestionada de AWS:
      arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:31
  pg8000           -> va en el zip de la funcion (no existe capa oficial)

Capa y zip conviven sin problema: Lambda monta primero la capa y luego
descomprime el paquete de la funcion encima.

Para empaquetar pg8000 (PowerShell, desde esta carpeta):
    mkdir build
    pip install pg8000 -t .\build
    copy lambda_carga_rds.py .\build\
    Compress-Archive -Path .\build\* -DestinationPath .\carga_rds.zip


VARIABLES DE ENTORNO
--------------------
  S3_BUCKET     (obligatoria)
  PGHOST        (obligatoria)
  PGUSER        (obligatoria)
  PGPASSWORD    (obligatoria)
  PGPORT        default 5432
  PGDATABASE    default postgres
  ZONA_CONSUMO  default "consumption_zone"
  TABLA         default "analisis_oportunidad"

CONFIGURACION
-------------
  Runtime : Python 3.12      Timeout : 2 min
  Memoria : 512 MB           Rol     : LabRole
  VPC     : la misma de RDS, con el SG que permita el 5432
"""

import os
import io

import boto3
import pandas as pd
import pg8000.native

s3 = boto3.client("s3")

ZONA_CONSUMO = os.getenv("ZONA_CONSUMO", "consumption_zone")
ARCHIVO = os.getenv("ARCHIVO_CONSUMO", "analisis_oportunidad.parquet")
TABLA = os.getenv("TABLA", "analisis_oportunidad")

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLA} (
    fsq_place_id                  TEXT PRIMARY KEY,
    gasolinera                    VARCHAR(200) NOT NULL,
    address                       VARCHAR(300),
    latitude                      DOUBLE PRECISION,
    longitude                     DOUBLE PRECISION,
    competidores_500m             INTEGER,
    dist_competidor_mas_cercano_m INTEGER,
    dist_al_centro_m              INTEGER,
    cobertura_valida              BOOLEAN,
    clasificacion                 VARCHAR(40),
    actualizado_en                TIMESTAMPTZ DEFAULT NOW()
);
"""

UPSERT = f"""
INSERT INTO {TABLA} (
    fsq_place_id, gasolinera, address, latitude, longitude,
    competidores_500m, dist_competidor_mas_cercano_m, dist_al_centro_m,
    cobertura_valida, clasificacion
) VALUES (
    :fsq_place_id, :gasolinera, :address, :latitude, :longitude,
    :competidores_500m, :dist_competidor_mas_cercano_m, :dist_al_centro_m,
    :cobertura_valida, :clasificacion
)
ON CONFLICT (fsq_place_id) DO UPDATE SET
    gasolinera                    = EXCLUDED.gasolinera,
    address                       = EXCLUDED.address,
    latitude                      = EXCLUDED.latitude,
    longitude                     = EXCLUDED.longitude,
    competidores_500m             = EXCLUDED.competidores_500m,
    dist_competidor_mas_cercano_m = EXCLUDED.dist_competidor_mas_cercano_m,
    dist_al_centro_m              = EXCLUDED.dist_al_centro_m,
    cobertura_valida              = EXCLUDED.cobertura_valida,
    clasificacion                 = EXCLUDED.clasificacion,
    actualizado_en                = NOW();
"""


def leer_consumo(bucket):
    """Descarga el Parquet de la zona de consumo a un DataFrame."""
    key = f"{ZONA_CONSUMO}/{ARCHIVO}"
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    return df, f"s3://{bucket}/{key}"


def valor(fila, campo, defecto=None):
    """Convierte NaN de pandas a None, que es lo que entiende PostgreSQL."""
    v = fila.get(campo, defecto)
    return None if pd.isna(v) else v


def lambda_handler(event, context):
    bucket = os.getenv("S3_BUCKET", "").strip()
    host = os.getenv("PGHOST", "").strip()
    usuario = os.getenv("PGUSER", "").strip()
    clave = os.getenv("PGPASSWORD", "")

    faltan = [n for n, v in [("S3_BUCKET", bucket), ("PGHOST", host),
                             ("PGUSER", usuario), ("PGPASSWORD", clave)] if not v]
    if faltan:
        return {"statusCode": 500,
                "error": f"Faltan variables de entorno: {', '.join(faltan)}"}

    # ---------- Lectura del Parquet ----------
    try:
        df, origen = leer_consumo(bucket)
    except Exception as e:
        return {"statusCode": 404,
                "error": f"No se pudo leer la zona de consumo: "
                         f"{type(e).__name__}: {e}"}

    if df.empty:
        return {"statusCode": 204, "mensaje": "El Parquet no tiene filas"}

    print(f"Leidas {len(df)} filas desde {origen}")

    # ---------- Carga a RDS ----------
    try:
        conn = pg8000.native.Connection(
            user=usuario,
            password=clave,
            host=host,
            port=int(os.getenv("PGPORT", "5432")),
            database=os.getenv("PGDATABASE", "postgres"),
            timeout=20,
        )
    except Exception as e:
        return {"statusCode": 503,
                "error": f"No se pudo conectar a RDS: {type(e).__name__}: {e}. "
                         f"Verifique el Security Group (puerto 5432) y que la "
                         f"Lambda este en la misma VPC."}

    cargadas = 0
    try:
        conn.run(DDL)
        for _, fila in df.iterrows():
            conn.run(
                UPSERT,
                fsq_place_id=str(fila["fsq_place_id"]),
                gasolinera=str(fila["gasolinera"])[:200],
                address=(str(valor(fila, "address"))[:300]
                         if valor(fila, "address") is not None else None),
                latitude=float(fila["latitude"]),
                longitude=float(fila["longitude"]),
                competidores_500m=int(fila["competidores_500m"]),
                dist_competidor_mas_cercano_m=int(fila["dist_competidor_mas_cercano_m"]),
                dist_al_centro_m=int(fila["dist_al_centro_m"]),
                cobertura_valida=bool(fila["cobertura_valida"]),
                clasificacion=str(fila["clasificacion"])[:40],
            )
            cargadas += 1

        total = conn.run(f"SELECT COUNT(*) FROM {TABLA}")[0][0]
        resumen_clases = conn.run(
            f"SELECT clasificacion, COUNT(*) FROM {TABLA} "
            f"GROUP BY clasificacion ORDER BY 2 DESC"
        )
    finally:
        conn.close()

    resultado = {
        "statusCode": 200,
        "origen": origen,
        "filas_procesadas": cargadas,
        "total_en_tabla": total,
        "tabla": TABLA,
        "por_clasificacion": {c: n for c, n in resumen_clases},
    }
    print(resultado)
    return resultado
