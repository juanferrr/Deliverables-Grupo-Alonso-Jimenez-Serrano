import os
import io
import csv
import json
import ast
import boto3
import pg8000.native

S3_BUCKET = os.getenv("S3_BUCKET", "juanpablojimenez")
RAW_KEY = os.getenv("RAW_KEY", "raw_zone/restaurants_raw.csv")

PGHOST = os.environ["PGHOST"]
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "postgres")
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]

UPSERT_SQL = """
INSERT INTO datos_externos (name, address, categories, latitude, longitude, distance)
VALUES (:name, :address, :categories::jsonb, :latitude, :longitude, :distance)
ON CONFLICT (name)
DO UPDATE SET
    address = EXCLUDED.address,
    categories = EXCLUDED.categories,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    distance = EXCLUDED.distance;
"""


def parse_categories(raw_value):
    """
    El CSV crudo en S3 trae esta columna como el str() plano de una
    lista de diccionarios de Python (comillas simples, no es JSON
    todavía). ast.literal_eval() la reconstruye como objeto Python,
    y json.dumps() la serializa como JSON válido para el cast ::jsonb.
    """
    if not raw_value:
        return json.dumps([])
    try:
        parsed = ast.literal_eval(raw_value)
    except (ValueError, SyntaxError):
        # Por si en algún momento la fuente ya entrega JSON válido
        parsed = json.loads(raw_value)
    return json.dumps(parsed)


def to_float(value):
    return float(value) if value not in (None, "") else None


def to_int(value):
    return int(float(value)) if value not in (None, "") else None


def lambda_handler(event, context):
    processed = 0
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=RAW_KEY)
        body_text = obj["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(body_text))

        conn = pg8000.native.Connection(
            user=PGUSER,
            password=PGPASSWORD,
            host=PGHOST,
            port=PGPORT,
            database=PGDATABASE,
        )

        try:
            for row in reader:
                name = (row.get("name") or "").strip()
                if not name:
                    continue  # sin nombre no hay llave para el UPSERT

                conn.run(
                    UPSERT_SQL,
                    name=name,
                    address=row.get("address") or None,
                    categories=parse_categories(row.get("categories")),
                    latitude=to_float(row.get("latitude")),
                    longitude=to_float(row.get("longitude")),
                    distance=to_int(row.get("distance")),
                )
                processed += 1
        finally:
            conn.close()

        print(f"OK: {processed} filas procesadas desde s3://{S3_BUCKET}/{RAW_KEY}")
        return {"statusCode": 200, "processed": processed}

    except Exception as e:
        print(f"ERROR en la carga: {type(e).__name__}: {e}")
        return {"statusCode": 500, "error": str(e)}