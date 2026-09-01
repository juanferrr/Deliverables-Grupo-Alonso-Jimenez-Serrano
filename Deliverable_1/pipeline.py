"""
pipeline.py
===========
Orquestador del pipeline de datos - Arquitectura Medallion sobre AWS S3.
 
Workshop / Deliverable 1 - Data Pipeline, Quality, and Medallion Architecture
Herramientas de Computacion en la Nube - Maestria en Analitica Aplicada
Universidad de La Sabana
 
Flujo:
    Foursquare API
        -> raw_zone/restaurants_raw.csv              (CSV crudo, tal como llega)
        -> [validaciones de calidad]
        -> optimized_zone/restaurants_processed.parquet   (limpio + columnar)
        -> consumption_zone/category_kpis.parquet         (KPIs pre-agregados)
        -> verificacion FinOps (tamanos en bytes por zona)
 
Ejecucion:
    python pipeline.py
"""
 
from __future__ import annotations
 
import io
import os
import sys
 
import boto3
import duckdb
import pandas as pd
import requests
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from dotenv import load_dotenv
from prefect import flow, get_run_logger, task
 
load_dotenv()
 
# --------------------------------------------------------------------------- #
# Configuracion centralizada
# --------------------------------------------------------------------------- #
 
# --- Fuente de datos (Foursquare Places API) ---
FOURSQUARE_URL = "https://places-api.foursquare.com/places/search"
FOURSQUARE_VERSION = "2025-06-17"
SEARCH_QUERY = "restaurants"
SEARCH_LL = "4.85876,-74.05866"  # Chia, Cundinamarca
SEARCH_LIMIT = 50
 
# --- Data Lake (S3) ---
BUCKET = os.getenv("S3_BUCKET", "juanpablojimenez")
RAW_KEY = "raw_zone/restaurants_raw.csv"
OPTIMIZED_KEY = "optimized_zone/restaurants_processed.parquet"
CONSUMPTION_KEY = "consumption_zone/category_kpis.parquet"
 
# --- Rutas locales de trabajo ---
OUTPUT_DIR = "output"
LOCAL_RAW = f"{OUTPUT_DIR}/restaurants_raw.csv"
LOCAL_OPTIMIZED = f"{OUTPUT_DIR}/restaurants_processed.parquet"
LOCAL_KPIS = f"{OUTPUT_DIR}/category_kpis.parquet"
 
# --- Regla de negocio de calidad ---
# Si menos del 80% de los registros sobreviven las validaciones
# (es decir, mas del 20% son invalidos), se aborta la carga a S3.
MIN_QUALITY_RATIO = 0.80
 
 
class PipelineError(Exception):
    """
    Error controlado del pipeline.
 
    Se usa para diferenciar los fallos *esperados* (API caida, credenciales
    vencidas, archivo faltante, calidad insuficiente) de los bugs reales.
    El flow captura esta excepcion y detiene la ejecucion de forma ordenada
    (graceful degradation) en lugar de reventar con un traceback.
    """
 
 
# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
 
def get_s3_client():
    """Crea el cliente de S3 con las credenciales temporales del Learner Lab."""
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
 
    if not all([access_key, secret_key, session_token]):
        raise PipelineError(
            "Faltan credenciales de AWS en el archivo .env. "
            "Abre el Learner Lab -> 'AWS Details' y copia "
            "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY y AWS_SESSION_TOKEN."
        )
 
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
 
 
def translate_s3_error(error: ClientError, context: str) -> PipelineError:
    """Convierte un ClientError de boto3 en un mensaje accionable para el usuario."""
    code = error.response.get("Error", {}).get("Code", "")
 
    if code in ("ExpiredToken", "ExpiredTokenException", "InvalidToken", "RequestExpired"):
        return PipelineError(
            f"[{context}] Las credenciales temporales de AWS Academy expiraron. "
            "Inicia una nueva sesion del Learner Lab (Start Lab), abre 'AWS Details' "
            "y actualiza las tres variables AWS_* en tu archivo .env."
        )
    if code in ("NoSuchBucket",):
        return PipelineError(
            f"[{context}] El bucket '{BUCKET}' no existe o no es visible con estas "
            "credenciales. Verifica el nombre en la variable S3_BUCKET."
        )
    if code in ("NoSuchKey", "404"):
        return PipelineError(
            f"[{context}] El objeto solicitado no existe en s3://{BUCKET}. "
            "Ejecuta primero la etapa de ingesta y carga."
        )
    if code in ("AccessDenied", "403"):
        return PipelineError(
            f"[{context}] Acceso denegado por S3. Revisa los permisos del bucket "
            "y que la sesion del Learner Lab siga activa."
        )
    return PipelineError(f"[{context}] Error de S3 ({code}): {error}")
 
 
def _first_category_name(categories) -> str | None:
    """
    Extrae el nombre de la primera categoria del objeto anidado de Foursquare.
 
    La API devuelve algo como:
        [{'fsq_category_id': '4bf58...', 'name': 'Steakhouse', ...}, ...]
    Guardar esa estructura tal cual en un CSV la vuelve un string inutilizable,
    asi que se aplana aqui, en la ingesta, antes de persistir.
    """
    if isinstance(categories, list) and categories:
        first = categories[0]
        if isinstance(first, dict):
            return first.get("name")
    return None
 
 
# --------------------------------------------------------------------------- #
# ETAPA 1 - Ingesta
# --------------------------------------------------------------------------- #
 
@task(name="1. Ingesta - Foursquare API", retries=2, retry_delay_seconds=5)
def ingest_places() -> pd.DataFrame:
    """
    Consulta la Foursquare Places API y devuelve un DataFrame aplanado.
 
    Reintenta 2 veces ante fallos transitorios de red antes de rendirse.
    """
    logger = get_run_logger()
 
    api_key = os.getenv("FOURSQUARE_API_KEY")
    if not api_key:
        raise PipelineError("FOURSQUARE_API_KEY no encontrada en el archivo .env")
 
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key.strip()}",
        "X-Places-Api-Version": FOURSQUARE_VERSION,
    }
    params = {"query": SEARCH_QUERY, "ll": SEARCH_LL, "limit": SEARCH_LIMIT}
 
    logger.info(f"Consultando Foursquare: '{SEARCH_QUERY}' cerca de {SEARCH_LL}")
 
    try:
        response = requests.get(FOURSQUARE_URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.Timeout:
        raise PipelineError("La API de Foursquare no respondio dentro de 30 segundos.")
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "desconocido"
        hint = " (revisa que la FOURSQUARE_API_KEY sea valida)" if status in (401, 403) else ""
        raise PipelineError(f"La API de Foursquare respondio con HTTP {status}{hint}.")
    except requests.exceptions.RequestException as exc:
        raise PipelineError(f"Fallo de red al consultar Foursquare: {exc}")
    except ValueError:
        raise PipelineError("La API de Foursquare devolvio una respuesta que no es JSON valido.")
 
    results = payload.get("results", [])
    if not results:
        raise PipelineError("La API respondio correctamente pero no devolvio ningun lugar.")
 
    df = pd.json_normalize(results)
 
    # Se construye un esquema explicito: la capa raw guarda solo lo que se
    # necesita, pero ya aplanado y con nombres estables.
    raw = pd.DataFrame()
    raw["name"] = df.get("name")
    raw["address"] = df.get("location.address")
 
    # Fallback: Foursquare no siempre trae 'address', pero casi siempre
    # trae 'formatted_address'. Esto rescata registros que quedarian nulos.
    if "location.formatted_address" in df.columns:
        raw["address"] = raw["address"].fillna(df["location.formatted_address"])
 
    raw["category_name"] = df.get("categories", pd.Series(dtype=object)).apply(_first_category_name)
    raw["latitude"] = df.get("latitude")
    raw["longitude"] = df.get("longitude")
    raw["distance"] = df.get("distance")
 
    logger.info(f"Ingesta exitosa: {len(raw)} lugares recuperados.")
    return raw
 
 
@task(name="2. Persistencia local del CSV crudo")
def save_raw_csv(df: pd.DataFrame) -> str:
    """Guarda el DataFrame crudo en disco antes de subirlo a la capa raw."""
    logger = get_run_logger()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
 
    try:
        df.to_csv(LOCAL_RAW, index=False, encoding="utf-8")
    except OSError as exc:
        raise PipelineError(f"No se pudo escribir el archivo local {LOCAL_RAW}: {exc}")
 
    size = os.path.getsize(LOCAL_RAW)
    logger.info(f"CSV crudo guardado en {LOCAL_RAW} ({size:,} bytes)")
    return LOCAL_RAW
 
 
# --------------------------------------------------------------------------- #
# Carga a S3 (reutilizable por las tres zonas)
# --------------------------------------------------------------------------- #
 
@task(name="Carga a S3")
def upload_to_s3(local_path: str, key: str) -> str:
    """Sube un archivo local a una key de S3. Funcion generica para las 3 zonas."""
    logger = get_run_logger()
 
    if not os.path.isfile(local_path):
        raise PipelineError(
            f"Archivo local no encontrado: {local_path}. "
            "La etapa anterior del pipeline no genero su salida."
        )
 
    client = get_s3_client()
 
    try:
        client.upload_file(local_path, BUCKET, key)
    except ClientError as exc:
        raise translate_s3_error(exc, context=f"subida de {key}")
    except NoCredentialsError:
        raise PipelineError("boto3 no encontro credenciales de AWS utilizables.")
    except BotoCoreError as exc:
        raise PipelineError(f"Error de conexion con AWS al subir {key}: {exc}")
 
    uri = f"s3://{BUCKET}/{key}"
    logger.info(f"Subido correctamente a {uri}")
    return uri
 
 
@task(name="3. Lectura desde la capa Raw (S3)")
def read_raw_from_s3() -> pd.DataFrame:
    """
    Lee el CSV desde la capa raw del Data Lake, en memoria.
 
    Leer desde S3 (y no desde el disco local) es lo que hace que la capa raw
    sea la verdadera fuente de verdad del pipeline: cualquier transformacion
    posterior parte de lo que efectivamente quedo almacenado en el lake.
    """
    logger = get_run_logger()
    client = get_s3_client()
 
    try:
        obj = client.get_object(Bucket=BUCKET, Key=RAW_KEY)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    except ClientError as exc:
        raise translate_s3_error(exc, context=f"lectura de {RAW_KEY}")
    except NoCredentialsError:
        raise PipelineError("boto3 no encontro credenciales de AWS utilizables.")
    except pd.errors.EmptyDataError:
        raise PipelineError(f"El objeto {RAW_KEY} esta vacio o corrupto.")
 
    logger.info(f"Capa raw leida desde S3: {len(df)} registros, {len(df.columns)} columnas.")
    return df
 
 
# --------------------------------------------------------------------------- #
# ETAPA 2 - Validaciones de calidad de datos
# --------------------------------------------------------------------------- #
 
@task(name="4. Validacion de calidad de datos")
def validate_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica las validaciones programaticas sobre el DataFrame.
 
    Validaciones:
      1. latitude / longitude no nulas y dentro de rango geografico valido.
      2. distance numerica y mayor o igual a cero.
      3. name no nulo ni vacio.
      4. Sin duplicados por (name, latitude, longitude).
 
    Regla de negocio: si sobrevive menos del MIN_QUALITY_RATIO de los
    registros, se aborta la carga a S3 en lugar de contaminar el Data Lake.
    """
    logger = get_run_logger()
 
    total = len(df)
    if total == 0:
        raise PipelineError("El dataset llego vacio a la etapa de validacion.")
 
    required = {"name", "latitude", "longitude", "distance"}
    missing = required - set(df.columns)
    if missing:
        raise PipelineError(
            f"El dataset no tiene las columnas requeridas: {sorted(missing)}. "
            "Es posible que la capa raw en S3 tenga un esquema antiguo; "
            "vuelve a ejecutar el pipeline completo para regenerarla."
        )
 
    # --- Validacion 1: coordenadas presentes y en rango ---
    latitude = pd.to_numeric(df["latitude"], errors="coerce")
    longitude = pd.to_numeric(df["longitude"], errors="coerce")
    coords_ok = (
        latitude.notna()
        & longitude.notna()
        & latitude.between(-90, 90)
        & longitude.between(-180, 180)
    )
 
    # --- Validacion 2: distance numerica y no negativa ---
    distance = pd.to_numeric(df["distance"], errors="coerce")
    distance_ok = distance.notna() & (distance >= 0)
 
    # --- Validacion 3: nombre util ---
    name_ok = df["name"].notna() & df["name"].astype(str).str.strip().ne("")
 
    failures = {
        "coordenadas nulas o fuera de rango": int((~coords_ok).sum()),
        "distance no numerica o negativa": int((~distance_ok).sum()),
        "nombre nulo o vacio": int((~name_ok).sum()),
    }
 
    valid_mask = coords_ok & distance_ok & name_ok
    clean = df.loc[valid_mask].copy()
    clean["latitude"] = latitude.loc[valid_mask]
    clean["longitude"] = longitude.loc[valid_mask]
    clean["distance"] = distance.loc[valid_mask]
 
    # --- Validacion 4: duplicados ---
    before_dedup = len(clean)
    clean = clean.drop_duplicates(subset=["name", "latitude", "longitude"], keep="first")
    failures["duplicados exactos"] = before_dedup - len(clean)
 
    # Normalizacion final de tipos y valores nulos de negocio.
    clean["distance"] = clean["distance"].astype(int)
    if "category_name" in clean.columns:
        clean["category_name"] = clean["category_name"].fillna("Sin categoria")
    else:
        clean["category_name"] = "Sin categoria"
 
    surviving = len(clean)
    ratio = surviving / total
 
    logger.info("--- Reporte de calidad de datos ---")
    logger.info(f"Registros de entrada : {total}")
    for check, count in failures.items():
        logger.info(f"  Descartados por {check}: {count}")
    logger.info(f"Registros validos    : {surviving} ({ratio:.1%})")
 
    if ratio < MIN_QUALITY_RATIO:
        raise PipelineError(
            f"Umbral de calidad no alcanzado: solo {ratio:.1%} de los registros son "
            f"validos (minimo requerido {MIN_QUALITY_RATIO:.0%}). "
            "Se aborta la carga a las capas optimized y consumption para no "
            "contaminar el Data Lake con datos poco confiables."
        )
 
    logger.info("Umbral de calidad superado. El pipeline continua.")
    return clean
 
 
# --------------------------------------------------------------------------- #
# ETAPA 3 - Capa optimizada (Parquet columnar)
# --------------------------------------------------------------------------- #
 
@task(name="5. Capa Optimized - Parquet")
def write_optimized_parquet(df: pd.DataFrame) -> str:
    """Escribe el DataFrame validado en formato columnar Parquet con compresion."""
    logger = get_run_logger()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
 
    try:
        df.to_parquet(LOCAL_OPTIMIZED, engine="pyarrow", compression="snappy", index=False)
    except ImportError:
        raise PipelineError("Falta la dependencia 'pyarrow'. Instalala con: pip install pyarrow")
    except OSError as exc:
        raise PipelineError(f"No se pudo escribir el Parquet optimizado: {exc}")
 
    size = os.path.getsize(LOCAL_OPTIMIZED)
    logger.info(f"Parquet optimizado generado: {LOCAL_OPTIMIZED} ({size:,} bytes)")
    return LOCAL_OPTIMIZED
 
 
# --------------------------------------------------------------------------- #
# ETAPA 4 - Capa de consumo (KPIs pre-agregados con DuckDB)
# --------------------------------------------------------------------------- #
 
@task(name="6. Capa Consumption - KPIs con DuckDB")
def build_consumption_layer() -> pd.DataFrame:
    """
    Genera la tabla analitica resumida con DuckDB y la exporta como Parquet.
 
    Pre-agregar los KPIs aqui evita que un dashboard de BI tenga que escanear
    el dataset completo en cada consulta: la herramienta lee unas pocas filas
    ya calculadas en lugar de recorrer todos los registros de detalle.
    """
    logger = get_run_logger()
 
    if not os.path.isfile(LOCAL_OPTIMIZED):
        raise PipelineError(
            f"No existe el Parquet optimizado ({LOCAL_OPTIMIZED}). "
            "La etapa de transformacion no se completo."
        )
 
    con = duckdb.connect()
    try:
        kpis = con.execute(
            f"""
            SELECT
                category_name                       AS category,
                COUNT(*)                            AS total_restaurants,
                ROUND(AVG(distance), 2)             AS avg_distance_m,
                MIN(distance)                       AS min_distance_m,
                MAX(distance)                       AS max_distance_m,
                ROUND(AVG(latitude), 6)             AS centroid_lat,
                ROUND(AVG(longitude), 6)            AS centroid_lon
            FROM read_parquet('{LOCAL_OPTIMIZED}')
            GROUP BY category_name
            ORDER BY total_restaurants DESC, avg_distance_m ASC
            """
        ).df()
    except duckdb.Error as exc:
        raise PipelineError(f"DuckDB fallo al construir la capa de consumo: {exc}")
    finally:
        con.close()
 
    if kpis.empty:
        raise PipelineError("La agregacion de DuckDB no produjo ninguna fila.")
 
    try:
        kpis.to_parquet(LOCAL_KPIS, engine="pyarrow", compression="snappy", index=False)
    except OSError as exc:
        raise PipelineError(f"No se pudo escribir el Parquet de KPIs: {exc}")
 
    logger.info(f"Capa de consumo generada: {len(kpis)} categorias agregadas.")
    print("\n--- KPIs por categoria (capa de consumo) ---")
    print(kpis.to_string(index=False))
 
    return kpis
 
 
# --------------------------------------------------------------------------- #
# ETAPA 5 - Verificacion programatica y FinOps
# --------------------------------------------------------------------------- #
 
@task(name="7. Verificacion FinOps - tamanos por zona")
def verify_zones() -> pd.DataFrame:
    """
    Recorre las tres zonas del Data Lake con list_objects_v2 e imprime el
    tamano de cada objeto, demostrando empiricamente el ahorro del formato
    columnar frente al CSV original.
    """
    logger = get_run_logger()
    client = get_s3_client()
 
    zones = {
        "raw_zone": "raw_zone/",
        "optimized_zone": "optimized_zone/",
        "consumption_zone": "consumption_zone/",
    }
 
    rows = []
    for zone, prefix in zones.items():
        try:
            response = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        except ClientError as exc:
            raise translate_s3_error(exc, context=f"listado de {prefix}")
 
        contents = response.get("Contents", [])
        if not contents:
            logger.warning(f"La zona '{zone}' no contiene objetos.")
            continue
 
        for obj in contents:
            rows.append(
                {
                    "zone": zone,
                    "key": obj["Key"],
                    "size_bytes": obj["Size"],
                    "size_kb": round(obj["Size"] / 1024, 2),
                }
            )
 
    if not rows:
        raise PipelineError("No se encontro ningun objeto en el Data Lake.")
 
    inventory = pd.DataFrame(rows)
 
    print("\n--- Inventario del Data Lake (verificacion con list_objects_v2) ---")
    print(inventory.to_string(index=False))
 
    # Comparacion directa raw (CSV) vs optimized (Parquet)
    raw_row = inventory.loc[inventory["key"] == RAW_KEY, "size_bytes"]
    opt_row = inventory.loc[inventory["key"] == OPTIMIZED_KEY, "size_bytes"]
 
    if not raw_row.empty and not opt_row.empty:
        raw_bytes = int(raw_row.iloc[0])
        opt_bytes = int(opt_row.iloc[0])
        saving = (1 - opt_bytes / raw_bytes) * 100 if raw_bytes else 0.0
 
        print("\n--- Analisis FinOps: CSV vs Parquet ---")
        print(f"  raw_zone       (CSV)     : {raw_bytes:>10,} bytes")
        print(f"  optimized_zone (Parquet) : {opt_bytes:>10,} bytes")
        if saving >= 0:
            print(f"  Reduccion de tamano      : {saving:>10.1f} %")
        else:
            # En datasets muy pequenos los metadatos de Parquet pueden pesar
            # mas que el ahorro de la compresion. El beneficio aparece al escalar.
            print(f"  Incremento de tamano     : {abs(saving):>10.1f} %")
            print(
                "  Nota: con volumenes pequenos el encabezado y los metadatos de\n"
                "  Parquet dominan el archivo. La ventaja del formato columnar se\n"
                "  materializa a partir de decenas de miles de filas, y sobre todo\n"
                "  en el volumen escaneado por consulta (column pruning)."
            )
 
    logger.info("Verificacion FinOps completada.")
    return inventory
 
 
@task(name="8. Benchmark de escalamiento columnar (local)")
def benchmark_columnar_scaling(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compara CSV vs Parquet replicando el dataset a distintos volumenes.
 
    Por que existe esta etapa: con 50 filas el Parquet suele pesar MAS que el
    CSV, porque su encabezado, el diccionario y el footer de metadatos son un
    costo fijo de unos pocos KB. Ese costo fijo se amortiza al crecer el
    volumen, y a partir de ahi la codificacion por diccionario y la compresion
    por columna se imponen. Este benchmark encuentra ese punto de cruce con
    los datos reales del proyecto, y da evidencia empirica para el informe.
 
    Se ejecuta en local (archivos temporales): no genera costo ni trafico en S3.
    """
    logger = get_run_logger()
    os.makedirs(OUTPUT_DIR, exist_ok=True)
 
    scales = [1, 10, 100, 1_000]
    rows = []
 
    for factor in scales:
        scaled = pd.concat([df] * factor, ignore_index=True)
        csv_path = f"{OUTPUT_DIR}/_bench.csv"
        pq_path = f"{OUTPUT_DIR}/_bench.parquet"
 
        try:
            scaled.to_csv(csv_path, index=False, encoding="utf-8")
            scaled.to_parquet(pq_path, engine="pyarrow", compression="snappy", index=False)
            csv_bytes = os.path.getsize(csv_path)
            pq_bytes = os.path.getsize(pq_path)
        except OSError as exc:
            raise PipelineError(f"Fallo el benchmark de formatos: {exc}")
        finally:
            for temp in (csv_path, pq_path):
                if os.path.exists(temp):
                    os.remove(temp)
 
        rows.append(
            {
                "filas": len(scaled),
                "csv_bytes": csv_bytes,
                "parquet_bytes": pq_bytes,
                "ratio_parquet_csv": round(pq_bytes / csv_bytes, 3),
                "ahorro_pct": round((1 - pq_bytes / csv_bytes) * 100, 1),
            }
        )
 
    bench = pd.DataFrame(rows)
 
    print("\n--- Benchmark de escalamiento: CSV vs Parquet (mismo dataset replicado) ---")
    print(bench.to_string(index=False))
 
    crossover = bench.loc[bench["ahorro_pct"] > 0]
    if not crossover.empty:
        first = crossover.iloc[0]
        print(
            f"\n  Punto de cruce: a partir de ~{int(first['filas']):,} filas el Parquet "
            f"ya es mas pequeno que el CSV ({first['ahorro_pct']}% de ahorro)."
        )
        print(
            f"  Al maximo volumen probado ({int(bench.iloc[-1]['filas']):,} filas) "
            f"el ahorro llega a {bench.iloc[-1]['ahorro_pct']}%."
        )
    else:
        print("\n  En los volumenes probados el CSV sigue siendo mas compacto.")
 
    logger.info("Benchmark de formatos completado.")
    return bench
 
 
# --------------------------------------------------------------------------- #
# ORQUESTADOR
# --------------------------------------------------------------------------- #
 
@flow(name="Medallion Data Pipeline - Restaurantes Chia", log_prints=True)
def medallion_pipeline() -> bool:
    """
    Orquesta el pipeline completo: Raw -> Optimized -> Consumption.
 
    Cualquier PipelineError detiene la ejecucion de forma ordenada, registrando
    un mensaje descriptivo en lugar de propagar un traceback (graceful degradation).
    """
    logger = get_run_logger()
    logger.info("=" * 70)
    logger.info(f"Iniciando pipeline sobre el bucket s3://{BUCKET}")
    logger.info("=" * 70)
 
    try:
        # --- Capa Raw ---
        df_raw = ingest_places()
        save_raw_csv(df_raw)
        upload_to_s3(LOCAL_RAW, RAW_KEY)
 
        # --- Capa Optimized ---
        df_from_lake = read_raw_from_s3()
        df_clean = validate_quality(df_from_lake)
        write_optimized_parquet(df_clean)
        upload_to_s3(LOCAL_OPTIMIZED, OPTIMIZED_KEY)
 
        # --- Capa Consumption ---
        build_consumption_layer()
        upload_to_s3(LOCAL_KPIS, CONSUMPTION_KEY)
 
        # --- Verificacion y evidencia FinOps ---
        verify_zones()
        benchmark_columnar_scaling(df_clean)
 
    except PipelineError as exc:
        logger.error("=" * 70)
        logger.error(f"PIPELINE DETENIDO: {exc}")
        logger.error("=" * 70)
        return False
 
    except Exception as exc:  # noqa: BLE001 - red de seguridad del orquestador
        logger.error("=" * 70)
        logger.error(f"ERROR INESPERADO ({type(exc).__name__}): {exc}")
        logger.error("=" * 70)
        return False
 
    logger.info("=" * 70)
    logger.info("Pipeline completado con exito. Las tres zonas estan actualizadas.")
    logger.info("=" * 70)
    return True
 
 
if __name__ == "__main__":
    ok = medallion_pipeline()
    sys.exit(0 if ok else 1)
 