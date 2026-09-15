"""
Lambda de ingesta rotativa con arquitectura medallion completa.

    Foursquare Places API
            |
            v
    raw_zone/        JSON crudo, inmutable, una particion por corrida
            |
            v
    optimized_zone/  Parquet deduplicado y tipado (columnar, comprimido)
            |
            v
    consumption/     Tabla analitica en Parquet, lista para cargar a RDS


POR QUE TRES CAPAS
------------------
raw_zone conserva la respuesta original sin tocar, de modo que cualquier
error de transformacion es reprocesable sin volver a llamar a la API.
optimized_zone aplica deduplicacion por fsq_place_id y tipado, y guarda en
Parquet: formato columnar y comprimido, que pesa una fraccion del CSV y se
consulta mucho mas rapido. consumption_zone contiene el cruce espacial ya
resuelto, listo para cargar a RDS sin calculos adicionales.


ESTRATEGIA DE INGESTA
---------------------
El endpoint /places/search tiene tope de 50 resultados y no ofrece paginacion
(no hay offset, cursor, ni forma de excluir ids ya vistos). Para superarlo se
tesela el municipio de Chia con rectangulos (parametros ne/sw) y se consulta
una celda distinta en cada corrida, guardando el cursor en S3. Asi el centro
geografico cambia en cada ejecucion y el acumulado crece.


DEPENDENCIAS
------------
Requiere pandas y pyarrow, que NO vienen en el runtime de Lambda. Se obtienen
agregando la capa gestionada de AWS (Layers -> Add a layer -> Specify an ARN):

  arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:31

Si la capa no esta presente, la funcion degrada con elegancia: escribe
raw_zone y avisa en el log que omitio las capas superiores.


VARIABLES DE ENTORNO
--------------------
  FOURSQUARE_API_KEY     (obligatoria)
  S3_BUCKET              (obligatoria)
  PERFIL                 restaurantes | gasolineras   (default: restaurantes)
  CELDAS_POR_EJECUCION   celdas a procesar por corrida (default: 12)
  RADIO_ANALISIS_M       buffer del cruce espacial en metros (default: 500)

CONFIGURACION SUGERIDA
----------------------
  Runtime  : Python 3.12
  Rol      : LabRole
  Timeout  : 2 min
  Memoria  : 512 MB   (pandas + pyarrow no caben comodos en 128 MB)
"""

import os
import io
import json
import math
import time
import datetime
import urllib.parse
import urllib.request

import boto3

# pandas/pyarrow llegan por la capa gestionada. Si falta, se degrada.
try:
    import pandas as pd
    import numpy as np
    PANDAS_OK = True
except ImportError:
    PANDAS_OK = False


# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------

FSQ_ENDPOINT = "https://places-api.foursquare.com/places/search"
FSQ_API_VERSION = "2025-06-17"
LIMITE_POR_LLAMADA = 50            # tope duro de la API

# Bounding box del municipio de Chia, Cundinamarca
BBOX_SUR, BBOX_OESTE = 4.825, -74.095
BBOX_NORTE, BBOX_ESTE = 4.895, -74.015
LADO_CELDA_M = 1000

# Centro de referencia comun a todas las ingestas del curso
LAT_REF, LON_REF = 4.85876, -74.05866

PAUSA_ENTRE_LLAMADAS = 0.3

PERFILES = {
    "restaurantes": {"query": "restaurants"},
    "gasolineras": {"fsq_category_ids": "4bf58dd8d48988d113951735"},
}

COLUMNAS = ["fsq_place_id", "name", "address", "categories",
            "latitude", "longitude", "distance"]

# Prefijos de las zonas del data lake. Se dejan como variables para poder
# renombrarlos sin tocar el resto del codigo.
# Nota: el Deliverable 1 especificaba literalmente "consumption_zone".
ZONA_RAW = os.getenv("ZONA_RAW", "raw_zone")
ZONA_OPTIMIZED = os.getenv("ZONA_OPTIMIZED", "optimized_zone")
ZONA_CONSUMO = os.getenv("ZONA_CONSUMO", "consumption")

s3 = boto3.client("s3")


# ---------------------------------------------------------------------------
# Rejilla rectangular
# ---------------------------------------------------------------------------
def construir_rejilla():
    """Embaldosa el bbox con rectangulos contiguos.

    Se usan rectangulos (ne/sw) en vez de circulos (ll/radius) porque
    embaldosan el plano sin dejar huecos entre celdas vecinas.
    """
    m_lat = 110574.0
    m_lon = 111320.0 * math.cos(math.radians(LAT_REF))
    paso_lat = LADO_CELDA_M / m_lat
    paso_lon = LADO_CELDA_M / m_lon

    filas = max(1, int(math.ceil((BBOX_NORTE - BBOX_SUR) / paso_lat)))
    cols = max(1, int(math.ceil((BBOX_ESTE - BBOX_OESTE) / paso_lon)))

    celdas = []
    for i in range(filas):
        sur = BBOX_SUR + i * paso_lat
        norte = min(sur + paso_lat, BBOX_NORTE)
        for j in range(cols):
            oeste = BBOX_OESTE + j * paso_lon
            este = min(oeste + paso_lon, BBOX_ESTE)
            celdas.append((round(sur, 6), round(oeste, 6),
                           round(norte, 6), round(este, 6)))
    return celdas


def haversine(lat1, lon1, lat2, lon2):
    """Distancia en metros entre dos puntos. Escalar."""
    R = 6371000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# CAPA 1 - RAW ZONE
# ---------------------------------------------------------------------------
def consultar_celda(celda, params_perfil, api_key):
    sur, oeste, norte, este = celda
    params = {
        "sw": f"{sur},{oeste}",
        "ne": f"{norte},{este}",
        "limit": LIMITE_POR_LLAMADA,
        **params_perfil,
    }
    req = urllib.request.Request(
        f"{FSQ_ENDPOINT}?{urllib.parse.urlencode(params)}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
            "X-Places-Api-Version": FSQ_API_VERSION,
        },
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8")).get("results", [])


def escribir_raw(bucket, perfil, crudos, stamp):
    """Guarda la respuesta original sin transformar. Particionado por fecha
    para que Athena o Glue puedan podar particiones si se usan despues."""
    fecha = stamp[:8]
    key = f"{ZONA_RAW}/{perfil}/fecha={fecha[:4]}-{fecha[4:6]}-{fecha[6:8]}/{stamp}.json"
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(crudos, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return key


# ---------------------------------------------------------------------------
# CAPA 2 - OPTIMIZED ZONE
# ---------------------------------------------------------------------------
def aplanar(place):
    lat, lon = place.get("latitude"), place.get("longitude")
    if lat is None or lon is None:
        return None
    pid = place.get("fsq_place_id") or place.get("fsq_id")
    if not pid:
        return None
    loc = place.get("location") or {}
    return {
        "fsq_place_id": pid,
        "name": (place.get("name") or "").strip(),
        "address": loc.get("address") or loc.get("formatted_address") or "",
        "categories": json.dumps(place.get("categories") or [], ensure_ascii=False),
        "latitude": float(lat),
        "longitude": float(lon),
        "distance": int(round(haversine(LAT_REF, LON_REF, float(lat), float(lon)))),
    }


def leer_parquet(bucket, key):
    """Lee un Parquet de S3 a DataFrame; devuelve None si no existe."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except Exception:
        return None


def escribir_parquet(bucket, key, df):
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue(),
                  ContentType="application/octet-stream")
    return len(buf.getvalue())


def escribir_optimized(bucket, perfil, filas_nuevas):
    """Fusiona lo recien ingerido con el historico y reescribe el Parquet.

    La deduplicacion va por fsq_place_id, el identificador unico del
    establecimiento. Usar 'name' como clave colapsaria establecimientos
    distintos de la misma marca (dos Terpel en ubicaciones diferentes).
    """
    key = f"{ZONA_OPTIMIZED}/{perfil}/{perfil}.parquet"

    df_nuevo = pd.DataFrame(filas_nuevas, columns=COLUMNAS)
    df_previo = leer_parquet(bucket, key)

    if df_previo is not None and not df_previo.empty:
        antes = len(df_previo)
        df = pd.concat([df_previo, df_nuevo], ignore_index=True)
    else:
        antes = 0
        df = df_nuevo

    # El ultimo registro gana: refresca datos de corridas anteriores
    df = df.drop_duplicates(subset=["fsq_place_id"], keep="last")

    df = df.astype({
        "fsq_place_id": "string",
        "name": "string",
        "address": "string",
        "categories": "string",
        "latitude": "float64",
        "longitude": "float64",
        "distance": "int32",
    })

    bytes_escritos = escribir_parquet(bucket, key, df)
    return {"key": key, "filas": len(df), "nuevas": len(df) - antes,
            "bytes": bytes_escritos}


# ---------------------------------------------------------------------------
# CAPA 3 - CONSUMPTION ZONE
# ---------------------------------------------------------------------------
def escribir_consumption(bucket, radio_m):
    """Cruce espacial gasolineras x restaurantes: el indicador de negocio.

    Solo se calcula cuando ambos datasets existen en optimized_zone.
    El resultado se publica unicamente en Parquet: formato columnar y
    comprimido. El cargador a RDS lo lee con pandas.read_parquet().
    """
    gas = leer_parquet(bucket, f"{ZONA_OPTIMIZED}/gasolineras/gasolineras.parquet")
    res = leer_parquet(bucket, f"{ZONA_OPTIMIZED}/restaurantes/restaurantes.parquet")

    if gas is None or res is None or gas.empty or res.empty:
        return {"omitido": "faltan datasets en optimized_zone"}

    # Haversine vectorizado: matriz (gasolineras x restaurantes)
    R = 6371000
    g_lat = np.radians(gas["latitude"].to_numpy())[:, None]
    g_lon = np.radians(gas["longitude"].to_numpy())[:, None]
    r_lat = np.radians(res["latitude"].to_numpy())[None, :]
    r_lon = np.radians(res["longitude"].to_numpy())[None, :]

    a = (np.sin((r_lat - g_lat) / 2) ** 2 +
         np.cos(g_lat) * np.cos(r_lat) * np.sin((r_lon - g_lon) / 2) ** 2)
    dist = 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))

    competidores = (dist <= radio_m).sum(axis=1)
    mas_cercano = dist.min(axis=1)

    out = pd.DataFrame({
        "fsq_place_id": gas["fsq_place_id"].to_numpy(),
        "gasolinera": gas["name"].to_numpy(),
        "address": gas["address"].to_numpy(),
        "latitude": gas["latitude"].to_numpy(),
        "longitude": gas["longitude"].to_numpy(),
        "competidores_500m": competidores.astype("int32"),
        "dist_competidor_mas_cercano_m": np.round(mas_cercano).astype("int32"),
        "dist_al_centro_m": gas["distance"].to_numpy(),
    })

    # Cobertura valida: una gasolinera mas alla del alcance del muestreo de
    # restaurantes tendria cero competidores por falta de datos, no por
    # ausencia real de oferta. Se marca para no leer mal el ranking.
    alcance_restaurantes = int(res["distance"].max())
    out["cobertura_valida"] = out["dist_al_centro_m"] <= alcance_restaurantes

    out["clasificacion"] = np.where(
        ~out["cobertura_valida"], "Fuera de cobertura",
        np.where(out["competidores_500m"] == 0, "Alta oportunidad",
                 np.where(out["competidores_500m"] <= 2, "Oportunidad media",
                          "Zona saturada")))

    out = out.sort_values(
        ["cobertura_valida", "competidores_500m", "dist_competidor_mas_cercano_m"],
        ascending=[False, True, False]).reset_index(drop=True)

    # Solo Parquet. Publicar ademas un CSV duplicaria el almacenamiento y
    # contradiria el argumento FinOps que justifica el formato columnar.
    key = f"{ZONA_CONSUMO}/analisis_oportunidad.parquet"
    bytes_parquet = escribir_parquet(bucket, key, out)

    return {
        "key": f"s3://{bucket}/{key}",
        "bytes": bytes_parquet,
        "gasolineras": int(len(gas)),
        "restaurantes": int(len(res)),
        "alcance_muestreo_restaurantes_m": alcance_restaurantes,
        "en_cobertura_valida": int(out["cobertura_valida"].sum()),
        "fuera_de_cobertura": int((~out["cobertura_valida"]).sum()),
        "alta_oportunidad": int((out["clasificacion"] == "Alta oportunidad").sum()),
    }


# ---------------------------------------------------------------------------
# Estado del cursor
# ---------------------------------------------------------------------------
def leer_estado(bucket, perfil, total):
    try:
        obj = s3.get_object(Bucket=bucket, Key=f"state/{perfil}_cursor.json")
        est = json.loads(obj["Body"].read().decode("utf-8"))
        if est.get("total_celdas") != total:
            raise ValueError("la rejilla cambio")
        return est
    except Exception:
        return {"proxima_celda": 0, "ciclo": 1, "total_celdas": total}


def guardar_estado(bucket, perfil, estado):
    s3.put_object(
        Bucket=bucket, Key=f"state/{perfil}_cursor.json",
        Body=json.dumps(estado, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    api_key = (os.getenv("FSQ_API_KEY") or
               os.getenv("FOURSQUARE_API_KEY") or "").strip()
    bucket = os.getenv("S3_BUCKET", "").strip()
    if not api_key:
        return {"statusCode": 500, "error": "Falta FOURSQUARE_API_KEY"}
    if not bucket:
        return {"statusCode": 500, "error": "Falta S3_BUCKET"}

    perfil = (event or {}).get("perfil") or os.getenv("PERFIL", "restaurantes")
    if perfil not in PERFILES:
        return {"statusCode": 400, "error": f"perfil invalido: {perfil}"}

    lote = int(os.getenv("CELDAS_POR_EJECUCION", "12"))
    radio = int(os.getenv("RADIO_ANALISIS_M", "500"))

    celdas = construir_rejilla()
    estado = leer_estado(bucket, perfil, len(celdas))
    inicio = estado["proxima_celda"]

    print(f"perfil={perfil} rejilla={len(celdas)} celdas ciclo={estado['ciclo']} "
          f"inicio=celda {inicio} lote={lote}")

    # ---------------- Ingesta ----------------
    crudos, filas, topadas, procesadas = [], [], 0, 0

    for k in range(lote):
        idx = (inicio + k) % len(celdas)
        celda = celdas[idx]
        try:
            resultados = consultar_celda(celda, PERFILES[perfil], api_key)
        except Exception as e:
            print(f"  celda {idx}: {type(e).__name__}: {e} (omitida)")
            procesadas += 1
            continue

        if len(resultados) >= LIMITE_POR_LLAMADA:
            topadas += 1
            print(f"  celda {idx}: TOPE de 50 -> zona truncada, reduzca LADO_CELDA_M")

        crudos.extend(resultados)
        for p in resultados:
            f = aplanar(p)
            if f:
                filas.append(f)

        print(f"  celda {idx} {celda}: {len(resultados)} resultados")
        procesadas += 1
        time.sleep(PAUSA_ENTRE_LLAMADAS)

    stamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    # ---------------- Capa 1: raw ----------------
    raw_key = escribir_raw(bucket, perfil, crudos, stamp)

    # ---------------- Capas 2 y 3 ----------------
    optimized = consumption = None
    if PANDAS_OK:
        if filas:
            optimized = escribir_optimized(bucket, perfil, filas)
        consumption = escribir_consumption(bucket, radio)
    else:
        print("AVISO: pandas/pyarrow no disponibles. Solo se escribio raw_zone. "
              "Agregue la capa AWSSDKPandas-Python312 a la funcion.")

    # ---------------- Avance del cursor ----------------
    siguiente = (inicio + procesadas) % len(celdas)
    ciclo = estado["ciclo"] + (1 if siguiente <= inicio else 0)
    guardar_estado(bucket, perfil, {
        "proxima_celda": siguiente,
        "ciclo": ciclo,
        "total_celdas": len(celdas),
        "ultima_corrida": stamp,
    })

    resumen = {
        "statusCode": 200,
        "perfil": perfil,
        "celdas_procesadas": procesadas,
        "celdas_en_el_tope": topadas,
        "registros_crudos": len(crudos),
        "raw_zone": f"s3://{bucket}/{raw_key}",
        "optimized_zone": optimized,
        "consumption_zone": consumption,
        "proxima_celda": siguiente,
        "ciclo": ciclo,
        "pandas_disponible": PANDAS_OK,
    }
    print(json.dumps(resumen, ensure_ascii=False, indent=2, default=str))
    return resumen
