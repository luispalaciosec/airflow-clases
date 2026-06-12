"""
─────────────────────────────────────────────────────────────────────
DAG S4 — ETL completo: API → transformación → Postgres
Sesión 4 · Módulo 2

Objetivo: pipeline end-to-end real usando PostgresHook,
manejo de errores y retries.

API usada: Open-Meteo (gratuita, sin API key)
https://open-meteo.com/
─────────────────────────────────────────────────────────────────────
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging
import json

log = logging.getLogger(__name__)

# ── Configuración ────────────────────────────────────────────────────────────
CIUDAD = "Guayaquil"
LATITUD = -2.1962
LONGITUD = -79.8862
DIAS_HISTORICO = 7


@dag(
    dag_id="s4_etl_clima_postgres",
    description="Sesión 4 — ETL de clima desde API pública a Postgres",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    retries=2,
    retry_delay=timedelta(minutes=2),
    tags=["sesion-4", "modulo-2", "etl", "postgres", "api"],
)
def etl_clima_postgres():
    """
    ### ETL: clima → Postgres
    Extrae datos meteorológicos de Open-Meteo, los transforma
    con pandas y los carga en la tabla `ventas` de pipeline_db.

    **Conexión requerida:** `postgres_lab` (ya configurada en airflow_settings.yaml)
    """

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract() -> str:
        """Extrae datos de temperatura de los últimos 7 días desde Open-Meteo."""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": LATITUD,
            "longitude": LONGITUD,
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
            "timezone": "America/Guayaquil",
            "past_days": DIAS_HISTORICO,
            "forecast_days": 1,
        }
        log.info("🌐 Extrayendo datos de %s (lat=%s, lon=%s)", CIUDAD, LATITUD, LONGITUD)
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        log.info("✅ Datos extraídos: %d días", len(data["daily"]["time"]))
        return json.dumps(data["daily"])

    @task
    def transform(raw_json: str) -> str:
        """Transforma el JSON crudo en un DataFrame limpio."""
        data = json.loads(raw_json)
        df = pd.DataFrame({
            "fecha": pd.to_datetime(data["time"]),
            "temp_max": data["temperature_2m_max"],
            "temp_min": data["temperature_2m_min"],
            "precipitacion": data["precipitation_sum"],
            "ciudad": CIUDAD,
        })
        df["temp_promedio"] = (df["temp_max"] + df["temp_min"]) / 2
        df = df.dropna()
        log.info("🔄 Transformados %d registros — columnas: %s", len(df), df.columns.tolist())
        return df.to_json(orient="records", date_format="iso")

    @task
    def load(records_json: str) -> dict:
        """Carga los datos transformados en Postgres usando PostgresHook."""
        records = json.loads(records_json)
        if not records:
            log.warning("⚠️  Sin registros para cargar")
            return {"cargados": 0}

        hook = PostgresHook(postgres_conn_id="postgres_lab")

        # Crear tabla si no existe
        hook.run("""
            CREATE TABLE IF NOT EXISTS clima_historico (
                id            SERIAL PRIMARY KEY,
                fecha         DATE NOT NULL,
                ciudad        VARCHAR(100),
                temp_max      NUMERIC(5,2),
                temp_min      NUMERIC(5,2),
                temp_promedio NUMERIC(5,2),
                precipitacion NUMERIC(8,2),
                cargado_en    TIMESTAMP DEFAULT NOW(),
                UNIQUE(fecha, ciudad)
            );
        """)

        # Insertar con upsert (no duplicar si ya existe)
        inserted = 0
        for r in records:
            try:
                hook.run("""
                    INSERT INTO clima_historico (fecha, ciudad, temp_max, temp_min, temp_promedio, precipitacion)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (fecha, ciudad) DO UPDATE SET
                        temp_max = EXCLUDED.temp_max,
                        temp_min = EXCLUDED.temp_min,
                        temp_promedio = EXCLUDED.temp_promedio,
                        precipitacion = EXCLUDED.precipitacion,
                        cargado_en = NOW();
                """, parameters=(
                    r["fecha"][:10], r["ciudad"],
                    r["temp_max"], r["temp_min"],
                    r["temp_promedio"], r["precipitacion"],
                ))
                inserted += 1
            except Exception as e:
                log.error("❌ Error insertando registro %s: %s", r, e)

        log.info("✅ Cargados %d / %d registros en postgres_lab.clima_historico", inserted, len(records))
        return {"cargados": inserted, "total": len(records)}

    @task
    def log_pipeline(resultado: dict):
        """Registra la ejecución en la tabla de auditoría."""
        hook = PostgresHook(postgres_conn_id="postgres_lab")
        hook.run("""
            INSERT INTO pipeline_log (dag_id, tarea, status, registros)
            VALUES ('s4_etl_clima_postgres', 'load', 'success', %s);
        """, parameters=(resultado.get("cargados", 0),))
        log.info("📋 Ejecución registrada en pipeline_log")

    # ── Pipeline ─────────────────────────────────────────────────────────────
    raw = extract()
    transformed = transform(raw)
    resultado = load(transformed)
    log_pipeline(resultado)


etl_clima_postgres()
