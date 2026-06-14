"""
─────────────────────────────────────────────────────────────────────
DAG S3 — Hooks y Conexiones: HttpHook + PostgresHook
Sesión 3 · Módulo 2

Objetivo: practicar el uso de HttpHook para consumir una API real
y PostgresHook para leer y escribir en la base de datos.

Este DAG es el laboratorio guiado de S3. Tiene 4 tasks que
cubren las 3 operaciones clave de PostgresHook:
  - .run()          → crear tabla / ejecutar SQL
  - .get_records()  → leer filas
  - .insert_rows()  → insertar en batch

API usada: Open-Meteo (gratuita, sin API key)
Conexiones requeridas: open_meteo_api, postgres_lab
─────────────────────────────────────────────────────────────────────
"""

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
import logging

log = logging.getLogger(__name__)


@dag(
    dag_id="s3_hooks_conexiones",
    description="Sesión 3 — HttpHook + PostgresHook en práctica",
    schedule=None,                  # manual: ejecutar desde la UI
    start_date=days_ago(1),
    catchup=False,
    tags=["sesion-3", "modulo-2", "hooks", "api", "postgres"],
)
def s3_hooks_conexiones():
    """
    ### S3 — Hooks y Conexiones
    Pipeline de 4 tasks que demuestra las herramientas del día:

    **Task 1 — preparar_tabla**: Crea la tabla `lecturas_clima` si no existe.
    Usa `PostgresHook.run()` para ejecutar DDL.

    **Task 2 — extraer_clima**: Llama la API de Open-Meteo con `HttpHook`.
    Retorna JSON con temperatura y lluvia de Guayaquil.

    **Task 3 — insertar_lecturas**: Inserta los registros en batch.
    Usa `PostgresHook.insert_rows()`.

    **Task 4 — verificar_datos**: Lee los datos insertados.
    Usa `PostgresHook.get_records()` y muestra estadísticas.

    **Conexiones necesarias** (ya configuradas en airflow_settings.yaml):
    - `postgres_lab` → PostgreSQL del curso
    - `open_meteo_api` → API de clima

    **Puerto pgAdmin**: 5050 — para ver los datos visualmente.
    """

    # ── TASK 1 — Crear tabla si no existe ────────────────────────────────
    @task
    def preparar_tabla():
        """
        Crea la tabla lecturas_clima usando PostgresHook.run().
        Esta es la forma más directa de ejecutar SQL sin retorno.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_lab")

        # .run() ejecuta cualquier SQL — DDL, DML, etc.
        hook.run("""
            CREATE TABLE IF NOT EXISTS lecturas_clima (
                id          SERIAL PRIMARY KEY,
                fecha       DATE NOT NULL,
                ciudad      VARCHAR(100) NOT NULL DEFAULT 'Guayaquil',
                temp_max    NUMERIC(5, 2),
                temp_min    NUMERIC(5, 2),
                lluvia_mm   NUMERIC(8, 2) DEFAULT 0.0,
                insertado_en TIMESTAMP DEFAULT NOW(),
                UNIQUE(fecha, ciudad)
            );
        """)

        log.info("✅ Tabla lecturas_clima lista")

        # Verificar que la tabla existe
        resultado = hook.get_records(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'lecturas_clima';"
        )
        log.info("📋 Tabla existe en BD: %s", resultado[0][0] == 1)
        return "tabla_lista"

    # ── TASK 2 — Extraer datos de la API con HttpHook ────────────────────
    @task
    def extraer_clima(tabla_status: str) -> str:
        """
        Llama a Open-Meteo usando HttpHook.
        Recibe el status de la task anterior via XCom (prueba de dependencia).
        """
        from airflow.providers.http.hooks.http import HttpHook

        log.info("📡 Estado de tabla recibido via XCom: %s", tabla_status)

        hook = HttpHook(
            method="GET",
            http_conn_id="open_meteo_api"
        )

        # Parámetros de la request — Guayaquil, Ecuador
        params = {
            "latitude": -2.1962,
            "longitude": -79.8862,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
            ],
            "timezone": "America/Guayaquil",
            "past_days": 5,         # últimos 5 días
            "forecast_days": 1,
        }

        # HttpHook.run() hace el request usando la conexión configurada
        response = hook.run(
            endpoint="/v1/forecast",
            data=params,
        )
        response.raise_for_status()

        data = response.json()
        dias = len(data["daily"]["time"])
        log.info("✅ Datos extraídos: %d días de Guayaquil", dias)
        log.info("📅 Rango: %s → %s",
                 data["daily"]["time"][0],
                 data["daily"]["time"][-1])

        # Serializar para pasar via XCom
        return json.dumps(data["daily"])

    # ── TASK 3 — Insertar en batch con insert_rows() ─────────────────────
    @task
    def insertar_lecturas(raw_json: str) -> dict:
        """
        Inserta registros en batch con PostgresHook.insert_rows().
        Más eficiente que hacer un INSERT por cada fila.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        data = json.loads(raw_json)

        # Construir las filas como lista de tuplas
        filas = []
        for i, fecha in enumerate(data["time"]):
            temp_max = data["temperature_2m_max"][i]
            temp_min = data["temperature_2m_min"][i]
            lluvia   = data["precipitation_sum"][i] or 0.0
            filas.append((fecha, "Guayaquil", temp_max, temp_min, lluvia))

        log.info("📦 Preparadas %d filas para insertar", len(filas))

        hook = PostgresHook(postgres_conn_id="postgres_lab")

        # Upsert fila a fila con ON CONFLICT — idempotente
        insertadas = 0
        for fila in filas:
            hook.run("""
                INSERT INTO lecturas_clima (fecha, ciudad, temp_max, temp_min, lluvia_mm)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fecha, ciudad) DO UPDATE SET
                    temp_max     = EXCLUDED.temp_max,
                    temp_min     = EXCLUDED.temp_min,
                    lluvia_mm    = EXCLUDED.lluvia_mm,
                    insertado_en = NOW();
            """, parameters=fila)
            insertadas += 1

        log.info("✅ %d registros insertados/actualizados", insertadas)
        return {"filas_insertadas": len(filas), "ciudad": "Guayaquil"}

    # ── TASK 4 — Verificar con get_records() ─────────────────────────────
    @task
    def verificar_datos(resultado: dict):
        """
        Lee los datos insertados con PostgresHook.get_records().
        Muestra estadísticas en los logs — visibles en Airflow UI.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_lab")

        # .get_records() retorna lista de tuplas [(col1, col2), ...]
        registros = hook.get_records("""
            SELECT
                fecha,
                temp_max,
                temp_min,
                ROUND((temp_max + temp_min) / 2, 2) AS temp_promedio,
                lluvia_mm
            FROM lecturas_clima
            WHERE ciudad = 'Guayaquil'
            ORDER BY fecha DESC
            LIMIT 10;
        """)

        log.info("=" * 55)
        log.info("📊 LECTURAS DE GUAYAQUIL — últimos registros")
        log.info("%-12s %8s %8s %8s %8s",
                 "Fecha", "Max°C", "Min°C", "Prom°C", "Lluvia")
        log.info("-" * 55)
        for r in registros:
            log.info("%-12s %8.1f %8.1f %8.1f %8.1f mm",
                     str(r[0]), r[1], r[2], r[3], r[4])
        log.info("=" * 55)

        # Estadísticas globales
        stats = hook.get_records("""
            SELECT
                COUNT(*)                           AS total_dias,
                ROUND(AVG(temp_max), 2)            AS temp_max_promedio,
                ROUND(AVG(temp_min), 2)            AS temp_min_promedio,
                MAX(temp_max)                      AS temp_mas_alta,
                MIN(temp_min)                      AS temp_mas_baja,
                SUM(lluvia_mm)                     AS lluvia_total
            FROM lecturas_clima
            WHERE ciudad = 'Guayaquil';
        """)

        s = stats[0]
        log.info("📈 Estadísticas globales:")
        log.info("   Total días registrados : %d", s[0])
        log.info("   Temp. máx. promedio    : %.2f°C", s[1])
        log.info("   Temp. mín. promedio    : %.2f°C", s[2])
        log.info("   Temperatura más alta   : %.2f°C", s[3])
        log.info("   Temperatura más baja   : %.2f°C", s[4])
        log.info("   Lluvia total acumulada : %.2f mm", s[5])

        log.info("✅ Verificación completada — ve los datos en pgAdmin puerto 5050")
        log.info("   Tabla: lecturas_clima | BD: pipeline_db")

    # ── Pipeline ─────────────────────────────────────────────────────────
    status   = preparar_tabla()
    raw      = extraer_clima(status)
    resultado = insertar_lecturas(raw)
    verificar_datos(resultado)


# Instanciar el DAG
s3_hooks_conexiones()
