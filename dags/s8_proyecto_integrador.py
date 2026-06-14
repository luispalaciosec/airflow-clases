"""
─────────────────────────────────────────────────────────────────────
DAG S8 — Proyecto Integrador
Sesión 8 · Jueves 25 Junio 2026 · Módulo 4

El pipeline que conecta TODO el curso en un solo DAG:

  M2 ETL    → extraer datos desde Open-Meteo API (HttpHook)
  M2 ETL    → transformar con pandas (limpiar, calcular stats)
  M2 ETL    → cargar en tabla resumen_integrador (PostgresHook + upsert)
  M3 ML     → evaluar calidad de datos (gate de calidad)
  M3 ML     → entrenar modelo de clasificación de temperatura
  M3 ML     → registrar experimento en MLflow
  M4 LLM    → construir prompt con los resultados
  M4 LLM    → llamar a gpt-4o-mini para generar reporte ejecutivo
  M4 LLM    → guardar reporte en Postgres + exportar como .txt

Conexiones: postgres_lab, open_meteo_api
Variables:  OPENAI_API_KEY (env), DATA_PATH, OUTPUTS_PATH
Schedule:   None (manual para el ejercicio del lab)
─────────────────────────────────────────────────────────────────────
"""

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
import logging
import os

log = logging.getLogger(__name__)

DATA_PATH    = Variable.get("DATA_PATH",    default_var="/usr/local/airflow/include/data")
OUTPUTS_PATH = Variable.get("OUTPUTS_PATH", default_var="/usr/local/airflow/include/outputs")
MODELS_PATH  = Variable.get("MODELS_PATH",  default_var="/usr/local/airflow/include/models")


@dag(
    dag_id="s8_proyecto_integrador",
    description="S8 — Pipeline integrador: ETL + ML + LLM + Postgres",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    retries=2,
    retry_delay=timedelta(minutes=2),
    tags=["sesion-8", "proyecto", "integrador", "etl", "ml", "llm"],
)
def s8_proyecto_integrador():
    """
    ### S8 — Proyecto Integrador

    Pipeline completo que conecta los 4 módulos del curso:

    ```
    extraer_clima()
         ↓
    transformar_datos()
         ↓
    cargar_postgres()    ←── M2 ETL (S3-S4)
         ↓
    evaluar_calidad()    ←── M3 ML  (S5)
         ↓
    entrenar_modelo()    ←── M3 ML  (S6) + MLflow
         ↓
    generar_reporte()    ←── M4 LLM (S7)
         ↓
    guardar_resultado()  ←── exporta .txt + guarda en Postgres
    ```

    **Conexiones requeridas:**
    - `postgres_lab` → PostgreSQL del curso
    - `open_meteo_api` → API de clima gratuita

    **Variables de entorno:**
    - `OPENAI_API_KEY` → en el archivo .env del Codespace

    **Outputs:**
    - Tabla `resumen_integrador` en PostgreSQL
    - Tabla `ml_metrics` con métricas del modelo
    - Tabla `llm_outputs` con el reporte generado
    - Archivo `.txt` en `include/outputs/`
    """

    # ── TASK 1: Extraer datos del clima (M2 - ETL) ───────────────────
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extraer_clima() -> str:
        """
        M2 — Extrae los últimos 14 días de clima de Guayaquil.
        Usa HttpHook con la conexión open_meteo_api.
        """
        from airflow.providers.http.hooks.http import HttpHook

        hook = HttpHook(method="GET", http_conn_id="open_meteo_api")

        response = hook.run(
            endpoint="/v1/forecast",
            data={
                "latitude":       -2.1962,
                "longitude":      -79.8862,
                "daily": [
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum",
                    "windspeed_10m_max",
                ],
                "timezone":      "America/Guayaquil",
                "past_days":     14,
                "forecast_days": 1,
            }
        )
        response.raise_for_status()
        data = response.json()

        n = len(data["daily"]["time"])
        log.info("✅ [M2-ETL] Extraídos %d días de datos de Guayaquil", n)
        log.info("   Rango: %s → %s",
                 data["daily"]["time"][0], data["daily"]["time"][-1])

        return json.dumps(data["daily"])

    # ── TASK 2: Transformar (M2 - ETL) ───────────────────────────────
    @task
    def transformar_datos(raw_json: str) -> str:
        """
        M2 — Limpia y enriquece los datos con pandas.
        Calcula temperatura promedio, categoría de calor y flag de lluvia.
        """
        import pandas as pd

        data = json.loads(raw_json)

        df = pd.DataFrame({
            "fecha":      data["time"],
            "temp_max":   data["temperature_2m_max"],
            "temp_min":   data["temperature_2m_min"],
            "lluvia_mm":  data["precipitation_sum"],
            "viento_max": data["windspeed_10m_max"],
        })

        # Limpiar nulls
        df = df.dropna(subset=["temp_max", "temp_min"])
        df["lluvia_mm"]  = df["lluvia_mm"].fillna(0.0)
        df["viento_max"] = df["viento_max"].fillna(0.0)

        # Features derivados
        df["temp_promedio"] = ((df["temp_max"] + df["temp_min"]) / 2).round(2)
        df["lluvia_flag"]   = (df["lluvia_mm"] > 5.0).astype(int)

        # Categoría de temperatura (target para el modelo ML)
        # 0 = fresco (<26°C), 1 = cálido (26-30°C), 2 = muy cálido (>30°C)
        df["temp_categoria"] = pd.cut(
            df["temp_promedio"],
            bins=[-float("inf"), 26, 30, float("inf")],
            labels=[0, 1, 2]
        ).astype(int)

        df["ciudad"] = "Guayaquil"

        log.info("✅ [M2-ETL] Datos transformados: %d filas, %d columnas",
                 len(df), len(df.columns))
        log.info("   Distribución de categorías:\n%s",
                 df["temp_categoria"].value_counts().to_string())

        return df.to_json(orient="records")

    # ── TASK 3: Cargar en PostgreSQL (M2 - ETL) ──────────────────────
    @task
    def cargar_postgres(data_json: str) -> dict:
        """
        M2 — Carga los datos en la tabla resumen_integrador.
        Usa upsert (ON CONFLICT) para idempotencia.
        """
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = pd.read_json(data_json)
        hook = PostgresHook(postgres_conn_id="postgres_lab")

        # Crear tabla si no existe
        hook.run("""
            CREATE TABLE IF NOT EXISTS resumen_integrador (
                id              SERIAL PRIMARY KEY,
                fecha           DATE NOT NULL,
                ciudad          VARCHAR(100) NOT NULL,
                temp_max        NUMERIC(5,2),
                temp_min        NUMERIC(5,2),
                temp_promedio   NUMERIC(5,2),
                lluvia_mm       NUMERIC(8,2),
                lluvia_flag     INTEGER DEFAULT 0,
                viento_max      NUMERIC(6,2),
                temp_categoria  INTEGER,
                actualizado_en  TIMESTAMP DEFAULT NOW(),
                UNIQUE(fecha, ciudad)
            );
        """)

        insertadas = 0
        for _, row in df.iterrows():
            hook.run("""
                INSERT INTO resumen_integrador
                    (fecha, ciudad, temp_max, temp_min, temp_promedio,
                     lluvia_mm, lluvia_flag, viento_max, temp_categoria)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (fecha, ciudad) DO UPDATE SET
                    temp_max       = EXCLUDED.temp_max,
                    temp_min       = EXCLUDED.temp_min,
                    temp_promedio  = EXCLUDED.temp_promedio,
                    lluvia_mm      = EXCLUDED.lluvia_mm,
                    lluvia_flag    = EXCLUDED.lluvia_flag,
                    viento_max     = EXCLUDED.viento_max,
                    temp_categoria = EXCLUDED.temp_categoria,
                    actualizado_en = NOW();
            """, parameters=(
                row["fecha"], row["ciudad"],
                row["temp_max"], row["temp_min"], row["temp_promedio"],
                row["lluvia_mm"], row["lluvia_flag"],
                row["viento_max"], row["temp_categoria"],
            ))
            insertadas += 1

        log.info("✅ [M2-ETL] %d registros en resumen_integrador", insertadas)

        # Stats para las siguientes tasks
        stats = hook.get_records("""
            SELECT
                COUNT(*)                        AS total,
                ROUND(AVG(temp_promedio), 2)    AS temp_prom,
                MAX(temp_max)                   AS temp_alta,
                MIN(temp_min)                   AS temp_baja,
                SUM(lluvia_mm)                  AS lluvia_total,
                SUM(lluvia_flag)                AS dias_lluvia
            FROM resumen_integrador
            WHERE ciudad = 'Guayaquil'
              AND fecha >= CURRENT_DATE - INTERVAL '14 days';
        """)[0]

        return {
            "total":        int(stats[0]),
            "temp_prom":    float(stats[1]),
            "temp_alta":    float(stats[2]),
            "temp_baja":    float(stats[3]),
            "lluvia_total": float(stats[4]),
            "dias_lluvia":  int(stats[5]),
            "data_json":    data_json,
        }

    # ── TASK 4: Evaluar calidad + entrenar modelo (M3 - ML) ──────────
    @task
    def entrenar_modelo_mlflow(info: dict) -> dict:
        """
        M3 — Evalúa calidad de datos y entrena un clasificador de
        temperatura con scikit-learn. Registra todo en MLflow.
        """
        import pandas as pd
        import mlflow
        import mlflow.sklearn
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler
        from sklearn.metrics import accuracy_score, f1_score, classification_report

        df = pd.read_json(info["data_json"])

        # Gate de calidad (M3 - S5)
        n = len(df)
        nulls = df[["temp_max","temp_min","lluvia_mm"]].isnull().sum().sum()
        clases = df["temp_categoria"].nunique()

        log.info("🔍 [M3-ML] Evaluación de calidad:")
        log.info("   Muestras    : %d  %s", n, "✅" if n >= 10 else "❌")
        log.info("   Nulos       : %d  %s", nulls, "✅" if nulls == 0 else "⚠️")
        log.info("   Clases      : %d", clases)

        if n < 10:
            log.warning("⚠️  Datos insuficientes para entrenar — saltando ML")
            return {**info, "accuracy": None, "f1": None, "run_id": None}

        # Features y target
        features = ["temp_max", "temp_min", "lluvia_mm", "viento_max", "lluvia_flag"]
        X = df[features].fillna(0)
        y = df["temp_categoria"]

        # Con pocos datos usamos todos para train si no hay suficiente para split
        if n >= 15:
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
        else:
            X_train, X_test, y_train, y_test = X, X, y, y

        scaler = StandardScaler()
        X_train_sc = scaler.fit_transform(X_train)
        X_test_sc  = scaler.transform(X_test)

        # MLflow tracking (M3 - S6)
        tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5001")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("proyecto_integrador_s8")

        params = {"n_estimators": 50, "max_depth": 4, "random_state": 42}

        with mlflow.start_run(run_name="s8_clima_guayaquil") as run:
            mlflow.log_params(params)
            mlflow.log_param("n_muestras", n)
            mlflow.log_param("features",   features)

            modelo = RandomForestClassifier(**params)
            modelo.fit(X_train_sc, y_train)

            y_pred   = modelo.predict(X_test_sc)
            accuracy = round(accuracy_score(y_test, y_pred), 4)
            f1       = round(f1_score(y_test, y_pred, average="weighted", zero_division=0), 4)

            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1_score", f1)
            mlflow.sklearn.log_model(modelo, "model")

            run_id = run.info.run_id

        log.info("✅ [M3-ML] Modelo entrenado y registrado en MLflow")
        log.info("   Accuracy : %.4f", accuracy)
        log.info("   F1-Score : %.4f", f1)
        log.info("   Run ID   : %s", run_id)

        # Guardar métricas en Postgres
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_lab")
        hook.run("""
            CREATE TABLE IF NOT EXISTS ml_metrics (
                id          SERIAL PRIMARY KEY,
                run_id      VARCHAR(50),
                dag_id      VARCHAR(100),
                modelo      VARCHAR(100),
                accuracy    NUMERIC(6,4),
                f1_score    NUMERIC(6,4),
                n_muestras  INTEGER,
                creado_en   TIMESTAMP DEFAULT NOW()
            );
        """)
        hook.run("""
            INSERT INTO ml_metrics (run_id, dag_id, modelo, accuracy, f1_score, n_muestras)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, parameters=(run_id, "s8_proyecto_integrador",
                          "RandomForestClassifier", accuracy, f1, n))

        return {**info, "accuracy": accuracy, "f1": f1, "run_id": run_id}

    # ── TASK 5: Generar reporte con LLM (M4 - LLM) ───────────────────
    @task
    def generar_reporte_llm(info: dict) -> str:
        """
        M4 — Construye un prompt con todos los resultados del pipeline
        y llama a gpt-4o-mini para generar un reporte ejecutivo.
        """
        import os
        try:
            from openai import OpenAI
        except ImportError:
            log.warning("⚠️  openai no instalado — usando reporte de placeholder")
            return _reporte_placeholder(info)

        api_key = os.environ.get("OPENAI_API_KEY", "")
        if not api_key or api_key.startswith("sk-placeholder"):
            log.warning("⚠️  OPENAI_API_KEY no configurada — usando placeholder")
            return _reporte_placeholder(info)

        # Construir prompt con contexto real del pipeline
        acc_str = f"{info['accuracy']*100:.1f}%" if info.get("accuracy") else "N/A"
        f1_str  = f"{info['f1']:.4f}"            if info.get("f1")       else "N/A"

        prompt = f"""Eres un analista de datos experto. Genera un reporte ejecutivo
en español basado en los siguientes resultados de un pipeline de datos de Guayaquil, Ecuador.

## Datos del período (últimos 14 días)
- Total de días analizados: {info['total']}
- Temperatura promedio: {info['temp_prom']}°C
- Temperatura más alta registrada: {info['temp_alta']}°C
- Temperatura más baja registrada: {info['temp_baja']}°C
- Lluvia total acumulada: {info['lluvia_total']} mm
- Días con lluvia significativa (>5mm): {info['dias_lluvia']}

## Modelo de Machine Learning
- Modelo entrenado: RandomForest para clasificación de temperatura
- Accuracy obtenida: {acc_str}
- F1-Score: {f1_str}

## Instrucciones para el reporte
Escribe 3 párrafos concisos:
1. Resumen del clima del período con los datos más relevantes
2. Interpretación del modelo ML y qué tan confiable es para predecir categorías de temperatura
3. Recomendación ejecutiva breve (ej: actividades, precauciones, tendencias)

Usa un tono profesional pero accesible. Máximo 250 palabras."""

        client   = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.7,
        )

        reporte      = response.choices[0].message.content
        prompt_tok   = response.usage.prompt_tokens
        complete_tok = response.usage.completion_tokens

        log.info("✅ [M4-LLM] Reporte generado por gpt-4o-mini")
        log.info("   Tokens prompt    : %d", prompt_tok)
        log.info("   Tokens completion: %d", complete_tok)
        log.info("   Costo estimado   : ~$%.5f USD",
                 (prompt_tok * 0.00015 + complete_tok * 0.0006) / 1000)
        log.info("\n--- REPORTE ---\n%s\n--- FIN ---", reporte)

        return json.dumps({
            "reporte":       reporte,
            "prompt_tokens": prompt_tok,
            "comp_tokens":   complete_tok,
        })

    def _reporte_placeholder(info: dict) -> str:
        """Reporte de fallback cuando no hay API key."""
        reporte = (
            f"REPORTE PLACEHOLDER — configura OPENAI_API_KEY para el reporte real.\n\n"
            f"Período analizado: {info['total']} días de Guayaquil.\n"
            f"Temperatura promedio: {info['temp_prom']}°C | "
            f"Máxima: {info['temp_alta']}°C | Mínima: {info['temp_baja']}°C.\n"
            f"Lluvia acumulada: {info['lluvia_total']} mm en {info['dias_lluvia']} días.\n"
            f"Modelo ML: accuracy={info.get('accuracy','N/A')} | "
            f"F1={info.get('f1','N/A')}."
        )
        return json.dumps({"reporte": reporte, "prompt_tokens": 0, "comp_tokens": 0})

    # ── TASK 6: Guardar resultado final (M2+M4) ───────────────────────
    @task
    def guardar_resultado(info: dict, llm_json: str):
        """
        Guarda el reporte en Postgres (llm_outputs) y exporta como .txt.
        Cierra el loop del pipeline integrador.
        """
        import os
        from datetime import datetime
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        llm_data = json.loads(llm_json)
        reporte  = llm_data["reporte"]
        hook     = PostgresHook(postgres_conn_id="postgres_lab")

        # Crear tabla si no existe
        hook.run("""
            CREATE TABLE IF NOT EXISTS llm_outputs (
                id               SERIAL PRIMARY KEY,
                dag_id           VARCHAR(100),
                modelo           VARCHAR(50),
                prompt_tokens    INTEGER,
                completion_tokens INTEGER,
                resumen          TEXT,
                creado_en        TIMESTAMP DEFAULT NOW()
            );
        """)

        hook.run("""
            INSERT INTO llm_outputs
                (dag_id, modelo, prompt_tokens, completion_tokens, resumen)
            VALUES (%s, %s, %s, %s, %s);
        """, parameters=(
            "s8_proyecto_integrador",
            "gpt-4o-mini",
            llm_data.get("prompt_tokens", 0),
            llm_data.get("comp_tokens", 0),
            reporte,
        ))

        # Exportar como .txt
        os.makedirs(OUTPUTS_PATH, exist_ok=True)
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(OUTPUTS_PATH, f"reporte_clima_{ts}.txt")

        with open(filename, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write("REPORTE INTEGRADOR — AIRFLOW PIPELINE S8\n")
            f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 60 + "\n\n")
            f.write(reporte)
            f.write("\n\n")
            f.write("-" * 60 + "\n")
            f.write(f"Accuracy del modelo: {info.get('accuracy','N/A')}\n")
            f.write(f"F1-Score:            {info.get('f1','N/A')}\n")
            f.write(f"MLflow Run ID:       {info.get('run_id','N/A')}\n")
            f.write(f"Datos:               {info['total']} días analizados\n")
            f.write("-" * 60 + "\n")

        log.info("=" * 55)
        log.info("🏁  PIPELINE INTEGRADOR COMPLETADO")
        log.info("   Clima extraído   : %d días", info["total"])
        log.info("   Temp. promedio   : %.2f°C", info["temp_prom"])
        log.info("   Lluvia total     : %.1f mm", info["lluvia_total"])
        log.info("   ML accuracy      : %s", info.get("accuracy", "N/A"))
        log.info("   Reporte guardado : %s", filename)
        log.info("   Tablas Postgres  : resumen_integrador, ml_metrics, llm_outputs")
        log.info("=" * 55)

    # ── Pipeline ─────────────────────────────────────────────────────
    raw       = extraer_clima()
    limpio    = transformar_datos(raw)
    info      = cargar_postgres(limpio)
    info_ml   = entrenar_modelo_mlflow(info)
    llm_out   = generar_reporte_llm(info_ml)
    guardar_resultado(info_ml, llm_out)


s8_proyecto_integrador()
