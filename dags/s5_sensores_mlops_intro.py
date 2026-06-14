"""
─────────────────────────────────────────────────────────────────────
DAG S5 — Sensores y MLOps: esperar condiciones + ciclo de vida ML
Sesión 5 · Módulo 3

Objetivo: practicar Sensores de Airflow y entender el ciclo de vida
de un modelo de ML en producción antes de pasar al pipeline
completo de S6.

Este DAG tiene DOS partes independientes que puedes ejecutar:

PARTE A — FileSensor
  Simula un pipeline que espera a que exista un archivo CSV
  antes de procesarlo. Crea el archivo como parte del ejercicio.

PARTE B — MLOps intro: datos → preparar → evaluar → decidir
  Pipeline simple que evalúa si los datos son suficientes para
  entrenar un modelo, y toma una decisión de branching basada
  en calidad de datos — conectando S2 (branching) con S6 (ML).

Conexiones requeridas: postgres_lab
─────────────────────────────────────────────────────────────────────
"""

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import os

log = logging.getLogger(__name__)

DATA_PATH = Variable.get("DATA_PATH", default_var="/usr/local/airflow/include/data")
MODELS_PATH = Variable.get("MODELS_PATH", default_var="/usr/local/airflow/include/models")


# ══════════════════════════════════════════════════════════════════════════
# DAG A — FileSensor: esperar un archivo antes de procesar
# ══════════════════════════════════════════════════════════════════════════
@dag(
    dag_id="s5a_file_sensor",
    description="Sesión 5A — FileSensor: esperar archivo CSV antes de procesar",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["sesion-5", "modulo-3", "sensor", "filesensor"],
)
def s5a_file_sensor():
    """
    ### S5A — FileSensor en acción
    Simula el patrón más común de sensores:
    **esperar un archivo antes de procesarlo**.

    **Flujo:**
    1. `crear_archivo_demo` → genera un CSV de ventas en include/data/
    2. `esperar_archivo` → FileSensor que hace poke cada 10 segundos
    3. `procesar_csv` → lee el archivo y calcula estadísticas
    4. `archivar` → mueve el archivo a outputs/ como procesado

    **Para ver el sensor en acción:**
    - Desactiva la Task 1 en el Graph y ejecuta el DAG
    - El sensor quedará en amarillo (poking) hasta que crees el archivo
    - Créalo con: `touch /usr/local/airflow/include/data/ventas_demo.csv`
    """

    @task
    def crear_archivo_demo():
        """Genera el CSV que el sensor va a esperar."""
        import csv
        from datetime import datetime, timedelta
        import random

        filepath = os.path.join(DATA_PATH, "ventas_demo.csv")
        os.makedirs(DATA_PATH, exist_ok=True)

        productos = ["Laptop", "Monitor", "Teclado", "Mouse", "Auriculares"]
        vendedores = ["Ana García", "Carlos López", "María Rodríguez"]

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["fecha", "producto", "cantidad", "precio", "vendedor"])
            for i in range(20):
                fecha = (datetime.now() - timedelta(days=i % 7)).strftime("%Y-%m-%d")
                producto = random.choice(productos)
                cantidad = random.randint(1, 10)
                precio   = round(random.uniform(25, 1200), 2)
                vendedor = random.choice(vendedores)
                writer.writerow([fecha, producto, cantidad, precio, vendedor])

        log.info("✅ Archivo creado: %s (20 registros)", filepath)
        return filepath

    # FileSensor — hace poke cada 10 segundos, timeout 5 minutos
    esperar = FileSensor(
        task_id="esperar_archivo",
        filepath=os.path.join(DATA_PATH, "ventas_demo.csv"),
        poke_interval=10,           # revisar cada 10 segundos
        timeout=300,                # máximo 5 minutos esperando
        mode="poke",                # "reschedule" libera el worker mientras espera
        soft_fail=False,            # falla duro si no aparece el archivo
    )

    @task
    def procesar_csv() -> dict:
        """Lee el CSV y calcula estadísticas básicas."""
        import csv

        filepath = os.path.join(DATA_PATH, "ventas_demo.csv")

        totales_por_producto = {}
        total_ventas = 0.0
        n_registros = 0

        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                n_registros += 1
                total = float(row["cantidad"]) * float(row["precio"])
                total_ventas += total
                prod = row["producto"]
                totales_por_producto[prod] = totales_por_producto.get(prod, 0) + total

        log.info("=" * 50)
        log.info("📊 REPORTE DEL CSV PROCESADO")
        log.info("   Registros  : %d", n_registros)
        log.info("   Total ventas: $%.2f", total_ventas)
        log.info("   Por producto:")
        for prod, total in sorted(totales_por_producto.items(),
                                   key=lambda x: x[1], reverse=True):
            log.info("     %-15s $%.2f", prod, total)
        log.info("=" * 50)

        return {
            "n_registros": n_registros,
            "total_ventas": round(total_ventas, 2),
            "productos": list(totales_por_producto.keys()),
        }

    @task
    def archivar(stats: dict):
        """Mueve el CSV a outputs/ indicando que fue procesado."""
        import shutil
        from datetime import datetime

        src = os.path.join(DATA_PATH, "ventas_demo.csv")
        outputs_path = Variable.get("OUTPUTS_PATH",
                                    default_var="/usr/local/airflow/include/outputs")
        os.makedirs(outputs_path, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dst = os.path.join(outputs_path, f"ventas_procesadas_{timestamp}.csv")
        shutil.move(src, dst)

        log.info("📁 Archivo archivado en: %s", dst)
        log.info("   Registros procesados: %d", stats["n_registros"])
        log.info("   Total ventas: $%.2f", stats["total_ventas"])

    # ── Pipeline ─────────────────────────────────────────────────────────
    crear = crear_archivo_demo()
    crear >> esperar
    stats = procesar_csv()
    esperar >> stats
    archivar(stats)


s5a_file_sensor()


# ══════════════════════════════════════════════════════════════════════════
# DAG B — MLOps intro: ciclo de vida de un modelo
# ══════════════════════════════════════════════════════════════════════════
@dag(
    dag_id="s5b_mlops_intro",
    description="Sesión 5B — Ciclo de vida ML: datos → evaluar → decidir → preparar",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["sesion-5", "modulo-3", "mlops", "branching", "sklearn"],
)
def s5b_mlops_intro():
    """
    ### S5B — MLOps: el ciclo de vida de un modelo
    Pipeline que introduce los conceptos de MLOps antes del
    pipeline completo de S6.

    **Flujo con branching:**
    ```
    cargar_datos → evaluar_calidad → [entrenar_mini / datos_insuficientes]
                                           ↓
                                    guardar_modelo_demo
    ```

    **Lo que aprenderás:**
    - Cómo evaluar si los datos son aptos para entrenar
    - BranchOperator en contexto ML (conecta con S2)
    - Pipeline simple con scikit-learn dentro de un @task
    - Cómo guardar y cargar un modelo con joblib
    - Preparación para MLflow en S6
    """

    @task
    def cargar_datos() -> str:
        """
        Carga datos de entrenamiento desde Postgres o usa el dataset Iris.
        Evalúa si hay suficientes registros para entrenar un modelo real.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Intentar cargar desde la tabla de ventas del curso
        try:
            hook = PostgresHook(postgres_conn_id="postgres_lab")
            registros = hook.get_records("SELECT COUNT(*) FROM ventas;")
            n_ventas = registros[0][0]
            log.info("📦 Registros en tabla ventas: %d", n_ventas)
        except Exception:
            n_ventas = 0
            log.warning("⚠️  Tabla ventas no encontrada — usando dataset alternativo")

        # Para el lab siempre usamos Iris (dataset estándar de ML)
        from sklearn.datasets import load_iris
        import pandas as pd

        iris = load_iris(as_frame=True)
        df = iris.frame.rename(columns={"target": "label"})

        log.info("📊 Dataset cargado: %d muestras, %d features",
                 len(df), len(df.columns) - 1)
        log.info("   Clases: %s", list(iris.target_names))
        log.info("   Balance de clases:\n%s", df["label"].value_counts().to_string())

        return df.to_json(orient="records")

    @task
    def evaluar_calidad(data_json: str) -> dict:
        """
        Evalúa la calidad del dataset antes de entrenar.
        En producción esto incluiría: detección de drift, outliers, etc.
        """
        import pandas as pd

        df = pd.read_json(data_json)

        n_muestras = len(df)
        n_features = len(df.columns) - 1
        n_clases = df["label"].nunique()
        nulls = df.isnull().sum().sum()
        min_clase = df["label"].value_counts().min()

        # Criterios de calidad mínimos
        suficientes_muestras = n_muestras >= 50
        no_hay_nulls         = nulls == 0
        balance_ok           = min_clase >= 10
        apto_para_entrenar   = suficientes_muestras and no_hay_nulls and balance_ok

        log.info("🔍 EVALUACIÓN DE CALIDAD DE DATOS")
        log.info("   Muestras        : %d  %s", n_muestras,
                 "✅" if suficientes_muestras else "❌")
        log.info("   Features        : %d", n_features)
        log.info("   Clases          : %d", n_clases)
        log.info("   Nulos           : %d  %s", nulls, "✅" if no_hay_nulls else "❌")
        log.info("   Mín. por clase  : %d  %s", min_clase,
                 "✅" if balance_ok else "❌")
        log.info("   → APTO para entrenar: %s", "✅ SÍ" if apto_para_entrenar else "❌ NO")

        return {
            "n_muestras": n_muestras,
            "n_features": n_features,
            "n_clases": n_clases,
            "apto": apto_para_entrenar,
            "data_json": data_json,
        }

    def decidir_entrenamiento(**context):
        """BranchOperator: decide si entrenar o reportar datos insuficientes."""
        calidad = context["ti"].xcom_pull(task_ids="evaluar_calidad")
        if calidad["apto"]:
            log.info("✅ Datos aptos — ruta: entrenar_mini_modelo")
            return "entrenar_mini_modelo"
        log.warning("❌ Datos NO aptos — ruta: datos_insuficientes")
        return "datos_insuficientes"

    branch = BranchPythonOperator(
        task_id="decidir_entrenamiento",
        python_callable=decidir_entrenamiento,
    )

    @task
    def entrenar_mini_modelo():
        """
        Entrena un modelo simple de clasificación.
        Introducción a scikit-learn antes del pipeline completo en S6.
        En S6 esto se hará con tracking en MLflow.
        """
        from sklearn.datasets import load_iris
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler
        from sklearn.tree import DecisionTreeClassifier
        from sklearn.metrics import accuracy_score, classification_report
        import joblib

        log.info("🤖 Iniciando entrenamiento del modelo demo...")

        # Cargar y dividir datos
        iris = load_iris()
        X_train, X_test, y_train, y_test = train_test_split(
            iris.data, iris.target,
            test_size=0.2, random_state=42, stratify=iris.target
        )

        # Escalar features
        scaler = StandardScaler()
        X_train_sc = scaler.fit_transform(X_train)
        X_test_sc  = scaler.transform(X_test)

        # Entrenar un árbol de decisión (simple, interpretable)
        modelo = DecisionTreeClassifier(max_depth=4, random_state=42)
        modelo.fit(X_train_sc, y_train)

        # Evaluar
        y_pred = modelo.predict(X_test_sc)
        accuracy = accuracy_score(y_test, y_pred)

        log.info("📊 RESULTADOS DEL ENTRENAMIENTO")
        log.info("   Modelo    : DecisionTreeClassifier (max_depth=4)")
        log.info("   Train     : %d muestras", len(X_train))
        log.info("   Test      : %d muestras", len(X_test))
        log.info("   Accuracy  : %.4f  (%.1f%%)", accuracy, accuracy * 100)
        log.info("\n%s", classification_report(y_test, y_pred,
                                               target_names=iris.target_names))

        # Guardar el modelo en include/models/
        os.makedirs(MODELS_PATH, exist_ok=True)
        model_path = os.path.join(MODELS_PATH, "demo_iris_tree.pkl")
        joblib.dump({"modelo": modelo, "scaler": scaler}, model_path)
        log.info("💾 Modelo guardado en: %s", model_path)
        log.info("📌 En S6 haremos esto mismo pero con MLflow — verás las métricas en la UI")

        return {
            "accuracy": round(accuracy, 4),
            "model_path": model_path,
            "n_train": len(X_train),
        }

    @task
    def datos_insuficientes():
        """Rama alternativa cuando los datos no son aptos."""
        log.warning("⚠️  Los datos no cumplen los criterios mínimos de calidad.")
        log.warning("   Acciones recomendadas:")
        log.warning("   1. Verificar fuentes de datos")
        log.warning("   2. Revisar proceso de ETL upstream")
        log.warning("   3. Aumentar el período de recolección de datos")

    fin = EmptyOperator(
        task_id="fin",
        trigger_rule="none_failed_min_one_success",
    )

    # ── Pipeline con branching ────────────────────────────────────────────
    datos    = cargar_datos()
    calidad  = evaluar_calidad(datos)
    entrenar = entrenar_mini_modelo()
    insuf    = datos_insuficientes()

    calidad >> branch >> [entrenar, insuf] >> fin


s5b_mlops_intro()
