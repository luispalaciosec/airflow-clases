"""
─────────────────────────────────────────────────────────────────────
DAG S6 — Pipeline de ML con scikit-learn + MLflow
Sesión 6 · Módulo 3

Objetivo: orquestar entrenamiento de un modelo, registrar métricas
en MLflow y guardar el modelo en include/models/.
─────────────────────────────────────────────────────────────────────
"""

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging
import json
import os

log = logging.getLogger(__name__)


@dag(
    dag_id="s6_pipeline_ml_mlflow",
    description="Sesión 6 — Entrenamiento de modelo con tracking en MLflow",
    schedule="@weekly",
    start_date=days_ago(1),
    catchup=False,
    tags=["sesion-6", "modulo-3", "ml", "mlflow", "sklearn"],
)
def pipeline_ml_mlflow():
    """
    ### Pipeline ML: datos → entrenamiento → MLflow → Postgres
    Entrena un modelo de clasificación con scikit-learn,
    registra métricas en MLflow y guarda el modelo serializado.

    **Servicios:** postgres_lab · mlflow (puerto 5001)
    """

    @task
    def cargar_datos() -> str:
        """Carga datos de entrenamiento (dataset Iris como ejemplo del curso)."""
        from sklearn.datasets import load_iris
        import pandas as pd

        iris = load_iris(as_frame=True)
        df = iris.frame.rename(columns={"target": "label"})
        log.info("📦 Dataset cargado: %d filas, %d columnas", *df.shape)
        log.info("📊 Distribución de clases:\n%s", df["label"].value_counts().to_string())
        return df.to_json(orient="records")

    @task
    def preprocesar(data_json: str) -> dict:
        """Divide datos en train/test y escala features."""
        import pandas as pd
        from sklearn.model_selection import train_test_split
        from sklearn.preprocessing import StandardScaler

        df = pd.read_json(data_json)
        X = df.drop("label", axis=1)
        y = df["label"]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        scaler = StandardScaler()
        X_train_sc = scaler.fit_transform(X_train)
        X_test_sc = scaler.transform(X_test)

        log.info("✂️  Train: %d muestras | Test: %d muestras", len(X_train), len(X_test))
        return {
            "X_train": X_train_sc.tolist(),
            "X_test": X_test_sc.tolist(),
            "y_train": y_train.tolist(),
            "y_test": y_test.tolist(),
        }

    @task
    def entrenar_y_registrar(splits: dict) -> dict:
        """Entrena el modelo y registra todo en MLflow."""
        import numpy as np
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import accuracy_score, f1_score, classification_report
        import mlflow
        import mlflow.sklearn
        import joblib

        mlflow_uri = Variable.get("MLFLOW_TRACKING_URI", default_var="http://mlflow:5001")
        local_artifact_path = "/tmp/mlflow_artifacts"
        os.makedirs(local_artifact_path, exist_ok=True)
       
        mlflow.set_tracking_uri(mlflow_uri) 

        # Estructura correcta para crear el experimento con ruta alterna si no existe
        nombre_experimento = "curso-airflow-ml-v5"
        exp = mlflow.get_experiment_by_name(nombre_experimento)
        
        if exp is None:
            mlflow.create_experiment(
                name=nombre_experimento,
                artifact_location="/tmp/mlruns_artifacts"
            )
        
        mlflow.set_experiment(nombre_experimento)

        X_train = np.array(splits["X_train"])
        X_test = np.array(splits["X_test"])
        y_train = splits["y_train"]
        y_test = splits["y_test"]

        hiperparametros = {"n_estimators": 100, "max_depth": 5, "random_state": 42}
        

        with mlflow.start_run() as run:
            # Entrenar
            modelo = RandomForestClassifier(**hiperparametros)
            modelo.fit(X_train, y_train)

            # Evaluar
            y_pred = modelo.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred, average="weighted")

            # Registrar en MLflow
            mlflow.log_params(hiperparametros)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1_score", f1)
            mlflow.sklearn.log_model(modelo, "random_forest_iris")

            run_id = run.info.run_id
            log.info("🎯 Accuracy: %.4f | F1: %.4f | MLflow run: %s", accuracy, f1, run_id)
            log.info("\n%s", classification_report(y_test, y_pred))

        # Guardar modelo en include/models/
        models_path = Variable.get("MODELS_PATH", default_var="/usr/local/airflow/include/models")
        os.makedirs(models_path, exist_ok=True)
        model_path = os.path.join(models_path, "random_forest_iris.pkl")
        joblib.dump(modelo, model_path)
        log.info("💾 Modelo guardado en: %s", model_path)

        return {
            "run_id": run_id,
            "accuracy": round(accuracy, 4),
            "f1_score": round(f1, 4),
            "modelo": "RandomForestClassifier",
        }

    @task
    def guardar_metricas(metricas: dict):
        """Persiste las métricas del modelo en Postgres para historial."""
        hook = PostgresHook(postgres_conn_id="postgres_lab")
        hook.run("""
            INSERT INTO ml_metrics (modelo, version, accuracy, f1_score, mlflow_run)
            VALUES (%s, %s, %s, %s, %s);
        """, parameters=(
            metricas["modelo"],
            "v1.0",
            metricas["accuracy"],
            metricas["f1_score"],
            metricas["run_id"],
        ))
        log.info("📋 Métricas guardadas — accuracy=%.4f", metricas["accuracy"])

    # ── Pipeline ─────────────────────────────────────────────────────────────
    datos = cargar_datos()
    splits = preprocesar(datos)
    metricas = entrenar_y_registrar(splits)
    guardar_metricas(metricas)


pipeline_ml_mlflow()
