"""
─────────────────────────────────────────────────────────────────────
DAG S1 — Mi primer DAG
Sesión 1 · Módulo 1

Objetivo: verificar que el entorno funciona y entender la anatomía
de un DAG: tasks, dependencias, logs.
─────────────────────────────────────────────────────────────────────
"""

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime
import logging

log = logging.getLogger(__name__)


@dag(
    dag_id="s1_mi_primer_dag",
    description="Sesión 1 — verificación del entorno y primer pipeline",
    schedule=None,                  # manual: no corre automáticamente
    start_date=days_ago(1),
    catchup=False,
    tags=["sesion-1", "modulo-1", "fundamentos"],
)
def mi_primer_dag():
    """
    ### Mi primer DAG 🚀
    Verifica que Airflow, Python y las conexiones están funcionando.

    **Ejecutar:** click en ▶ Trigger DAG en la UI.
    """

    @task
    def verificar_entorno():
        """Task 1: comprueba versiones y dependencias instaladas."""
        import sys
        import pandas as pd
        import sklearn

        info = {
            "python": sys.version,
            "pandas": pd.__version__,
            "scikit-learn": sklearn.__version__,
        }
        log.info("✅ Entorno verificado: %s", info)
        return info

    @task
    def saludar(info: dict):
        """Task 2: recibe el output de la task anterior via XCom."""
        nombre = "estudiante"
        mensaje = f"Hola {nombre}! Python {info['python'][:6]} listo."
        log.info("👋 %s", mensaje)
        return mensaje

    @task
    def contar_hasta(mensaje: str, limite: int = 5):
        """Task 3: bucle simple para ver logs en tiempo real."""
        for i in range(1, limite + 1):
            log.info("Contando: %d / %d", i, limite)
        log.info("✅ Task completada. Mensaje recibido: '%s'", mensaje)
        return {"conteo": limite, "completado": True}

    # ── Dependencias: cada task recibe el output de la anterior ──────────────
    info = verificar_entorno()
    msg = saludar(info)
    contar_hasta(msg)


# Instanciar el DAG
mi_primer_dag()
