"""
─────────────────────────────────────────────────────────────────────
DAG S2 — Operadores y branching
Sesión 2 · Módulo 1

Objetivo: practicar BranchPythonOperator, XComs y dependencias
condicionales.
─────────────────────────────────────────────────────────────────────
"""

from airflow.decorators import dag, task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import random

log = logging.getLogger(__name__)


@dag(
    dag_id="s2_operadores_branching",
    description="Sesión 2 — BranchOperator, XComs y dependencias",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["sesion-2", "modulo-1", "operadores"],
)
def operadores_branching():
    """
    ### Operadores y branching
    Simula un pipeline que decide qué ruta seguir según el valor de un dato.

    **Flujo:**
    1. `generar_numero` → produce un número aleatorio
    2. `decidir_ruta` → bifurca según si es par o impar
    3. `procesar_par` / `procesar_impar` → ramas alternativas
    4. `fin` → converge siempre
    """

    @task
    def generar_numero() -> int:
        numero = random.randint(1, 100)
        log.info("🎲 Número generado: %d", numero)
        return numero

    def decidir_ruta_fn(**context):
        """Función de branching — recibe XCom y devuelve el task_id a ejecutar."""
        numero = context["ti"].xcom_pull(task_ids="generar_numero")
        log.info("🔀 Decidiendo ruta para número: %d", numero)
        if numero % 2 == 0:
            return "procesar_par"
        return "procesar_impar"

    @task
    def procesar_par():
        log.info("✅ Número PAR detectado — ruta de procesamiento A")
        return "par"

    @task
    def procesar_impar():
        log.info("✅ Número IMPAR detectado — ruta de procesamiento B")
        return "impar"

    # ── Dependencias ─────────────────────────────────────────────────────────
    inicio = generar_numero()

    branch = BranchPythonOperator(
        task_id="decidir_ruta",
        python_callable=decidir_ruta_fn,
    )

    fin = EmptyOperator(
        task_id="fin",
        trigger_rule="none_failed_min_one_success",  # converge ambas ramas
    )

    par = procesar_par()
    impar = procesar_impar()

    inicio >> branch >> [par, impar] >> fin


operadores_branching()
