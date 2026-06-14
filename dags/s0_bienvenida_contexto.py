"""
─────────────────────────────────────────────────────────────────────
DAG S0 — Bienvenida y Contexto (Dummy DAG)
Sesión 0 · Pre-curso

Propósito: DAG de tipo dummy para verificar que el entorno
funciona correctamente ANTES de la primera clase real.

No hace nada útil — ese es el punto.
Sirve para que el alumno:
  1. Vea que Airflow está corriendo
  2. Aprenda a activar un DAG
  3. Aprenda a triggear un run manual
  4. Aprenda a ver los logs
  5. Entienda los estados: queued → running → success

Las 3 tasks son @task Python que solo imprimen mensajes
y duermen brevemente — nada de conexiones ni dependencias externas.
─────────────────────────────────────────────────────────────────────
"""

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import time
import logging

log = logging.getLogger(__name__)


@dag(
    dag_id="s0_bienvenida_contexto",
    description="S0 — DAG dummy para verificar el entorno antes del curso",
    schedule=None,          # siempre manual
    start_date=days_ago(1),
    catchup=False,
    tags=["sesion-0", "dummy", "bienvenida"],
)
def s0_bienvenida_contexto():
    """
    ### S0 — DAG de Bienvenida

    **Este DAG no hace nada útil — y eso está bien.**

    Es tu primer contacto con Airflow. Antes de construir
    pipelines reales necesitas saber cómo funciona la interfaz.

    **¿Qué aprender con este DAG?**

    1. Activar un DAG con el toggle de la izquierda
    2. Ejecutarlo con el botón ▶ (Trigger DAG)
    3. Ver el flujo en **Graph View** — las 3 cajas en verde
    4. Hacer click en una tarea → **Logs** → leer el output
    5. Ver los **XComs** — cómo la Task 1 le pasa datos a la Task 2

    **Estados de una tarea:**
    - 🟡 queued → esperando un worker
    - 🔵 running → ejecutándose ahora
    - 🟢 success → completada sin errores
    - 🔴 failed → ocurrió un error (ver logs)
    - ⚪ skipped → fue omitida (branching)
    """

    @task
    def verificar_entorno() -> dict:
        """
        Task 1 — Verifica que Python y las librerías del curso estén OK.
        Retorna un dict con las versiones — visible en los XComs.
        """
        import sys
        import platform

        log.info("=" * 50)
        log.info("👋  Bienvenido al curso de Airflow + IA")
        log.info("=" * 50)
        log.info("")
        log.info("🔍 Verificando el entorno...")

        info = {}

        # Python
        info["python"] = sys.version.split()[0]
        log.info("  ✅ Python %s", info["python"])

        # pandas
        try:
            import pandas as pd
            info["pandas"] = pd.__version__
            log.info("  ✅ pandas %s", info["pandas"])
        except ImportError:
            info["pandas"] = "NO INSTALADO"
            log.warning("  ❌ pandas no encontrado")

        # scikit-learn
        try:
            import sklearn
            info["sklearn"] = sklearn.__version__
            log.info("  ✅ scikit-learn %s", info["sklearn"])
        except ImportError:
            info["sklearn"] = "NO INSTALADO"
            log.warning("  ❌ scikit-learn no encontrado")

        # mlflow
        try:
            import mlflow
            info["mlflow"] = mlflow.__version__
            log.info("  ✅ mlflow %s", info["mlflow"])
        except ImportError:
            info["mlflow"] = "NO INSTALADO"
            log.warning("  ❌ mlflow no encontrado")

        # requests
        try:
            import requests
            info["requests"] = requests.__version__
            log.info("  ✅ requests %s", info["requests"])
        except ImportError:
            info["requests"] = "NO INSTALADO"
            log.warning("  ❌ requests no encontrado")

        log.info("")
        log.info("💡 Este dict aparece en los XComs de esta tarea.")
        log.info("   Haz click en: Task → XCom para verlo.")
        log.info("")
        log.info("✅ Task 1 completada — entorno verificado")

        return info

    @task
    def saludar(entorno: dict) -> str:
        """
        Task 2 — Recibe el dict de la Task 1 via XCom y genera un saludo.
        Demuestra cómo las tasks se pasan datos entre sí.
        """
        log.info("=" * 50)
        log.info("📦 Datos recibidos de la Task 1 via XCom:")
        log.info("=" * 50)

        for lib, version in entorno.items():
            estado = "✅" if version != "NO INSTALADO" else "❌"
            log.info("   %s  %-15s  %s", estado, lib, version)

        log.info("")
        log.info("🎓 BIENVENIDO AL CURSO")
        log.info("")
        log.info("   Pipelines de Datos con Apache Airflow 2.x + IA")
        log.info("   20 horas · 8 sesiones · 75% práctica")
        log.info("")
        log.info("   Módulos que veremos:")
        log.info("   M1 → Fundamentos de Airflow (S1-S2)")
        log.info("   M2 → ETL Real con Hooks y PostgreSQL (S3-S4)")
        log.info("   M3 → ML Pipelines con scikit-learn + MLflow (S5-S6)")
        log.info("   M4 → LLMs en DAGs con OpenAI/Gemini (S7-S8)")
        log.info("")

        mensaje = (
            f"Entorno OK — Python {entorno.get('python','?')} | "
            f"pandas {entorno.get('pandas','?')} | "
            f"sklearn {entorno.get('sklearn','?')} | "
            f"mlflow {entorno.get('mlflow','?')}"
        )

        log.info("📝 Mensaje para la siguiente task:")
        log.info("   %s", mensaje)
        log.info("")
        log.info("✅ Task 2 completada — saludo enviado")

        return mensaje

    @task
    def contar_hasta(mensaje: str):
        """
        Task 3 — Cuenta hasta 5 con una pausa breve.
        Demuestra un proceso que tarda tiempo y cómo se ve en los logs.
        """
        log.info("=" * 50)
        log.info("🔢 Contando hasta 5 (con pausa de 1 segundo)")
        log.info("=" * 50)
        log.info("")
        log.info("Mensaje recibido de Task 2: %s", mensaje)
        log.info("")

        for i in range(1, 6):
            log.info("  Paso %d de 5 ... ✓", i)
            time.sleep(1)

        log.info("")
        log.info("=" * 50)
        log.info("🚀  TODO LISTO — El entorno funciona correctamente")
        log.info("")
        log.info("   Próximos pasos:")
        log.info("   1. Abre pgAdmin en el puerto 5050")
        log.info("   2. Abre MLflow en el puerto 5001")
        log.info("   3. Mañana empieza S1 — tu primer DAG real")
        log.info("=" * 50)

    # ── Cadena de tasks ───────────────────────────────────────────────
    entorno  = verificar_entorno()
    mensaje  = saludar(entorno)
    contar_hasta(mensaje)


s0_bienvenida_contexto()
