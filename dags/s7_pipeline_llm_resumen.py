"""
─────────────────────────────────────────────────────────────────────
DAG S7 — Pipeline con LLM: datos → resumen automático con GPT
Sesión 7 · Módulo 4

Objetivo: integrar la API de OpenAI en un DAG real.
El pipeline extrae datos de Postgres, construye un prompt
y genera un resumen ejecutivo con gpt-4o-mini.

Costo estimado por ejecución: ~$0.01 USD
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
    dag_id="s7_pipeline_llm_resumen",
    description="Sesión 7 — Resumen automático de datos con GPT-4o-mini",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["sesion-7", "modulo-4", "llm", "openai", "ia"],
)
def pipeline_llm_resumen():
    """
    ### Pipeline LLM: extrae datos → genera resumen con IA
    Consulta los últimos registros de clima en Postgres,
    construye un prompt y pide a GPT-4o-mini un resumen ejecutivo.

    **Requisito:** `OPENAI_API_KEY` en el archivo `.env`
    **Costo estimado:** ~$0.01 por ejecución con gpt-4o-mini
    """

    @task
    def extraer_datos_recientes() -> str:
        """Extrae los últimos 7 días de datos de clima desde Postgres."""
        hook = PostgresHook(postgres_conn_id="postgres_lab")
        registros = hook.get_records("""
            SELECT fecha, ciudad, temp_max, temp_min, temp_promedio, precipitacion
            FROM clima_historico
            ORDER BY fecha DESC
            LIMIT 7;
        """)
        
        if not registros:
            log.warning("⚠️  No hay datos en clima_historico — ejecuta s4_etl_clima_postgres primero")
            registros = [
                ("2024-01-20", "Guayaquil", 32.1, 24.5, 28.3, 0.0),
                ("2024-01-19", "Guayaquil", 31.8, 23.9, 27.9, 2.3),
                ("2024-01-18", "Guayaquil", 33.0, 25.1, 29.1, 0.0),
            ]
            
        log.info("📊 Datos extraídos: %d registros", len(registros))
        
        # Casteamos los valores numéricos a float para evitar el error de Decimal
        return json.dumps([
            {
                "fecha": str(r[0]), 
                "ciudad": r[1], 
                "temp_max": float(r[2]) if r[2] is not None else 0.0, 
                "temp_min": float(r[3]) if r[3] is not None else 0.0, 
                "temp_promedio": float(r[4]) if r[4] is not None else 0.0, 
                "precipitacion": float(r[5]) if r[5] is not None else 0.0
            }
            for r in registros
        ])

    @task
    def construir_prompt(datos_json: str) -> str:
        """Construye el prompt que se enviará al LLM."""
        datos = json.loads(datos_json)
        tabla = "\n".join([
            f"  {d['fecha']}: max={d['temp_max']}°C, min={d['temp_min']}°C, "
            f"promedio={d['temp_promedio']}°C, lluvia={d['precipitacion']}mm"
            for d in datos
        ])
        prompt = f"""Eres un analista meteorológico. Analiza los siguientes datos climáticos
de {datos[0]['ciudad']} y genera un reporte ejecutivo en español de máximo 3 párrafos.
Incluye: tendencia de temperaturas, días de lluvia, y una recomendación práctica.

DATOS:
{tabla}

REPORTE:"""
        log.info("📝 Prompt construido (%d caracteres)", len(prompt))
        return prompt

    @task
    def llamar_llm(prompt: str) -> dict:
        """Envía el prompt a GPT-4o-mini y retorna el resultado."""
        from openai import OpenAI

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key or api_key.startswith("sk-pon"):
            log.warning("⚠️  OPENAI_API_KEY no configurada — usando respuesta de ejemplo")
            return {
                "resumen": "Ejemplo: Los últimos 7 días mostraron temperaturas estables entre 24°C y 33°C en Guayaquil. No se registraron lluvias significativas. Se recomienda mantener hidratación durante actividades al aire libre.",
                "modelo": "gpt-4o-mini (simulado)",
                "tokens": 0,
            }

        client = OpenAI(api_key=api_key)
        log.info("🤖 Llamando a gpt-4o-mini...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.3,
        )
        resumen = response.choices[0].message.content.strip()
        tokens = response.usage.total_tokens
        log.info("✅ Resumen generado — %d tokens usados (~$%.4f USD)", tokens, tokens * 0.000000150)
        return {"resumen": resumen, "modelo": "gpt-4o-mini", "tokens": tokens}

    @task
    def guardar_y_exportar(resultado: dict, datos_json: str):
        """Guarda el resumen en Postgres y exporta a un archivo txt en include/outputs/."""
        # Guardar en Postgres
        hook = PostgresHook(postgres_conn_id="postgres_lab")
        hook.run("""
            INSERT INTO llm_outputs (fuente, resumen, modelo_llm, tokens_usados)
            VALUES (%s, %s, %s, %s);
        """, parameters=(
            "clima_historico",
            resultado["resumen"],
            resultado["modelo"],
            resultado["tokens"],
        ))

        # Exportar a archivo
        outputs_path = Variable.get("OUTPUTS_PATH", default_var="/usr/local/airflow/include/outputs")
        os.makedirs(outputs_path, exist_ok=True)
        from datetime import datetime
        filename = f"reporte_clima_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        filepath = os.path.join(outputs_path, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"REPORTE CLIMÁTICO AUTOMÁTICO\n{'='*40}\n\n")
            f.write(resultado["resumen"])
            f.write(f"\n\n[Generado por {resultado['modelo']} | {resultado['tokens']} tokens]\n")

        log.info("💾 Reporte guardado en: %s", filepath)

    # ── Pipeline ─────────────────────────────────────────────────────────────
    datos = extraer_datos_recientes()
    prompt = construir_prompt(datos)
    resultado = llamar_llm(prompt)
    guardar_y_exportar(resultado, datos)


pipeline_llm_resumen()
