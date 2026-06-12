# 🚀 Curso: Pipelines de Datos con Apache Airflow 2.x + IA

**20 horas · 8 sesiones · 75% práctica**

---

## ⚡ Inicio rápido (5 minutos)

### Paso 1 — Fork del repositorio

1. Ve a este repositorio en GitHub
2. Click en **Fork** (arriba a la derecha)
3. Selecciona tu cuenta personal → **Create fork**

### Paso 2 — Abrir en Codespace

1. En tu fork, click en **Code** → pestaña **Codespaces**
2. Click en **Create codespace on main**
3. Selecciona la máquina **4-core** (obligatorio para que Airflow tenga suficiente RAM)
4. Espera ~3-5 minutos mientras se construye el entorno

### Paso 3 — Levantar el stack

Una vez que el Codespace esté listo, en la terminal ejecuta:

```bash
# Iniciar todos los servicios (Airflow + Postgres + pgAdmin + MLflow)
astro dev start

# Verificar que todos los contenedores corren
docker ps
```

### Paso 4 — Acceder a los servicios

| Servicio | URL en Codespace | Usuario | Contraseña |
|----------|-----------------|---------|------------|
| **Airflow UI** | Puerto 8080 (se abre automáticamente) | `admin` | `admin` |
| **pgAdmin** | Puerto 5050 | `admin@curso.com` | `admin` |
| **MLflow** | Puerto 5001 | — | — |

> 💡 Los puertos se abren en la pestaña **Ports** de VS Code.
> Haz click en el ícono de globo 🌐 para abrir en el navegador.

### Paso 5 — Configurar tu API key de OpenAI (solo para S7-S8)

```bash
# Crea tu archivo .env copiando la plantilla
cp .env.example .env

# Edita el archivo y pega tu API key
code .env
# Cambia: OPENAI_API_KEY=sk-pon-tu-key-aqui
#      por: OPENAI_API_KEY=sk-proj-TU_KEY_REAL

# Reinicia para que Airflow tome la variable
astro dev restart
```

---

## 📁 Estructura del proyecto

```
curso-airflow/
├── dags/                        # ← Aquí están los DAGs del curso
│   ├── s1_mi_primer_dag.py      # Sesión 1: entorno y anatomía de un DAG
│   ├── s2_operadores_branching.py  # Sesión 2: operadores y XComs
│   ├── s4_etl_clima_postgres.py # Sesión 4: ETL real con API y Postgres
│   ├── s6_pipeline_ml_mlflow.py # Sesión 6: ML con scikit-learn + MLflow
│   └── s7_pipeline_llm_resumen.py  # Sesión 7: resumen con GPT-4o-mini
│
├── include/
│   ├── data/        # Archivos CSV de entrada para los labs
│   ├── sql/         # Scripts SQL (se ejecutan al crear la DB)
│   ├── models/      # Modelos .pkl guardados por los pipelines
│   └── outputs/     # Reportes generados por los DAGs
│
├── plugins/                     # Operadores y hooks customizados
├── .devcontainer/               # Configuración del Codespace
├── docker-compose.override.yml  # Postgres-lab + pgAdmin + MLflow
├── airflow_settings.yaml        # Conexiones y variables pre-configuradas
├── requirements.txt             # Dependencias Python
├── .env                         # 🔒 API keys (NO subir a Git)
└── .env.example                 # Plantilla del .env
```

---

## 🗓️ DAGs por sesión

| Sesión | DAG | Módulo | Temas |
|--------|-----|--------|-------|
| S1 | `s1_mi_primer_dag` | M1 | Entorno, primer DAG, logs |
| S2 | `s2_operadores_branching` | M1 | BranchOperator, XComs |
| S3 | *(lab guiado en clase)* | M2 | TaskFlow API, hooks |
| S4 | `s4_etl_clima_postgres` | M2 | ETL real, API, Postgres |
| S5 | *(lab guiado en clase)* | M3 | Sensores, FileSensor |
| S6 | `s6_pipeline_ml_mlflow` | M3 | scikit-learn, MLflow |
| S7 | `s7_pipeline_llm_resumen` | M4 | OpenAI, prompts en DAGs |
| S8 | *(proyecto integrador)* | M4 | Tu propio pipeline |

---

## 🔌 Conexiones disponibles en Airflow

Todas pre-configuradas en `airflow_settings.yaml`:

| conn_id | Tipo | Descripción |
|---------|------|-------------|
| `postgres_lab` | Postgres | Base de datos del curso (pipeline_db) |
| `mlflow_default` | HTTP | MLflow tracking server |
| `open_meteo_api` | HTTP | API de clima (sin API key) |

---

## 🗄️ Base de datos — tablas creadas automáticamente

| Tabla | Usada en | Descripción |
|-------|----------|-------------|
| `ventas` | S4 | Datos de ejemplo |
| `clima_historico` | S4 | Datos del ETL de clima |
| `pipeline_log` | S4 | Auditoría de ejecuciones |
| `ml_metrics` | S6 | Métricas de modelos entrenados |
| `llm_outputs` | S7 | Resúmenes generados por GPT |

---

## 🐛 Solución de problemas frecuentes

### Airflow no arranca / contenedores se reinician
```bash
# Verificar logs
astro dev logs

# Reiniciar limpio
astro dev stop
astro dev start
```

### Error: puerto 8080 ya en uso
```bash
astro config set webserver.port 8081
astro dev restart
```

### Error: No module named 'openai'
```bash
# Reconstruir la imagen con las dependencias
astro dev restart --no-cache
```

### Ver tablas en pgAdmin
1. Abre pgAdmin en el puerto 5050
2. Click derecho en **Servers** → **Register** → **Server**
3. Name: `postgres-lab`
4. Connection: Host=`postgres-lab`, Port=`5432`, DB=`pipeline_db`, User=`curso`, Pass=`curso123`

---

## 📚 Recursos adicionales

- [Documentación oficial de Apache Airflow](https://airflow.apache.org/docs/)
- [Astro CLI docs](https://www.astronomer.io/docs/astro/cli/overview)
- [TaskFlow API tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [OpenAI API docs](https://platform.openai.com/docs)
- [MLflow docs](https://mlflow.org/docs/latest/index.html)
