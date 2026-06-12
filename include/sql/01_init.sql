-- Script de inicialización de pipeline_db
-- Se ejecuta automáticamente cuando se crea el contenedor postgres-lab
-- ─────────────────────────────────────────────────────────────────────────────

-- S4: tabla para el lab ETL de ventas
CREATE TABLE IF NOT EXISTS ventas (
    id          SERIAL PRIMARY KEY,
    fecha       DATE NOT NULL,
    producto    VARCHAR(100) NOT NULL,
    cantidad    INTEGER NOT NULL,
    precio      NUMERIC(10,2) NOT NULL,
    total       NUMERIC(10,2) GENERATED ALWAYS AS (cantidad * precio) STORED,
    cargado_en  TIMESTAMP DEFAULT NOW()
);

-- S4: tabla para el log de ejecuciones del pipeline
CREATE TABLE IF NOT EXISTS pipeline_log (
    id           SERIAL PRIMARY KEY,
    dag_id       VARCHAR(100),
    run_id       VARCHAR(200),
    tarea        VARCHAR(100),
    status       VARCHAR(50),
    registros    INTEGER DEFAULT 0,
    ejecutado_en TIMESTAMP DEFAULT NOW()
);

-- S5-S6: tabla para almacenar métricas de modelos ML
CREATE TABLE IF NOT EXISTS ml_metrics (
    id           SERIAL PRIMARY KEY,
    modelo       VARCHAR(100),
    version      VARCHAR(50),
    accuracy     NUMERIC(5,4),
    f1_score     NUMERIC(5,4),
    mlflow_run   VARCHAR(200),
    entrenado_en TIMESTAMP DEFAULT NOW()
);

-- S7-S8: tabla para los resúmenes generados por LLM
CREATE TABLE IF NOT EXISTS llm_outputs (
    id           SERIAL PRIMARY KEY,
    fuente       VARCHAR(200),
    resumen      TEXT,
    modelo_llm   VARCHAR(100),
    tokens_usados INTEGER,
    generado_en  TIMESTAMP DEFAULT NOW()
);

-- Datos de ejemplo para los labs de S4
INSERT INTO ventas (fecha, producto, cantidad, precio) VALUES
    ('2024-01-15', 'Laptop', 2, 1200.00),
    ('2024-01-16', 'Monitor', 5, 350.00),
    ('2024-01-17', 'Teclado', 10, 45.00),
    ('2024-01-18', 'Mouse', 15, 25.00),
    ('2024-01-19', 'Laptop', 1, 1200.00)
ON CONFLICT DO NOTHING;
