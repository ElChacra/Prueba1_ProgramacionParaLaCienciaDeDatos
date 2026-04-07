"""
Pipeline 3 — Data Transformation: definición del pipeline.

Toma los 4 datasets limpios de 02_intermediate/ y produce
el dataset integrado y transformado en 03_primary/.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    integrar_tablas,
    crear_features,
    normalizar_numericos,
    codificar_categoricas,
    calcular_agregaciones,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Paso 1 — Integrar las 4 tablas en un dataset unificado
            node(
                func=integrar_tablas,
                inputs=[
                    "pacientes_cleaned",
                    "consultas_cleaned",
                    "examenes_cleaned",
                    "medicamentos_cleaned",
                ],
                outputs="dataset_integrado",
                name="integrar_tablas",
            ),
            # Paso 2 — Crear features derivadas
            node(
                func=crear_features,
                inputs="dataset_integrado",
                outputs="dataset_con_features",
                name="crear_features",
            ),
            # Paso 3 — Normalizar columnas numéricas
            node(
                func=normalizar_numericos,
                inputs="dataset_con_features",
                outputs="dataset_normalizado",
                name="normalizar_numericos",
            ),
            # Paso 4 — Codificar variables categóricas
            node(
                func=codificar_categoricas,
                inputs="dataset_normalizado",
                outputs="dataset_encoded",
                name="codificar_categoricas",
            ),
            # Paso 5 — Agregar métricas por paciente
            node(
                func=calcular_agregaciones,
                inputs="dataset_encoded",
                outputs="dataset_final",
                name="calcular_agregaciones",
            ),
        ]
    )
