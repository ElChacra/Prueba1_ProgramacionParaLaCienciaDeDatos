"""
Pipeline 2 — Data Cleaning: definición del pipeline.

ORDEN IMPORTANTE:
1. Limpiar pacientes primero (tabla padre principal).
2. Limpiar consultas usando pacientes limpios (para eliminar huérfanos).
3. Limpiar examenes usando consultas limpias.
4. Limpiar medicamentos usando consultas limpias.

Los outputs van a data/02_intermediate/ como define el catalog.yml.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    limpiar_pacientes,
    limpiar_consultas,
    limpiar_examenes,
    limpiar_medicamentos,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Paso 1 — Pacientes (sin dependencias)
            node(
                func=limpiar_pacientes,
                inputs=["pacientes_raw", "params:cleaning"],
                outputs="pacientes_cleaned",
                name="limpiar_pacientes",
            ),
            # Paso 2 — Consultas (necesita pacientes limpios para eliminar huérfanos)
            node(
                func=limpiar_consultas,
                inputs=["consultas_raw", "pacientes_cleaned", "params:cleaning"],
                outputs="consultas_cleaned",
                name="limpiar_consultas",
            ),
            # Paso 3 — Exámenes (necesita consultas limpias)
            node(
                func=limpiar_examenes,
                inputs=["examenes_raw", "consultas_cleaned", "params:cleaning"],
                outputs="examenes_cleaned",
                name="limpiar_examenes",
            ),
            # Paso 4 — Medicamentos (necesita consultas limpias)
            node(
                func=limpiar_medicamentos,
                inputs=["medicamentos_raw", "consultas_cleaned", "params:cleaning"],
                outputs="medicamentos_cleaned",
                name="limpiar_medicamentos",
            ),
        ]
    )
