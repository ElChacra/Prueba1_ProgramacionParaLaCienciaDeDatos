"""
Pipeline 4 — Data Validation: definición del pipeline.

Toma el dataset final y los datos crudos/limpios para
generar el reporte de validación en data/08_reporting/.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    validar_esquema,
    verificar_integridad,
    comparar_antes_despues,
    generar_reporte_validacion,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            # Paso 1 — Validar que el esquema del dataset final es correcto
            node(
                func=validar_esquema,
                inputs="dataset_final",
                outputs="resultado_esquema",
                name="validar_esquema",
            ),
            # Paso 2 — Verificar integridad del dataset final
            node(
                func=verificar_integridad,
                inputs="dataset_final",
                outputs="resultado_integridad",
                name="verificar_integridad",
            ),
            # Paso 3 — Comparar métricas antes y después de la limpieza
            node(
                func=comparar_antes_despues,
                inputs=[
                    "pacientes_raw",
                    "consultas_raw",
                    "examenes_raw",
                    "medicamentos_raw",
                    "pacientes_cleaned",
                    "consultas_cleaned",
                    "examenes_cleaned",
                    "medicamentos_cleaned",
                ],
                outputs="comparacion_antes_despues",
                name="comparar_antes_despues",
            ),
            # Paso 4 — Generar reporte unificado de validación
            node(
                func=generar_reporte_validacion,
                inputs=[
                    "resultado_esquema",
                    "resultado_integridad",
                    "comparacion_antes_despues",
                    "dataset_final",
                ],
                outputs="reporte_validacion",
                name="generar_reporte_validacion",
            ),
        ]
    )
