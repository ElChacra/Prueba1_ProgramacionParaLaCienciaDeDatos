"""
Pipeline 1 — Data Ingestion: definición del pipeline.
Conecta los nodos de exploración y el reporte diagnóstico.
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import generar_reporte_diagnostico


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=generar_reporte_diagnostico,
                inputs=[
                    "pacientes_raw",
                    "consultas_raw",
                    "examenes_raw",
                    "medicamentos_raw",
                ],
                outputs="reporte_diagnostico",
                name="generar_reporte_diagnostico",
            ),
        ]
    )
