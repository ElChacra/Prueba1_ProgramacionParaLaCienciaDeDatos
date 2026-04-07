"""
Pipeline Registry — Registro central de todos los pipelines.

Kedro usa este archivo para saber qué pipelines existen y cómo
ejecutarlos. El pipeline '__default__' es el que corre con 'kedro run'.
"""

from kedro.pipeline import Pipeline

from evaluacion_prueba_1_guillermocerda.pipelines import (
    data_ingestion,
    data_cleaning,
    data_transform,
    data_validation,
)


def register_pipelines() -> dict[str, Pipeline]:
    """
    Registra todos los pipelines del proyecto.

    Pipelines disponibles:
    - ingestion  : carga y diagnóstico inicial (AD 1.1)
    - cleaning   : limpieza de datos (AD 1.2)
    - transform  : transformación avanzada (AD 1.3)
    - validation : validación e integridad (AD 1.4)
    - __default__: ejecuta el flujo completo end-to-end
    """
    ingestion  = data_ingestion.create_pipeline()
    cleaning   = data_cleaning.create_pipeline()
    transform  = data_transform.create_pipeline()
    validation = data_validation.create_pipeline()

    return {
        "ingestion":   ingestion,
        "cleaning":    cleaning,
        "transform":   transform,
        "validation":  validation,
        "__default__": ingestion + cleaning + transform + validation,
    }
