"""
Pipeline 1 — Data Ingestion
Corresponde a AD 1.1: Estructuras de datos.

Responsabilidad: cargar los 4 CSV crudos y generar un reporte
de diagnóstico inicial (shape, nulos, duplicados, tipos).
"""

import pandas as pd
import logging

log = logging.getLogger(__name__)


def explorar_dataset(df: pd.DataFrame, nombre: str) -> dict:
    """
    Genera un resumen diagnóstico de un DataFrame.
    Retorna un diccionario con métricas básicas de calidad.
    """
    nulos = df.isnull().sum().sum()
    duplicados = df.duplicated().sum()

    resumen = {
        "tabla": nombre,
        "filas": df.shape[0],
        "columnas": df.shape[1],
        "nulos_totales": int(nulos),
        "pct_nulos": round(nulos / (df.shape[0] * df.shape[1]) * 100, 2),
        "duplicados": int(duplicados),
        "pct_duplicados": round(duplicados / df.shape[0] * 100, 2),
    }

    log.info("Diagnóstico [%s]: %d filas, %d nulos, %d duplicados",
             nombre, resumen["filas"], resumen["nulos_totales"], resumen["duplicados"])

    return resumen


def generar_reporte_diagnostico(
    pacientes: pd.DataFrame,
    consultas: pd.DataFrame,
    examenes: pd.DataFrame,
    medicamentos: pd.DataFrame,
) -> pd.DataFrame:
    """
    Nodo principal de ingestion.
    Recibe los 4 DataFrames crudos y produce un reporte
    de diagnóstico consolidado en data/08_reporting/.
    """
    tablas = {
        "pacientes": pacientes,
        "consultas": consultas,
        "examenes": examenes,
        "medicamentos": medicamentos,
    }

    filas_reporte = []
    for nombre, df in tablas.items():
        resumen = explorar_dataset(df, nombre)
        filas_reporte.append(resumen)

    reporte = pd.DataFrame(filas_reporte)

    log.info("Reporte de diagnóstico generado con %d tablas.", len(reporte))
    return reporte
