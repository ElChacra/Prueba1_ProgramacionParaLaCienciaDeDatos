"""
Pipeline 4 — Data Validation
Corresponde a AD 1.4: Flujo profesional de datos.

Responsabilidad: verificar integridad del dataset final,
comparar antes/después de la limpieza y generar reporte
de validación en data/08_reporting/.
"""

import pandas as pd
import numpy as np
import logging

log = logging.getLogger(__name__)


# ── NODO 1: VALIDAR ESQUEMA ──────────────────────────────────────────────────

def validar_esquema(df: pd.DataFrame) -> dict:
    """
    Verifica que el dataset final tenga las columnas y tipos esperados.

    JUSTIFICACIÓN: La validación de esquema detecta si algún paso del
    pipeline eliminó o renombró columnas accidentalmente. Es una auditoría
    automática de la estructura del dataset antes de usarlo en modelos.
    """
    columnas_requeridas = [
        "id_consulta", "id_paciente", "especialidad",
        "costo", "edad", "genero", "prevision",
        "costo_total", "grupo_etario",
        "resultado_promedio", "n_examenes", "n_medicamentos",
    ]

    resultado = {}
    for col in columnas_requeridas:
        existe = col in df.columns
        resultado[col] = {
            "existe": existe,
            "tipo": str(df[col].dtype) if existe else "AUSENTE",
            "nulos": int(df[col].isna().sum()) if existe else -1,
            "pct_nulos": round(df[col].isna().mean() * 100, 2) if existe else -1,
        }
        if not existe:
            log.warning("Columna requerida ausente: '%s'", col)

    n_ok = sum(1 for v in resultado.values() if v["existe"])
    log.info("Validación de esquema: %d/%d columnas presentes", n_ok, len(columnas_requeridas))
    return resultado


# ── NODO 2: VERIFICAR INTEGRIDAD ─────────────────────────────────────────────

def verificar_integridad(df: pd.DataFrame) -> dict:
    """
    Ejecuta checks de integridad sobre el dataset final.

    JUSTIFICACIÓN: Incluso después de limpiar, pueden existir problemas
    residuales. Esta función actúa como auditoría final (sumas de verificación)
    confirmando que el dataset cumple reglas de negocio mínimas antes
    de ser usado en análisis o modelos.
    """
    checks = {}

    # Check 1: sin filas completamente duplicadas
    n_dup = df.duplicated().sum()
    checks["sin_duplicados"] = {
        "ok": n_dup == 0,
        "valor": int(n_dup),
        "descripcion": "Filas completamente duplicadas",
    }

    # Check 2: costo_total no negativo
    if "costo_total" in df.columns:
        n_neg = (df["costo_total"] < 0).sum()
        checks["costo_total_positivo"] = {
            "ok": n_neg == 0,
            "valor": int(n_neg),
            "descripcion": "Registros con costo_total negativo",
        }

    # Check 3: edad en rango válido (0-120)
    if "edad" in df.columns:
        invalidas = ((df["edad"] < 0) | (df["edad"] > 120)).sum()
        checks["edad_valida"] = {
            "ok": invalidas == 0,
            "valor": int(invalidas),
            "descripcion": "Edades fuera de rango [0, 120]",
        }

    # Check 4: nulos en columnas clave < 10%
    for col in ["id_consulta", "id_paciente", "especialidad"]:
        if col in df.columns:
            pct = df[col].isna().mean() * 100
            checks[f"nulos_{col}"] = {
                "ok": pct < 10,
                "valor": round(pct, 2),
                "descripcion": f"% nulos en {col}",
            }

    n_ok = sum(1 for v in checks.values() if v["ok"])
    log.info("Integridad verificada: %d/%d checks pasados", n_ok, len(checks))
    return checks


# ── NODO 3: COMPARAR ANTES / DESPUÉS ─────────────────────────────────────────

def comparar_antes_despues(
    pacientes_raw: pd.DataFrame,
    consultas_raw: pd.DataFrame,
    examenes_raw: pd.DataFrame,
    medicamentos_raw: pd.DataFrame,
    pacientes_clean: pd.DataFrame,
    consultas_clean: pd.DataFrame,
    examenes_clean: pd.DataFrame,
    medicamentos_clean: pd.DataFrame,
) -> pd.DataFrame:
    """
    Genera una tabla comparativa de métricas antes y después de la limpieza.

    JUSTIFICACIÓN: La comparación antes/después es el principal argumento
    técnico para justificar las decisiones de limpieza tomadas (Ind. 9).
    Muestra de forma cuantificable el impacto de cada transformación.
    """
    tablas = {
        "pacientes":    (pacientes_raw,    pacientes_clean),
        "consultas":    (consultas_raw,    consultas_clean),
        "examenes":     (examenes_raw,     examenes_clean),
        "medicamentos": (medicamentos_raw, medicamentos_clean),
    }

    filas = []
    for nombre, (raw, clean) in tablas.items():
        nulos_raw   = raw.isnull().sum().sum()
        nulos_clean = clean.isnull().sum().sum()
        filas.append({
            "tabla":             nombre,
            "filas_antes":       len(raw),
            "filas_despues":     len(clean),
            "filas_eliminadas":  len(raw) - len(clean),
            "pct_eliminadas":    round((len(raw) - len(clean)) / len(raw) * 100, 2),
            "nulos_antes":       int(nulos_raw),
            "nulos_despues":     int(nulos_clean),
            "nulos_reducidos":   int(nulos_raw - nulos_clean),
            "duplicados_antes":  int(raw.duplicated().sum()),
            "duplicados_despues": int(clean.duplicated().sum()),
        })

    comparacion = pd.DataFrame(filas)
    log.info("Comparación antes/después generada para %d tablas.", len(comparacion))
    return comparacion


# ── NODO 4: GENERAR REPORTE FINAL ────────────────────────────────────────────

def generar_reporte_validacion(
    esquema: dict,
    integridad: dict,
    comparacion: pd.DataFrame,
    dataset_final: pd.DataFrame,
) -> pd.DataFrame:
    """
    Consolida todos los resultados de validación en un único reporte CSV.

    JUSTIFICACIÓN: Un reporte unificado permite auditar el pipeline completo
    en un solo archivo. Incluye métricas del dataset final para que el
    profesor pueda verificar la calidad sin ejecutar el código.
    """
    filas = []

    # Sección 1: checks de esquema
    for col, info in esquema.items():
        filas.append({
            "seccion":     "esquema",
            "elemento":    col,
            "estado":      "OK" if info["existe"] else "FALLO",
            "valor":       info["tipo"],
            "detalle":     f"nulos: {info['pct_nulos']}%",
        })

    # Sección 2: checks de integridad
    for check, info in integridad.items():
        filas.append({
            "seccion":  "integridad",
            "elemento": check,
            "estado":   "OK" if info["ok"] else "FALLO",
            "valor":    str(info["valor"]),
            "detalle":  info["descripcion"],
        })

    # Sección 3: resumen del dataset final
    filas.append({
        "seccion":  "dataset_final",
        "elemento": "total_filas",
        "estado":   "INFO",
        "valor":    str(len(dataset_final)),
        "detalle":  "Filas en el dataset listo para modelado",
    })
    filas.append({
        "seccion":  "dataset_final",
        "elemento": "total_columnas",
        "estado":   "INFO",
        "valor":    str(dataset_final.shape[1]),
        "detalle":  "Columnas en el dataset final (incluye features y encoded)",
    })
    filas.append({
        "seccion":  "dataset_final",
        "elemento": "pct_nulos_global",
        "estado":   "INFO",
        "valor":    str(round(dataset_final.isnull().mean().mean() * 100, 2)) + "%",
        "detalle":  "Porcentaje global de nulos en dataset final",
    })

    reporte = pd.DataFrame(filas)
    log.info("Reporte de validación generado: %d checks.", len(reporte))
    return reporte
