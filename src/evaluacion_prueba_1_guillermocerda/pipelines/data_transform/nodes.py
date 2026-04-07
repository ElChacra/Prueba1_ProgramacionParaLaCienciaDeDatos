"""
Pipeline 3 — Data Transformation
Corresponde a AD 1.3: Transformación avanzada de datos.

Responsabilidad: joins entre tablas, ingeniería de features,
normalización, encoding y agregaciones.
"""

import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import StandardScaler, LabelEncoder

log = logging.getLogger(__name__)


# ── NODO 1: INTEGRAR LAS 4 TABLAS (JOIN) ────────────────────────────────────

def integrar_tablas(
    pacientes: pd.DataFrame,
    consultas: pd.DataFrame,
    examenes: pd.DataFrame,
    medicamentos: pd.DataFrame,
) -> pd.DataFrame:
    """
    Realiza los joins entre las 4 tablas limpias.

    JUSTIFICACIÓN: Se usan left joins desde consultas hacia las demás tablas
    porque consultas es la tabla central del modelo (vincula pacientes,
    exámenes y medicamentos). Se usa left join para no perder consultas
    que no tengan examen o medicamento asociado — eso es clínicamente válido.

    Joins realizados:
    - consultas ← pacientes      (por id_paciente)
    - consultas ← examenes       (por id_consulta, agregado como métricas)
    - consultas ← medicamentos   (por id_consulta, agregado como métricas)
    """
    # Normalizar IDs a numérico para que los joins funcionen correctamente
    for df, col in [
        (pacientes, "id_paciente"),
        (consultas, "id_paciente"),
        (consultas, "id_consulta"),
        (examenes, "id_consulta"),
        (medicamentos, "id_consulta"),
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Agregar exámenes por consulta (promedio de resultado, conteo)
    examenes_agg = (
        examenes.groupby("id_consulta")
        .agg(
            resultado_promedio=("resultado", "mean"),
            n_examenes=("id_examen", "count"),
        )
        .reset_index()
    )

    # Agregar medicamentos por consulta (costo total, conteo)
    medicamentos_agg = (
        medicamentos.groupby("id_consulta")
        .agg(
            costo_medicamentos=("costo_unitario", "sum"),
            n_medicamentos=("id_prescripcion", "count"),
        )
        .reset_index()
    )

    # Join principal: consultas + pacientes
    df = consultas.merge(
        pacientes[["id_paciente", "fecha_nacimiento", "genero",
                   "prevision", "comuna"]],
        on="id_paciente",
        how="left",
    )

    # Join con agregados de exámenes
    df = df.merge(examenes_agg, on="id_consulta", how="left")

    # Join con agregados de medicamentos
    df = df.merge(medicamentos_agg, on="id_consulta", how="left")

    # Rellenar NaN en columnas agregadas (consultas sin examen/medicamento)
    df["resultado_promedio"] = df["resultado_promedio"].fillna(0)
    df["n_examenes"] = df["n_examenes"].fillna(0).astype(int)
    df["costo_medicamentos"] = df["costo_medicamentos"].fillna(0)
    df["n_medicamentos"] = df["n_medicamentos"].fillna(0).astype(int)

    log.info("Dataset integrado: %d filas x %d columnas", df.shape[0], df.shape[1])
    return df


# ── NODO 2: INGENIERÍA DE FEATURES ──────────────────────────────────────────

def crear_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Crea nuevas columnas derivadas a partir de los datos existentes.

    JUSTIFICACIÓN: El feature engineering permite extraer información
    implícita que los modelos de ML no pueden descubrir directamente.
    Features creados:
    - edad: calculada desde fecha_nacimiento (más útil que la fecha cruda)
    - grupo_etario: segmentación por rangos de edad clínicamente relevantes
    - costo_total: suma de costo consulta + medicamentos (gasto real del paciente)
    - resultado_fuera_rango: flag si el resultado promedio supera 200 (val_ref_max)
    - año_consulta / mes_consulta: permite análisis de estacionalidad
    """
    df = df.copy()

    # Feature 1: Edad en años desde fecha de nacimiento
    hoy = pd.Timestamp.today()
    if "fecha_nacimiento" in df.columns:
        df["edad"] = (
            (hoy - pd.to_datetime(df["fecha_nacimiento"], errors="coerce"))
            .dt.days / 365.25
        ).round(1)
    else:
        df["edad"] = np.nan

    # Feature 2: Grupo etario (segmentación clínica estándar)
    bins = [0, 18, 35, 60, 80, 120]
    labels = ["Pediátrico", "Joven", "Adulto", "Adulto mayor", "Longevo"]
    df["grupo_etario"] = pd.cut(
        df["edad"], bins=bins, labels=labels, right=False
    ).astype(str)
    df["grupo_etario"] = df["grupo_etario"].replace("nan", "Desconocido")

    # Feature 3: Costo total (consulta + medicamentos)
    costo_consulta = pd.to_numeric(df.get("costo", 0), errors="coerce").fillna(0)
    df["costo_total"] = costo_consulta + df["costo_medicamentos"].fillna(0)

    # Feature 4: Flag resultado fuera de rango normal (>200)
    df["resultado_fuera_rango"] = (
        df["resultado_promedio"].fillna(0) > 200
    ).astype(int)

    # Feature 5: Año y mes de la consulta (para análisis temporal)
    if "fecha" in df.columns:
        fecha_dt = pd.to_datetime(df["fecha"], errors="coerce")
        df["año_consulta"] = fecha_dt.dt.year
        df["mes_consulta"] = fecha_dt.dt.month

    log.info("Features creados: edad, grupo_etario, costo_total, resultado_fuera_rango, año/mes")
    return df


# ── NODO 3: NORMALIZACIÓN DE VARIABLES NUMÉRICAS ────────────────────────────

def normalizar_numericos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza columnas numéricas continuas con StandardScaler (Z-score).

    JUSTIFICACIÓN: Los modelos de ML son sensibles a la escala de las variables.
    'costo' puede estar en cientos de miles mientras 'resultado_promedio' en
    decenas — sin escalar, el costo dominaría cualquier modelo de distancia.
    StandardScaler transforma a media=0 y std=1, igualando la influencia.
    Se crean columnas nuevas con sufijo '_scaled' para conservar los originales.
    """
    df = df.copy()
    columnas_escalar = ["costo", "resultado_promedio", "costo_total",
                        "edad", "costo_medicamentos"]

    scaler = StandardScaler()

    cols_presentes = [c for c in columnas_escalar if c in df.columns
                      and df[c].notna().sum() > 0]

    if cols_presentes:
        valores = df[cols_presentes].fillna(0)
        escalados = scaler.fit_transform(valores)
        for i, col in enumerate(cols_presentes):
            df[col + "_scaled"] = escalados[:, i].round(4)

    log.info("Columnas normalizadas: %s", cols_presentes)
    return df


# ── NODO 4: ENCODING DE VARIABLES CATEGÓRICAS ───────────────────────────────

def codificar_categoricas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Codifica variables categóricas a formato numérico.

    JUSTIFICACIÓN: Los modelos de ML no pueden trabajar con strings.
    Se usa:
    - Label Encoding para 'genero' y 'prevision' (pocas categorías ordinales)
    - One-Hot Encoding para 'especialidad' y 'grupo_etario' (nominales,
      sin orden inherente — evita que el modelo asuma jerarquía numérica)

    Se limita OHE a columnas con <= 15 categorías para evitar alta dimensionalidad.
    """
    df = df.copy()

    # Label Encoding: genero, prevision
    for col in ["genero", "prevision"]:
        if col in df.columns:
            le = LabelEncoder()
            df[col + "_encoded"] = le.fit_transform(
                df[col].astype(str).fillna("Desconocido")
            )
            log.info("Label Encoding aplicado: '%s'", col)

    # One-Hot Encoding: especialidad, grupo_etario
    for col in ["especialidad", "grupo_etario"]:
        if col not in df.columns:
            continue
        n_cats = df[col].nunique()
        if n_cats <= 15:
            dummies = pd.get_dummies(
                df[col].astype(str),
                prefix=col,
                drop_first=False,
            )
            df = pd.concat([df, dummies], axis=1)
            log.info("One-Hot Encoding aplicado: '%s' (%d categorías)", col, n_cats)

    return df


# ── NODO 5: AGREGACIONES ADICIONALES (groupby) ──────────────────────────────

def calcular_agregaciones(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega métricas por paciente usando groupby.

    JUSTIFICACIÓN: Las agregaciones resumen el comportamiento histórico
    de cada paciente, generando features de mayor nivel semántico
    que los modelos pueden aprovechar para predicción.
    - gasto_total_paciente: suma de costo_total por id_paciente
    - n_consultas_paciente: cuántas veces consultó cada paciente
    - costo_promedio_consulta: gasto medio por visita
    """
    df = df.copy()

    if "id_paciente" not in df.columns:
        return df

    agg_paciente = (
        df.groupby("id_paciente")
        .agg(
            gasto_total_paciente=("costo_total", "sum"),
            n_consultas_paciente=("id_consulta", "count"),
            costo_promedio_consulta=("costo_total", "mean"),
        )
        .reset_index()
    )
    agg_paciente["costo_promedio_consulta"] = (
        agg_paciente["costo_promedio_consulta"].round(2)
    )

    df = df.merge(agg_paciente, on="id_paciente", how="left")
    log.info("Agregaciones por paciente añadidas: %d pacientes únicos",
             agg_paciente.shape[0])
    return df
