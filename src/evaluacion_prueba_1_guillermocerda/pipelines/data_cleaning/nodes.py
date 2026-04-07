"""
Pipeline 2 — Data Cleaning
Corresponde a AD 1.2: Limpieza de datos.

Cada función es un nodo independiente con justificación técnica
detallada en los comentarios, tal como exige la rúbrica (Ind. 4 y 9).
"""

import pandas as pd
import numpy as np
import logging
import re

def _forzar_utf8(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia caracteres no-UTF8 y de control basura de todas las columnas string."""
    for col in df.select_dtypes(include='object').columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.encode('utf-8', errors='ignore')
            .str.decode('utf-8')
            # Elimina C0 (exc. \t\n\r), DEL y C1 (\x80-\x9f) — bytes basura de encodings mal leídos
            .str.replace(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', regex=True)
        )
    return df

log = logging.getLogger(__name__)


# ── UTILIDADES ───────────────────────────────────────────────────────────────

def _parsear_fecha(serie: pd.Series) -> pd.Series:
    """
    Parser de dos pasos para fechas con formatos mixtos.

    JUSTIFICACIÓN: El EDA detectó dos formatos en el mismo campo:
    - YYYY-MM-DD (ISO, ej: 1940-03-05)
    - DD/MM/YYYY (ej: 01/01/1940)
    Usar dayfirst=True de forma global falla porque interpreta
    '1940-03-05' como día=1940, generando NaT masivo (~50% del campo).
    La solución es intentar cada formato de forma explícita.
    """
    # Paso 1: intentar formato ISO estándar
    resultado = pd.to_datetime(serie, format="%Y-%m-%d", errors="coerce")

    # Paso 2: donde falló ISO, intentar formato chileno DD/MM/YYYY
    mask = resultado.isna() & serie.notna()
    resultado[mask] = pd.to_datetime(
        serie[mask], format="%d/%m/%Y", errors="coerce"
    )

    n_invalidas = resultado.isna().sum() - serie.isna().sum()
    if n_invalidas > 0:
        log.warning("Fechas no parseables tras doble intento: %d", max(n_invalidas, 0))

    return resultado


def _limpiar_valor_numerico(serie: pd.Series) -> pd.Series:
    """
    Extrae el número de strings que contienen símbolos como $, ~ o texto.

    JUSTIFICACIÓN: El EDA encontró valores como '$433333.0', '216901.0 Aprox'
    y '~33556.0'. pd.to_numeric() los rechaza directamente.
    Se extraen con regex antes de convertir, para no perder datos válidos.
    """
    serie_limpia = (
        serie.astype(str)
        .str.replace(r"[$~]", "", regex=True)        # eliminar $ y ~
        .str.replace(r"(?i)\s*aprox", "", regex=True) # eliminar "aprox"
        .str.strip()
    )
    return pd.to_numeric(serie_limpia, errors="coerce")


# ── NODO 1: ELIMINAR FILAS SIN CLAVE PRIMARIA ───────────────────────────────

def eliminar_pks_nulas(df: pd.DataFrame, col_pk: str) -> pd.DataFrame:
    """
    Elimina filas donde la clave primaria es nula.

    JUSTIFICACIÓN: Una fila sin PK no es recuperable ni relacionable
    con otras tablas. No tiene sentido imputar un ID — es un registro
    inválido que debe eliminarse para mantener integridad referencial.
    """
    n_antes = len(df)
    df = df.dropna(subset=[col_pk]).copy()
    n_eliminadas = n_antes - len(df)

    log.info("PKs nulas eliminadas en '%s': %d filas", col_pk, n_eliminadas)
    return df


# ── NODO 2: ELIMINAR DUPLICADOS ──────────────────────────────────────────────

def eliminar_duplicados(df: pd.DataFrame, col_id: str) -> pd.DataFrame:
    """
    Elimina filas duplicadas, priorizando eliminar por ID duplicado.

    JUSTIFICACIÓN: El EDA detectó ~2.91% de duplicados en todas las tablas.
    Se ordenan por la columna ID para que keep='first' conserve
    el registro con ID más bajo (más antiguo/original).
    Primero se eliminan duplicados exactos de fila, luego por ID.
    """
    n_antes = len(df)

    # Paso 1: duplicados de fila completa
    df = df.drop_duplicates().copy()

    # Paso 2: duplicados por ID (conservar primera ocurrencia)
    if col_id in df.columns:
        df = df.drop_duplicates(subset=[col_id], keep="first").copy()

    log.info("Duplicados eliminados: %d filas", n_antes - len(df))
    return df


# ── NODO 3: PARSEAR FECHAS ───────────────────────────────────────────────────

def parsear_fechas(df: pd.DataFrame, columnas_fecha: list) -> pd.DataFrame:
    """
    Convierte columnas de fecha de string a datetime usando parser de dos pasos.

    JUSTIFICACIÓN: Ver función _parsear_fecha(). Se aplica columna por columna
    para poder tener control y log individual de cada una.
    """
    df = df.copy()
    for col in columnas_fecha:
        if col in df.columns:
            df[col] = _parsear_fecha(df[col])
            log.info("Fecha parseada: '%s' — NaT resultantes: %d",
                     col, df[col].isna().sum())
    return df


# ── NODO 4: NORMALIZAR STRINGS CATEGÓRICOS ───────────────────────────────────

def normalizar_strings(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    """
    Aplica strip() y title() a columnas categóricas con case inconsistente.

    JUSTIFICACIÓN: El EDA detectó hasta 24 variantes del mismo valor en
    'prevision' y 32 en 'especialidad' (PEDIATRÍA, pediatría, Pediatría...).
    strip() elimina espacios al inicio/fin; title() estandariza a Title Case.
    Esto reduce las variantes a su valor canónico único.

    EXCEPCIONES NO APLICADAS en este nodo:
    - 'tipo_examen': contiene siglas médicas (TAC, PCR) → se trata aparte
    - 'unidad': contiene unidades con formato médico (mg/dL, mmol/L)
    - 'frecuencia': contiene 'SOS' que no debe convertirse a 'Sos'
    """
    df = df.copy()
    for col in columnas:
        if col in df.columns:
            df[col] = df[col].str.strip().str.title()
            log.info("String normalizado: '%s'", col)
    return df


def normalizar_tipo_examen(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza tipo_examen preservando siglas médicas en mayúsculas.

    JUSTIFICACIÓN: str.title() convertiría 'TAC' → 'Tac' y 'PCR' → 'Pcr',
    lo que es incorrecto en contexto médico. Se usa un mapeo explícito
    para los valores conocidos, y title() solo para los demás.
    """
    if "tipo_examen" not in df.columns:
        return df

    df = df.copy()

    # Siglas que deben mantenerse en mayúsculas
    siglas = {"TAC", "PCR", "ECG"}

    def normalizar_valor(val):
        if pd.isna(val):
            return val
        limpio = str(val).strip()
        if limpio.upper() in siglas:
            return limpio.upper()
        return limpio.title()

    df["tipo_examen"] = df["tipo_examen"].apply(normalizar_valor)
    return df


def normalizar_unidad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza la columna 'unidad' solo con strip(), sin title().

    JUSTIFICACIÓN: Las unidades médicas tienen formato específico:
    mg/dL, mmol/L, U/L. Aplicar title() las destruiría (Mg/Dl, Mmol/L).
    Solo se eliminan espacios al inicio/fin.
    """
    if "unidad" not in df.columns:
        return df
    df = df.copy()
    df["unidad"] = df["unidad"].str.strip()
    return df


# ── NODO 5: LIMPIAR COLUMNAS NUMÉRICAS ──────────────────────────────────────

def limpiar_numericos(df: pd.DataFrame, columnas: list) -> pd.DataFrame:
    """
    Convierte columnas numéricas que llegaron como string con símbolos.

    JUSTIFICACIÓN: El EDA detectó valores como '$433333.0', '~33556.0'
    y '216901.0 Aprox' en columnas de costo. pd.to_numeric() los rechaza.
    Se limpian los símbolos con _limpiar_valor_numerico() antes de convertir.
    """
    df = df.copy()
    for col in columnas:
        if col in df.columns:
            df[col] = _limpiar_valor_numerico(df[col])
            log.info("Numérico limpiado: '%s' — NaN resultantes: %d",
                     col, df[col].isna().sum())
    return df


# ── NODO 6: IMPUTAR NULOS ────────────────────────────────────────────────────

def imputar_nulos_numericos(df: pd.DataFrame, columnas: list,
                             estrategia: str = "median") -> pd.DataFrame:
    """
    Imputa valores nulos en columnas numéricas con mediana o media.

    JUSTIFICACIÓN: Se prefiere la mediana sobre la media porque las columnas
    de costo tienen distribución sesgada (media=$505K vs mediana=$272K,
    detectado en EDA). La mediana es más robusta ante outliers extremos.
    No se eliminan las filas porque perderíamos registros válidos en
    otras columnas del mismo registro.
    """
    df = df.copy()
    for col in columnas:
        if col not in df.columns:
            continue
        n_nulos = df[col].isna().sum()
        if n_nulos == 0:
            continue
        if estrategia == "median":
            valor = df[col].median()
        else:
            valor = df[col].mean()
        df[col] = df[col].fillna(valor)
        log.info("Imputados %d nulos en '%s' con %s=%.2f",
                 n_nulos, col, estrategia, valor)
    return df


def imputar_nulos_categoricos(df: pd.DataFrame,
                               imputaciones: dict) -> pd.DataFrame:
    """
    Imputa valores nulos en columnas categóricas con un valor fijo.

    JUSTIFICACIÓN: Para columnas como 'genero' o 'especialidad' no tiene
    sentido imputar con la moda (asignaría el género más frecuente a un
    paciente real). Se usa 'Desconocido' o 'Sin diagnóstico' para ser
    transparente sobre la ausencia del dato sin eliminar el registro.
    """
    df = df.copy()
    for col, valor in imputaciones.items():
        if col not in df.columns:
            continue
        n_nulos = df[col].isna().sum()
        if n_nulos > 0:
            df[col] = df[col].fillna(valor)
            log.info("Imputados %d nulos en '%s' con '%s'",
                     n_nulos, col, valor)
    return df


# ── NODO 7: TRATAR OUTLIERS ──────────────────────────────────────────────────

def tratar_outliers_iqr(df: pd.DataFrame, columnas: list,
                         multiplicador: float = 1.5) -> pd.DataFrame:
    """
    Reemplaza outliers con la mediana usando el método IQR.

    JUSTIFICACIÓN: Se prefiere IQR sobre Z-score porque las columnas de
    costo y resultado tienen distribuciones asimétricas donde Z-score
    puede pasar por alto valores extremos. El método IQR es más robusto.
    Se reemplaza con la mediana (no se elimina la fila) para conservar
    el resto de información del registro.

    Aplica a: 'costo' en consultas (max=$45M detectado en EDA),
    'resultado' en examenes (valores negativos y >21000 detectados).
    """
    df = df.copy()
    for col in columnas:
        if col not in df.columns:
            continue
        serie = df[col].dropna()
        Q1 = serie.quantile(0.25)
        Q3 = serie.quantile(0.75)
        IQR = Q3 - Q1
        lim_inf = Q1 - multiplicador * IQR
        lim_sup = Q3 + multiplicador * IQR
        mediana = serie.median()

        mask_outlier = (df[col] < lim_inf) | (df[col] > lim_sup)
        n_outliers = mask_outlier.sum()
        df.loc[mask_outlier, col] = mediana

        log.info("Outliers tratados en '%s': %d valores → reemplazados con mediana=%.2f",
                 col, n_outliers, mediana)
    return df


# ── NODO 8: ELIMINAR HUÉRFANOS (integridad referencial) ─────────────────────

def eliminar_huerfanos(df_hijo: pd.DataFrame, col_fk: str,
                        df_padre: pd.DataFrame, col_pk: str,
                        nombre: str) -> pd.DataFrame:
    """
    Elimina registros cuya FK no existe en la tabla padre.

    JUSTIFICACIÓN: El EDA detectó 4-5% de registros huérfanos en cada tabla
    (ej: consultas con id_paciente inexistente en pacientes). Estos registros
    no son analizables en contexto relacional y podrían generar NaN en joins
    posteriores. Se eliminan en lugar de imputar porque un ID inexistente
    no tiene valor médico recuperable.
    """
    pk_vals = pd.to_numeric(df_padre[col_pk].dropna(), errors="coerce").dropna()
    fk_vals = pd.to_numeric(df_hijo[col_fk], errors="coerce")

    n_antes = len(df_hijo)
    df_hijo = df_hijo[fk_vals.isin(pk_vals)].copy()
    n_eliminados = n_antes - len(df_hijo)

    log.info("Huérfanos eliminados en %s.%s: %d filas", nombre, col_fk, n_eliminados)
    return df_hijo


# ── NODO 9: SEPARAR COLUMNAS DERIVADAS ───────────────────────────────────────

def separar_dosis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Separa la columna 'dosis' en 'dosis_valor' (float) y 'dosis_unidad' (str).

    JUSTIFICACIÓN: El EDA detectó que 'dosis' mezcla número y unidad
    en un mismo string ('100mg', '1g', '200mg'). Para análisis cuantitativo
    (comparar dosis entre medicamentos) se necesita la parte numérica separada.
    Se usa regex para extraer número y unidad de forma robusta.
    """
    if "dosis" not in df.columns:
        return df

    df = df.copy()
    extraido = df["dosis"].str.extract(r"([\d]+\.?[\d]*)\s*(mg|g|ml)", flags=re.IGNORECASE)
    df["dosis_valor"] = pd.to_numeric(extraido[0], errors="coerce")
    df["dosis_unidad"] = extraido[1].str.lower()

    log.info("Columna 'dosis' separada en 'dosis_valor' y 'dosis_unidad'")
    return df


def separar_valor_referencia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Separa 'valor_referencia' (ej: '0-100') en 'val_ref_min' y 'val_ref_max'.

    JUSTIFICACIÓN: El EDA detectó que 'valor_referencia' es un rango como
    string ('0-100', '10-200'). Para verificar si un resultado está dentro
    del rango normal se necesitan los límites como números separados.
    """
    if "valor_referencia" not in df.columns:
        return df

    df = df.copy()
    partes = df["valor_referencia"].str.split("-", expand=True)
    df["val_ref_min"] = pd.to_numeric(partes[0], errors="coerce")
    df["val_ref_max"] = pd.to_numeric(partes[1], errors="coerce")

    log.info("Columna 'valor_referencia' separada en 'val_ref_min' y 'val_ref_max'")
    return df


# ── NODOS PRINCIPALES (uno por tabla) ───────────────────────────────────────

def limpiar_pacientes(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """Limpieza completa de la tabla pacientes."""
    df = eliminar_pks_nulas(df, "id_paciente")
    df = eliminar_duplicados(df, "id_paciente")
    df = parsear_fechas(df, params["columnas_fecha"]["pacientes"])
    df = normalizar_strings(df, params["columnas_categoricas"]["pacientes"])
    df = imputar_nulos_categoricos(df, {
        "genero":    params["imputacion_categorica"]["genero"],
        "prevision": params["imputacion_categorica"]["prevision"],
    })
    log.info("Pacientes limpios: %d filas", len(df))
    df = _forzar_utf8(df)
    return df


def limpiar_consultas(df: pd.DataFrame, pacientes: pd.DataFrame,
                       params: dict) -> pd.DataFrame:
    """Limpieza completa de la tabla consultas."""
    df = eliminar_pks_nulas(df, "id_consulta")
    df = eliminar_duplicados(df, "id_consulta")
    df = parsear_fechas(df, params["columnas_fecha"]["consultas"])
    df = normalizar_strings(df, params["columnas_categoricas"]["consultas"])
    df = limpiar_numericos(df, params["columnas_numericas"]["consultas"])
    df = imputar_nulos_numericos(df, ["costo"], params["estrategia_nulos"])
    df = tratar_outliers_iqr(df, ["costo"], params["umbral_outliers_iqr"])
    df = imputar_nulos_categoricos(df, {
        "especialidad":           params["imputacion_categorica"]["especialidad"],
        "diagnostico_principal":  params["imputacion_categorica"]["diagnostico_principal"],
        "diagnostico_secundario": params["imputacion_categorica"]["diagnostico_secundario"],
    })
    df = eliminar_huerfanos(df, "id_paciente", pacientes, "id_paciente", "consultas")
    log.info("Consultas limpias: %d filas", len(df))
    df = _forzar_utf8(df)
    return df


def limpiar_examenes(df: pd.DataFrame, consultas: pd.DataFrame,
                      params: dict) -> pd.DataFrame:
    """Limpieza completa de la tabla examenes."""
    df = eliminar_pks_nulas(df, "id_examen")
    df = eliminar_duplicados(df, "id_examen")
    df = parsear_fechas(df, params["columnas_fecha"]["examenes"])
    df = normalizar_tipo_examen(df)
    df = normalizar_unidad(df)
    df = normalizar_strings(df, params["columnas_categoricas"]["examenes"])
    df = limpiar_numericos(df, params["columnas_numericas"]["examenes"])
    df = imputar_nulos_numericos(df, ["resultado"], params["estrategia_nulos"])
    df = tratar_outliers_iqr(df, ["resultado"], params["umbral_outliers_iqr"])
    df = separar_valor_referencia(df)
    df = imputar_nulos_categoricos(df, {
        "tipo_examen": params["imputacion_categorica"]["tipo_examen"],
        "laboratorio": params["imputacion_categorica"]["laboratorio"],
    })
    df = eliminar_huerfanos(df, "id_consulta", consultas, "id_consulta", "examenes")
    log.info("Exámenes limpios: %d filas", len(df))
    df = _forzar_utf8(df)
    return df


def limpiar_medicamentos(df: pd.DataFrame, consultas: pd.DataFrame,
                          params: dict) -> pd.DataFrame:
    """Limpieza completa de la tabla medicamentos."""
    df = eliminar_pks_nulas(df, "id_prescripcion")
    df = eliminar_duplicados(df, "id_prescripcion")
    df = normalizar_strings(df, params["columnas_categoricas"]["medicamentos"])
    df = limpiar_numericos(df, params["columnas_numericas"]["medicamentos"])
    df = imputar_nulos_numericos(df,
                                  ["costo_unitario", "duracion_dias"],
                                  params["estrategia_nulos"])
    df = separar_dosis(df)
    df = imputar_nulos_categoricos(df, {
        "medicamento": params["imputacion_categorica"]["medicamento"],
        "frecuencia":  params["imputacion_categorica"]["frecuencia"],
    })
    df = eliminar_huerfanos(df, "id_consulta", consultas, "id_consulta", "medicamentos")
    log.info("Medicamentos limpios: %d filas", len(df))
    df = _forzar_utf8(df)
    return df
