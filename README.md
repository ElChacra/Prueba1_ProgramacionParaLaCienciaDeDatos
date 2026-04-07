# Evaluacion Parcial 1 — Guillermo Cerda

[![Powered by Kedro](https://img.shields.io/badge/powered_by-kedro-ffc900?logo=kedro)](https://kedro.org)

Pipeline de procesamiento de datos clinicos hospitalarios construido con **Kedro 1.3.0**. Cubre ingestion, limpieza, transformacion y validacion de 4 tablas relacionales (pacientes, consultas, examenes, medicamentos).

**Asignatura:** SCY1101 — Programacion para la Ciencia de Datos  
**Mencion:** Data Science  
**Autor:** Guillermo Cerda

---

## Resultados del pipeline

Resumen cuantitativo del impacto del flujo de datos end-to-end:

| Tabla | Filas originales | Filas limpias | Nulos reducidos | Duplicados eliminados |
|---|---|---|---|---|
| pacientes | 412 | 381 | 65 | 12 |
| consultas | 824 | 697 | 378 | 24 |
| examenes | 618 | 481 | 113 | 18 |
| medicamentos | 515 | 400 | 147 | 15 |

**Dataset final:** 697 filas × 37 columnas — 1.04% nulos globales — 0 duplicados — 6/6 checks de integridad OK

---

## Requisitos

- Python >= 3.10
- pip o uv

---

## Instalacion rapida

```bash
# 1. Clonar o descomprimir el proyecto
cd evaluacion-prueba-1-guillermocerda

# 2. Crear y activar entorno virtual
python -m venv .venv
.venv\Scripts\activate        # Windows
# source .venv/bin/activate   # Linux/Mac

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Instalar el paquete del proyecto
pip install -e .

# 5. Ejecutar el pipeline completo
kedro run
```

---

## Estructura del proyecto

```
evaluacion-prueba-1-guillermocerda/
|-- conf/
|   |-- base/
|   |   |-- catalog.yml        # Definicion de datasets (rutas y encodings)
|   |   |-- parameters.yml     # Parametros configurables del pipeline
|   |   `-- logging.yml        # Configuracion de logs
|   `-- local/
|       `-- credentials.yml    # Credenciales locales (no commitear)
|-- data/
|   |-- 01_raw/                # CSV originales (INPUT - no modificar)
|   |   |-- pacientes.csv
|   |   |-- consultas.csv
|   |   |-- examenes.csv
|   |   `-- medicamentos.csv
|   |-- 02_intermediate/       # Tablas limpias (output pipeline cleaning)
|   |-- 03_primary/            # Dataset integrado y transformado
|   `-- 08_reporting/          # Reportes de diagnostico y validacion
|-- notebooks/
|   `-- EDA_Dataset_Salud_FIXED.ipynb   # Analisis exploratorio completo
|-- src/
|   `-- evaluacion_prueba_1_guillermocerda/
|       |-- pipelines/
|       |   |-- data_ingestion/    # Pipeline 1: carga y diagnostico (AD 1.1)
|       |   |-- data_cleaning/     # Pipeline 2: limpieza (AD 1.2)
|       |   |-- data_transform/    # Pipeline 3: transformacion y features (AD 1.3)
|       |   `-- data_validation/   # Pipeline 4: validacion de integridad (AD 1.4)
|       |-- pipeline_registry.py   # Registro central de pipelines
|       `-- settings.py
|-- requirements.txt
`-- pyproject.toml
```

---

## Datos de entrada

Los 4 archivos CSV deben estar en `data/01_raw/` con encoding **UTF-8**:

| Archivo | Descripcion | Filas |
|---|---|---|
| `pacientes.csv` | Datos demograficos de pacientes | 412 |
| `consultas.csv` | Registros de consultas medicas | 824 |
| `examenes.csv` | Resultados de examenes clinicos | 618 |
| `medicamentos.csv` | Prescripciones de medicamentos | 515 |

---

## Pipelines

El proyecto tiene 4 pipelines que se ejecutan en secuencia con `kedro run`:

### 1. `ingestion` — Diagnostico inicial (AD 1.1)

Carga los 4 CSV crudos y genera un reporte de calidad de datos con metricas de nulos, duplicados y shape por tabla.

Salida: `data/08_reporting/reporte_diagnostico.csv`

### 2. `cleaning` — Limpieza de datos (AD 1.2)

Aplica sobre cada tabla, con justificacion tecnica en el codigo:

- Eliminacion de filas sin clave primaria (PKs nulas no son recuperables)
- Eliminacion de duplicados por fila y por ID
- Parseo de fechas en formatos mixtos (ISO YYYY-MM-DD y chileno DD/MM/YYYY)
- Normalizacion de strings categoricos (strip + title case)
- Limpieza de columnas numericas con simbolos ($, ~, "Aprox")
- Imputacion de nulos: mediana para numericos (robusta ante outliers), valor fijo para categoricos
- Tratamiento de outliers por metodo IQR (reemplazo con mediana, no eliminacion)
- Eliminacion de registros huerfanos (integridad referencial entre tablas)
- Separacion de columnas compuestas: dosis → dosis_valor + dosis_unidad; valor_referencia → val_ref_min + val_ref_max

Salida: `data/02_intermediate/*_cleaned.csv`

### 3. `transform` — Transformacion y features (AD 1.3)

- Join de las 4 tablas limpias (left joins desde consultas como tabla central)
- Ingenieria de features: edad calculada, grupo etario, costo total, flag resultado fuera de rango, año/mes de consulta
- Normalizacion de columnas numericas (StandardScaler — media=0, std=1)
- Encoding de variables categoricas (LabelEncoder para ordinales, One-Hot para nominales)
- Agregaciones por paciente: gasto total, numero de consultas, costo promedio

Salida: `data/03_primary/dataset_final.csv`

### 4. `validation` — Validacion de integridad (AD 1.4)

- Validacion de esquema: verifica 12 columnas requeridas con sus tipos
- Verificacion de integridad: 6 checks de negocio (sin duplicados, costos positivos, edades validas, nulos en columnas clave < 10%)
- Reporte comparativo antes/despues de la limpieza

Salida: `data/08_reporting/reporte_validacion.csv`, `data/08_reporting/comparacion_antes_despues.csv`

---

## Comandos utiles

```bash
# Ejecutar pipeline completo (recomendado)
kedro run

# Ejecutar un pipeline especifico
kedro run --pipeline ingestion
kedro run --pipeline cleaning
kedro run --pipeline transform
kedro run --pipeline validation

# Reanudar desde un nodo especifico tras un fallo
kedro run --from-nodes "limpiar_consultas"

# Ver todos los datasets del catalogo
kedro catalog list

# Visualizar el pipeline en el navegador
kedro viz

# Abrir Jupyter con acceso al catalogo de Kedro
kedro jupyter notebook
```

---

## Parametros configurables

En `conf/base/parameters.yml` se pueden ajustar sin tocar el codigo fuente:

| Parametro | Valor por defecto | Descripcion |
|---|---|---|
| `cleaning.estrategia_nulos` | `"median"` | Estrategia de imputacion numerica (`median` o `mean`) |
| `cleaning.umbral_outliers_iqr` | `1.5` | Multiplicador IQR para deteccion de outliers |
| `cleaning.columnas_fecha.pacientes` | `["fecha_nacimiento"]` | Columnas de fecha a parsear en pacientes |
| `cleaning.columnas_fecha.consultas` | `["fecha"]` | Columnas de fecha a parsear en consultas |
| `cleaning.columnas_fecha.examenes` | `["fecha_examen"]` | Columnas de fecha a parsear en examenes |
| `cleaning.imputacion_categorica.*` | `"Desconocido"` | Valor de relleno para nulos categoricos |

---

## Control de versiones

Este proyecto usa **Git** para versionar el codigo fuente, notebooks y configuracion.

```bash
# Inicializar repositorio (si no existe)
git init
git add .
git commit -m "feat: pipeline inicial de datos hospitalarios"

# Registrar cambios incrementales
git add src/evaluacion_prueba_1_guillermocerda/pipelines/data_cleaning/nodes.py
git commit -m "fix: corregir parser de fechas con formato mixto"

# Ver historial de cambios
git log --oneline

# Crear rama para experimentar sin romper el pipeline principal
git checkout -b feature/nueva-transformacion
```

> **Nota:** Solo los CSV crudos de `data/01_raw/` se incluyen en el repositorio (son el input necesario para `kedro run`). Los datos generados por el pipeline (`02_intermediate/`, `03_primary/`, `08_reporting/`) estan excluidos via `.gitignore`. Se versiona el codigo fuente (`src/`), la configuracion (`conf/`), los notebooks (`notebooks/`) y los datos de entrada (`data/01_raw/`).

### Por que versionar con Git

- Permite rastrear cada decision tecnica tomada durante el desarrollo
- Facilita revertir cambios si un nodo rompe el pipeline
- Hace el proyecto reproducible por cualquier persona que clone el repositorio
- Es el estandar profesional en proyectos de ciencia de datos

---

## Dependencias

| Paquete | Version |
|---|---|
| kedro | ~=1.3.0 |
| kedro-datasets[pandas,pickle] | >=9.0 |
| pandas | >=3.0,<4.0 |
| numpy | >=2.4,<3.0 |
| scikit-learn | >=1.8 |
| ipython | >=8.10 |
| jupyterlab | >=4.0 |
| notebook | >=7.0 |

---

## Notas tecnicas

**Encoding de los datos:** Los CSV crudos estan en UTF-8. El catalogo los lee y guarda como UTF-8. Antes de guardar cada tabla limpia, el pipeline elimina caracteres de control basura (C0/C1) mediante la funcion `_forzar_utf8()`.

**Windows y UTF-8:** kedro-datasets abre archivos CSV en modo texto con el encoding del sistema (cp1252 en Windows). El catalogo usa `fs_args.open_args_save.encoding: utf-8` en todos los datasets de salida para forzar UTF-8 independiente del sistema operativo.

**Reproducibilidad:** El proyecto esta disenado para que cualquier persona pueda clonar el repositorio, instalar las dependencias con `pip install -r requirements.txt` y ejecutar `kedro run` obteniendo exactamente los mismos resultados. Los parametros configurables en `parameters.yml` permiten ajustar el comportamiento sin modificar el codigo fuente.

---

## Autor

Guillermo Cerda — Programacion para la Ciencia de Datos — SCY1101
