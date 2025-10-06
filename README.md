# ETL con PySpark para Análisis de Accidentes de Tráfico en EE.UU.

Este proyecto implementa un pipeline ETL completo usando PySpark para analizar datos de accidentes de tráfico en Estados Unidos de un dataset de Kaggle con 7 millones de registros aproximadamente.

## Descripción

El ETL realiza las siguientes operaciones:

### 1. **Extracción**
- Lee archivos CSV desde el directorio `input/`
- Dataset: [US Accidents (2016 - 2023)](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents)

### 2. **Transformación**
- Conversión de tipos de datos
- Limpieza de valores nulos e inconsistentes
- Creación de columnas derivadas (año, mes, día, hora, período del día)
- Categorización de datos (severidad, períodos temporales)

### 3. **Generación de Métricas**

#### Métricas Temporales:
- Accidentes por año
- Accidentes por mes
- Accidentes por día de la semana

#### Métricas Categóricas:
- Top 10 estados con más accidentes
- Top 10 ciudades con más accidentes
- Top 15 condiciones climáticas
- Distribución por severidad
- Distribución por período del día

### 4. **Carga**
- Guarda resultados en formato Parquet (eficiente y comprimido)
- Salida en el directorio `output/`

## Tecnologías

- **PySpark 4.0.1** - Procesamiento distribuido de datos
- **PyArrow 21.0.0** - Lectura/escritura eficiente de Parquet
- **Pandas 2.3.3** - Análisis de datos
- **Docker** - Contenerización
- **Python 3.13** - Lenguaje base
- **pytest** - Framework de testing

## Uso

### Ejecución con Docker

#### 1. Descargar el dataset

Descarga el dataset de Kaggle y colócalo en `input/`

#### 2. Construir la imagen Docker

```bash
docker build -t etl-spark-accidents:latest .
```
#### 3. Ejecutar el ETL

```bash
docker run --rm \
  -v $(pwd)/input:/app/input:ro \
  -v $(pwd)/output:/app/output \
  etl-spark-accidents:latest \
  /bin/bash -c "python etl_accidents/etl.py --input-dir /app/input --output-dir /app/output"
```

#### 4. Ejecutar tests

```bash
docker run --rm --name etl-tests etl-spark-accidents:latest /bin/bash -c "python -m pytest tests/ -v --tb=short"
```

### Ejecución Local

#### 1. Descargar el dataset

Descarga el dataset de Kaggle y colócalo en `input/`

#### 2. Instalar dependencias

```bash
# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # En Windows: venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

#### 3. Ejecutar el ETL

```bash
python etl.py --input-dir input/ --output-dir output/
```

## Testing

### Ejecutar todos los tests:

```bash
pytest tests/
```

### Ejecutar tests con coverage:

```bash
pytest tests/ --cov=etl_accidents --cov-report=html
```