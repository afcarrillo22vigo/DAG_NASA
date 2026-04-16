# ☄️ NASA Asteroid Tracker: End-to-End Data Pipeline

Este proyecto es un pipeline de datos automatizado que extrae, procesa y visualiza información sobre asteroides cercanos a la Tierra (Near Earth Objects) utilizando la API oficial de la NASA (NeoWs).

El sistema implementa una **Arquitectura Medallón** (Bronce, Plata, Oro) orquestada con Apache Airflow, gestionando el almacenamiento de datos en un Data Lake (MinIO) y sirviendo los datos procesados en un Data Warehouse (PostgreSQL) para su análisis en Metabase.

## 🛠️ Stack Tecnológico
* **Orquestación:** Apache Airflow
* **Data Lake:** MinIO (Compatible con AWS S3)
* **Data Warehouse:** PostgreSQL
* **Procesamiento ETL:** Python (Pandas, Boto3, Requests, SQLAlchemy)
* **Visualización (BI):** Metabase
* **Infraestructura & Seguridad:** Docker, Docker Compose, inyección de variables de entorno (`.env`).

## 🏗️ Arquitectura Medallón del Proyecto

1. 🥉 **Capa Bronce (Raw):** - Extracción de datos crudos desde la API REST de la NASA (`api.nasa.gov`).
   - Autenticación segura mediante API Keys inyectadas por variables de entorno.
   - Almacenamiento del JSON original sin alterar en el bucket `capa-bronce` de MinIO.

2. 🥈 **Capa Plata (Cleaned):** - Lectura del JSON crudo desde el Data Lake.
   - **Extracción manual de diccionarios y listas anidadas** para aplanar la estructura de datos.
   - Selección de métricas clave (nombre, diámetro máximo, velocidad, peligrosidad).
   - Conversión a formato tabular y almacenamiento como CSV en el bucket `capa-plata`.

3. 🥇 **Capa Oro (Business/Serving):** - Filtrado y transformación de negocio (Ej: Top 3 de asteroides más grandes del día).
   - Limpieza final de columnas y almacenamiento de respaldo en el bucket `capa-oro`.
   - Inyección dinámica en la base de datos **PostgreSQL** (`top_3_asteroides`).

4. 📊 **Capa de Visualización:** - Conexión de **Metabase** al Data Warehouse para generar paneles de control automáticos sobre el monitoreo espacial.

## 🚀 Cómo ejecutar el proyecto en local

### 1. Clonar el repositorio
```bash
git clone [https://github.com/tu-usuario/nasa-asteroid-tracker.git](https://github.com/tu-usuario/nasa-asteroid-tracker.git)
cd nasa-asteroid-tracker