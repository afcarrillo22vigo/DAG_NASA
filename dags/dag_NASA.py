import json
from airflow.sdk import dag, task
import pendulum
import requests
import os
import logging
import boto3
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from io import BytesIO
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score


@dag(
    dag_id="nasa_asteroides",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 15, tz="UTC"),
    catchup=False,
)
def pipeline_nasa():

    def conectarse_bd():
        s3_client = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",  # El nombre del contenedor en Docker
            aws_access_key_id="admin",  # Usuario escrito en el docker-compose
            aws_secret_access_key="password123",  # Contraseña escrita en el docker-compose
            region_name="us-east-1",  # Región por defecto
        )
        return s3_client

    @task
    def aplicar_machine_learning(csv_asteroides):
        s3 = conectarse_bd()
        nombre_archivo = csv_asteroides.get("archivo")

        # 1. Descargamos los datos limpios de la Capa Plata
        objeto = s3.get_object(Bucket="capa-plata", Key=nombre_archivo)
        df = pd.read_csv(objeto["Body"])

        # 2. Preparamos los datos para la IA
        # X (Pistas): Tamaño y velocidad
        x = df[["diametro_max_km", "velocidad_kmh"]]

        # y (Respuesta): Peligroso o no (convertimos True/False a 1 y 0 para que la máquina lo entienda)
        y = df["peligroso"].astype(int)

        # 3. Creamos el Cerebro Artificial (Un Bosque Aleatorio)
        modelo = RandomForestClassifier(random_state=42)

        # 4. ENTRENAMIENTO: La máquina estudia las pistas y las respuestas
        modelo.fit(x, y)

        # 5. EXAMEN: Le pedimos que prediga el peligro usando solo las pistas
        df["prediccion_IA"] = modelo.predict(x)

        # Calculamos la nota del examen (Precisión)
        nota = accuracy_score(y, df["prediccion_IA"])
        logging.info(f"IA Entrenada. Precisión del modelo: {nota * 100}%")

        # 6. Guardamos los datos con la opinión de la IA en Postgres
        motor = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
        df.to_sql("predicciones_asteroides", motor, if_exists="replace", index=False)

        return "Machine Learning finalizado"

    @task
    def extraer_bronce():
        secret_key = os.getenv("NASA_API_KEY")
        data = date.today()
        data_string = data.strftime("%Y-%m-%d")
        print(data)
        url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={data_string}&end_date={data_string}&api_key={secret_key}"

        respuesta = requests.get(url)
        datos = respuesta.json()
        nombre_archivo = f"nasa_crudo_{data_string}.json"

        s3 = conectarse_bd()
        s3.put_object(Bucket="capa-bronce", Key=nombre_archivo, Body=json.dumps(datos))

        total_asteroides = datos.get("element_count", 0)

        logging.info("Conexion CORRECTA!")
        logging.info(
            f"Hoy ha habido un total de {total_asteroides} asteroides pasando cerca de la tierra :)"
        )

        return {"archivo": nombre_archivo, "fecha": data_string}

    @task
    def transformar_plata(asteroides):
        nombre_archivo = asteroides["archivo"]
        fecha = asteroides["fecha"]

        s3 = conectarse_bd()
        objeto = s3.get_object(Bucket="capa-bronce", Key=nombre_archivo)
        datos_json = json.loads(objeto["Body"].read().decode("utf-8"))

        lista_asteroides = datos_json["near_earth_objects"][fecha]
        asteroides_limpios = []

        for ast in lista_asteroides:
            datos = {
                "id-asteroide": ast["id"],
                "nombre": ast["name"],
                "peligroso": ast["is_potentially_hazardous_asteroid"],
                "diametro_max_km": ast["estimated_diameter"]["kilometers"][
                    "estimated_diameter_max"
                ],
                "velocidad_kmh": ast["close_approach_data"][0]["relative_velocity"][
                    "kilometers_per_hour"
                ],
            }
            asteroides_limpios.append(datos)

        df = pd.DataFrame(asteroides_limpios)

        nombre_csv = f"asteroides_plata_{fecha}.csv"
        csv = df.to_csv(index=False)
        s3.put_object(Bucket="capa-plata", Key=nombre_csv, Body=csv)

        logging.info(f"Capa plata completada! Transformados {len(df)} asteroides.")
        return {"archivo": nombre_csv, "fecha": fecha}

    @task
    def cargar_oro(csv_asteroides):
        s3 = conectarse_bd()
        nombre_archivo = csv_asteroides.get("archivo")
        fecha = csv_asteroides.get("fecha")

        # 1. Descargar de plata
        objeto = s3.get_object(Bucket="capa-plata", Key=nombre_archivo)
        df = pd.read_csv(objeto["Body"])

        # 2. Transformación: Top 3 más grandes
        df_top3 = df.sort_values(by="diametro_max_km", ascending=False).head(3)
        df_top3_clean = df_top3[["nombre", "diametro_max_km", "velocidad_kmh"]]

        # 3. Subir a Oro en MinIO (Convertido a CSV)
        nombre_archivo = f"top_3_asteroides_{fecha}.csv"
        csv_oro = df_top3_clean.to_csv(index=False)
        s3.put_object(Bucket="capa-oro", Key=nombre_archivo, Body=csv_oro)

        # 4. Inyectar en Postgres (Sin espacios en el nombre)
        motor = create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
        df_top3_clean.to_sql(
            "top_3_asteroides", motor, if_exists="replace", index=False
        )
        df_lista_peligrosos = df[["nombre", "peligroso"]].copy()
        df_lista_peligrosos.to_sql(
            "Lista_Asteroides_Peligrosos", motor, if_exists="replace", index=False
        )

        logging.info("Capa Oro y Data Warehouse actualizados con el Top 3!")

    asteroides = extraer_bronce()
    csv_asteroides = transformar_plata(asteroides)
    cargar_oro(csv_asteroides)
    aplicar_machine_learning(csv_asteroides)


pipeline_nasa()
