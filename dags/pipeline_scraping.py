import logging
import boto3
from airflow.sdk import dag, task
import pendulum
import requests
from bs4 import BeautifulSoup
import pandas as pd


@dag(
    dag_id="scraping_libros",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 20, tz="UTC"),
    catchup=False,
    tags=["scraping", "hacker"],
)
def pipeline_scraping():

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
    def extraer_datos_web():
        datos_extraidos = []

        for pagina in range(1, 6):
            url = f"http://books.toscrape.com/catalogue/page-{pagina}.html"
            respuesta = requests.get(url)
            logging.info(f"Obtenido datos de la página {pagina}: {url}")

            # Convertimos el texto de la web en una "Sopa" navegable
            sopa = BeautifulSoup(respuesta.text, "html.parser")

            # Inspeccionando la web, sabemos que cada libro es un <article class="product_pod">
            libros = sopa.find_all("article", class_="product_pod")

            # Bucle para extraer los datos de cada libro
            for libro in libros:
                # El título está escondido en el atributo 'title' de la etiqueta <a> dentro de un <h3>
                titulo = libro.h3.a["title"]
                # El precio está en un párrafo <p> con la clase "price_color"
                precio = libro.find("p", class_="price_color").text

                datos_extraidos.append({"titulo": titulo, "precio": precio})

        # Lo metemos en un DataFrame
        df = pd.DataFrame(datos_extraidos)

        logging.info(f"Extracción completad. Se han extraído {len(df)} libros.")
        logging.info(df.head(20))  # Imprimimos los 5 primeros en el log de Airflow

        logging.info("Scraping finalizado")
        return datos_extraidos

    @task
    def guardar_datos(data):
        s3 = conectarse_bd()
        nombre_archivo = "books_scrapped.csv"

        csv = pd.DataFrame(data).to_csv()
        s3.put_object(Bucket="books-scraped", Key=nombre_archivo, Body=csv)

    # Lanzamos la tarea
    data = extraer_datos_web()
    guardar_datos(data)


pipeline_scraping()
