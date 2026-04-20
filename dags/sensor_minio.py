from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum
import boto3
import botocore


@dag(
    dag_id="sensor_minio",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 4, 15, tz="UTC"),
    catchup=False,
)
def pipeline_trampa():

    def conectarse_bd():
        return boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="admin",
            aws_secret_access_key="password123",
            region_name="us-east-1",
        )

    # poke_interval=10 significa que mirará cada 10 segundos
    @task.sensor(poke_interval=10, timeout=3600, mode="poke")
    def vigilar_carpeta():
        s3 = conectarse_bd()
        try:
            # head_object solo mira si el archivo existe, no lo descarga (ahorra memoria)
            s3.head_object(Bucket="zona-aterrizaje", Key="archivo_secreto.csv")
            print("Archivo detectado! Activando el resto del pipeline...")
            return True  # True para avanzar

        except botocore.exceptions.ClientError:
            # Si el archivo no existe, boto3 lanza un error. Lo atrapamos y decimos que espere.
            print("Aún no hay archivo...")
            return False  # False significa: "Vuelve a dormir y pregunta en 10 segundos"

    @task
    def procesar_archivo():
        s3 = conectarse_bd()
        nombre_archivo = "archivo_secreto.csv"

        # Le decimos a MinIO que copie el archivo a la nueva carpeta
        s3.copy_object(
            Bucket="archivos-procesados",
            CopySource={"Bucket": "zona-aterrizaje", "Key": nombre_archivo},
            Key=f"procesado_{nombre_archivo}",
        )

        # Borramos el original de la zona de aterrizaje para limpiar la trampa
        s3.delete_object(Bucket="zona-aterrizaje", Key=nombre_archivo)

        print("Archivo movido y trampa limpiada para la próxima vez!")
        return "Misión Cumplida!"

    lanzar_nasa = TriggerDagRunOperator(
        task_id="despertar_dag_nasa",
        trigger_dag_id="nasa_asteroides",  # Tiene que ser exactamente el dag_id del otro archivo
        reset_dag_run=True,  # Por si el DAG de la NASA ya había corrido hoy, esto lo fuerza a correr de nuevo
    )

    # Orden de ejecución
    vigilar_carpeta() >> procesar_archivo() >> lanzar_nasa


pipeline_trampa()
