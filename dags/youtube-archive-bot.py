from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime
from os import environ, path
from process.downloader import downloader
# from process.uploader import uploader
# from process.messenger import messenger

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 22),
    "schedule_interval": "@daily",
    "depends_on_past": True,
    "catchup": True,
    "retries": 0
}

FEED_PATH = environ.get("FEED_PATH")

def check_feed_exists(execution_date: str) -> bool:

    """
    Check if the CSV file for the given execution date exists.

    args:
        execution_date (str): The execution date in 'YYYY-MM-DD' format.

    returns:
        bool: True if the file exists, False otherwise.
    """

    return path.exists(f"{FEED_PATH}{execution_date}.csv")

with DAG(dag_id="youtube-archive-bot", default_args=default_args) as pipeline:
    
    """
    Automates the process of archiving YouTube videos. It reads CSV files containing YouTube video URLs to be added to 
    the repository, downloads the videos to a local directory and stores the corresponding metadata in an SQLite 
    database. It also uploads the newly downloaded videos to Google Drive and sends the metadata as a message to a 
    Discord server.

    This DAG consists of four tasks:
        1. `check_feed`: Verifies if a CSV file is available for the current execution date.
        2. `download_videos`: Downloads YouTube videos based on URLs from the CSV file.
        3. `upload_videos`: Uploads the downloaded videos to Google Drive.
        4. `send_messages`: Sends the records of metadata as messages to a Discord server.

    Runs daily and uses Apache Airflow's templated execution date for file path references.
    """

    check_feed = ShortCircuitOperator(task_id="check_feed_exists",
                                      python_callable=check_feed_exists,
                                      op_kwargs={"execution_date": "{{ ds }}"})

    download_videos = ShortCircuitOperator(task_id="downloader",
                                           python_callable=downloader,
                                           op_kwargs={"path": f"{FEED_PATH}{{{{ ds }}}}.csv"})

    # upload_videos = PythonOperator(task_id="uploader",
    #                                  python_callable=,
    #                                  op_kwargs={})
    #
    # send_messages = PythonOperator(task_id="messenger",
    #                                  python_callable=,
    #                                  op_kwargs={})

    check_feed >> download_videos #>> upload_videos >> send_messages