import sys

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from downloader import downloader

default_args = {
        'owner': 'airflow',
        'start_date': datetime.now(),
        'retries': 0,
        'depends_on_past': True
    }

with DAG(dag_id="youtube-archive-bot", default_args=default_args, schedule_interval=None) as pipeline:
    setup_environment = BashOperator(task_id="set_environment_variables",
                                     bash_command="source ./setup_environment.sh")

    download_videos = PythonOperator(task_id="downloader",
                                     python_callable=downloader,
                                     op_kwargs={"path": Variable.get("url_csv_path")})

    setup_environment >> download_videos