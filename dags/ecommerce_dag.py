from datetime import datetime
import warnings
from airflow import DAG
import sys
from airflow.operators.python import PythonOperator
from etl import run_etl

# Importe a função (assuma etl.py no path)
warnings.filterwarnings("ignore", category=SyntaxWarning)

# Importe sua função ETL (ajuste o path se etl.py não estiver no sys.path)

sys.path.append("/app")  # Adicione se etl.py estiver na raiz (montado via build)

with DAG(
    "ecommerce_pipeline",
    default_args={"owner": "rafael"},
    description="ETL E-commerce",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
        op_kwargs={"num_items": 100},
    )
