from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# 1. Định nghĩa Dataset chung đại diện cho nguồn dữ liệu
crypto_dataset = Dataset("postgres://airflow/public/crypto_prices")

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
}

@dag(
    dag_id='dataset_producer_pipeline',
    default_args=default_args,
    description='Producer DAG: Thu thap du lieu va danh dau Dataset',
    start_date=datetime(2026, 1, 1),
    schedule=None,            # Chạy khi có yêu cầu (hoặc hẹn giờ định kỳ)
    catchup=False,
    tags=['datasets', 'level_6_final'],
)
def producer_pipeline():

    # outlets=[crypto_dataset]: Khi task này xong, Airflow tự động phát tín hiệu
    @task(outlets=[crypto_dataset])
    def nap_du_lieu_moi():
        print("Đang nạp dữ liệu thị trường mới vào Database...")
        print("✅ Nạp dữ liệu hoàn tất! Phát tín hiệu cập nhật Dataset...")

    nap_du_lieu_moi()

producer_pipeline()
