from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# 1. Trỏ vào đúng Dataset mà Producer cập nhật
crypto_dataset = Dataset("postgres://airflow/public/crypto_prices")

default_args = {
    'owner': 'data_engineer',
}

@dag(
    dag_id='dataset_consumer_pipeline',
    default_args=default_args,
    description='Consumer DAG: Tu dong chay khi Dataset co du lieu moi',
    start_date=datetime(2026, 1, 1),
    # ĐIỂM SÁNG GIÁ: Lắng nghe Dataset, không cần cấu hình cron!
    schedule=[crypto_dataset],
    catchup=False,
    tags=['datasets', 'level_6_final'],
)
def consumer_pipeline():

    @task()
    def tinh_toan_chi_so_bao_cao():
        print("⚡ TÍN HIỆU NHẬN ĐƯỢC: Phát hiện Dataset vừa có dữ liệu mới!")
        print("📊 Đang đọc dữ liệu mới và tính toán báo cáo cho Ban Giám đốc...")
        print("🎉 Xuất báo cáo thành công!")

    tinh_toan_chi_so_bao_cao()

consumer_pipeline()
