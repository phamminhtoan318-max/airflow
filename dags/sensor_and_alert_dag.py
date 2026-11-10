from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.common.sql.sensors.sql import SqlSensor

# 1. Hàm tự động bắn cảnh báo khi có Task bị lỗi
def gui_canh_bao_that_bai(context):
    """
    Hàm này tự động nhận 'context' chứa toàn bộ thông tin về lỗi:
    tên DAG, tên Task bị fail, thời gian, và link xem log!
    """
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    error_msg = context.get('exception')
    log_url = context.get('task_instance').log_url

    print("==================================================")
    print("🚨 CẢNH BÁO BÁO ĐỘNG ĐỎ: PIPELINE GẶP SỰ CỐ!")
    print(f"👉 DAG bị lỗi: {dag_id}")
    print(f"👉 Task bị lỗi: {task_id}")
    print(f"👉 Chi tiết lỗi: {error_msg}")
    print(f"👉 Đường dẫn kiểm tra log: {log_url}")
    print("==================================================")
    # Trong thực tế, bạn sẽ thêm mã gửi tin nhắn Telegram/Slack/Email ở đây!

default_args = {
    'owner': 'data_engineer',
    'retries': 0,                            # Tắt retry để test callback ngay lập tức
    'on_failure_callback': gui_canh_bao_that_bai,  # Gắn callback báo động
}

@dag(
    dag_id='sensor_and_alert_pipeline',
    default_args=default_args,
    description='Pipeline su dung SqlSensor cho du lieu va co che bao dong Alerting',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['sensor', 'alerting', 'level_5'],
)
def sensor_alert_pipeline():

    # 2. SqlSensor: Chờ cho bảng crypto_prices có ít nhất 1 bản ghi
    cho_du_lieu_crypto = SqlSensor(
        task_id='cho_du_lieu_crypto',
        conn_id='postgres_default',
        sql="SELECT COUNT(*) FROM crypto_prices;",
        # Điều kiện: số dòng > 0 thì sensor coi như THÀNH CÔNG
        success=lambda count: count > 0,
        poke_interval=10,         # Cứ mỗi 10 giây kiểm tra lại 1 lần
        timeout=60,               # Tối đa chờ 60 giây, nếu không có sẽ fail
        mode='reschedule',        # Nhường tài nguyên trong lúc chờ
    )

    @task()
    def xu_ly_du_lieu_da_co():
        print("✅ Sensor phát hiện đã có dữ liệu trong Postgres! Bắt đầu xử lý...")

    @task()
    def task_co_tinh_gay_loi():
        """Task này cố tình ném ra Exception để kích hoạt hàm on_failure_callback"""
        print("Đang chuẩn bị kết nối hệ thống bên ngoài...")
        raise ConnectionError("Không thể kết nối đến máy chủ bên thứ ba (Mô phỏng lỗi)!")

    # Thiết lập luồng chạy:
    # Sensor xong -> Xử lý dữ liệu -> Gặp task lỗi (để kích hoạt chuông báo động)
    cho_du_lieu_crypto >> xu_ly_du_lieu_da_co() >> task_co_tinh_gay_loi()

sensor_alert_pipeline()