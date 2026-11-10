from datetime import datetime, timedelta
from airflow.decorators import dag, task

# 1. Cấu hình mặc định áp dụng cho TẤT CẢ các task trong DAG này
default_args = {
    'owner': 'ban',                      # Tên tác giả pipeline
    'retries': 1,                        # Nếu 1 task bị lỗi, Airflow tự thử lại 1 lần
    'retry_delay': timedelta(minutes=1), # Chờ 1 phút trước khi thử lại
}

# 2. @dag: Biến hàm bên dưới thành một DAG của Airflow
@dag(
    dag_id='my_first_dag',               # ID duy nhất của DAG (sẽ hiện trên giao diện Web)
    default_args=default_args,
    description='Pipeline dau tien cua toi',
    start_date=datetime(2026, 1, 1),     # Ngày bắt đầu tính lịch chạy
    schedule=None,                       # None = Chạy bằng tay (Manual), chưa tự động hẹn giờ
    catchup=False,                       # CỰC KỲ QUAN TRỌNG: Tránh việc Airflow tự chạy bù các ngày quá khứ
    tags=['learning'],                   # Gắn tag để dễ lọc trên Web UI
)
def my_first_pipeline():

    # 3. @task: Biến hàm Python thông thường thành 1 Task trong Airflow
    @task()
    def lay_du_lieu():
        print("Đang lấy dữ liệu từ nguồn...")
        du_lieu = {"so_luong": 100, "trang_thai": "tot"}
        return du_lieu                   # Dữ liệu return này tự động được lưu vào XCom

    @task()
    def xu_ly_du_lieu(data: dict):
        print(f"Đang xử lý dữ liệu: {data}")
        ket_qua = data["so_luong"] * 2
        return ket_qua

    @task()
    def luu_du_lieu(ket_qua: int):
        print(f"Lưu kết quả thành công: {ket_qua}")

    # 4. Xác định thứ tự chạy (Dependencies)
    # Vì raw_data là đầu ra của lay_du_lieu và là đầu vào của xu_ly_du_lieu,
    # Airflow sẽ TỰ ĐỘNG hiểu: lay_du_lieu PHẢI CHẠY TRƯỚC xu_ly_du_lieu!
    raw_data = lay_du_lieu()
    processed_data = xu_ly_du_lieu(raw_data)
    luu_du_lieu(processed_data)

# 5. Gọi hàm để Airflow nạp DAG vào hệ thống
my_first_pipeline()