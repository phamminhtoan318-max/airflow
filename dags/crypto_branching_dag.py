from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
}

@dag(
    dag_id='crypto_branching_pipeline',
    default_args=default_args,
    description='Pipeline re nhanh theo gia Bitcoin va hen gio chay',
    start_date=datetime(2026, 1, 1),
    schedule='*/5 * * * *',   # Tự động chạy mỗi 5 phút một lần!
    catchup=False,
    tags=['branching', 'learning'],
)
def crypto_branching_pipeline():

    @task()
    def lay_gia_bitcoin():
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
        response = requests.get(url, timeout=10)
        data = response.json()
        gia_usd = data.get('bitcoin', {}).get('usd', 0)
        print(f"Giá Bitcoin hiện tại: ${gia_usd}")
        return gia_usd

    # @task.branch: Task quyết định rẽ nhánh
    @task.branch()
    def kiem_tra_nguong_gia(gia_usd: float):
        nguong_gia = 75000.0
        if gia_usd < nguong_gia:
            print(f"Giá ${gia_usd} > ngưỡng ${nguong_gia} -> Chọn nhánh cảnh báo!")
            return "canh_bao_gia_cao"       # Trả về task_id của nhánh 1
        else:
            print(f"Giá ${gia_usd} <= ngưỡng ${nguong_gia} -> Chọn nhánh bình thường!")
            return "ghi_nhan_gia_on_dinh"   # Trả về task_id của nhánh 2

    @task(task_id="canh_bao_gia_cao")
    def canh_bao_gia_cao():
        print("🚨 BÁO ĐỘNG: Giá Bitcoin đang vượt mốc $75,000! Gửi thông báo đến team!")

    @task(task_id="ghi_nhan_gia_on_dinh")
    def ghi_nhan_gia_on_dinh():
        print("✅ Giá Bitcoin vẫn nằm trong vùng ổn định. Không cần cảnh báo.")

    # Task tổng kết cuối cùng: Cần TriggerRule để không bị skipped
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def ket_thuc_quy_trinh():
        print("🎉 Hoàn thành chu kỳ kiểm tra giá Bitcoin!")

    # Thiết lập luồng chạy
    gia = lay_gia_bitcoin()
    quyet_dinh = kiem_tra_nguong_gia(gia)

    nhanh_canh_bao = canh_bao_gia_cao()
    nhanh_on_dinh = ghi_nhan_gia_on_dinh()
    tong_ket = ket_thuc_quy_trinh()

    # Nối nhánh: quyet_dinh sẽ chỉ kích hoạt 1 trong 2 nhánh
    quyet_dinh >> [nhanh_canh_bao, nhanh_on_dinh] >> tong_ket

crypto_branching_pipeline()