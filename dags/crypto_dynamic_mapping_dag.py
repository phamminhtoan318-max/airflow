from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
from airflow.models import Variable

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
}

@dag(
    dag_id='crypto_dynamic_mapping_pipeline',
    default_args=default_args,
    description='Pipeline quet gia nhieu dong coin song song bang Dynamic Task Mapping',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=['dynamic_mapping', 'level_4'],
)
def crypto_dynamic_pipeline():

    @task()
    def lay_danh_sach_coin():
        """Lấy danh sách coin từ Airflow Variables (nếu chưa có thì dùng mặc định)"""
        danh_sach = Variable.get(
            "monitored_coins", 
            default_var=["bitcoin", "ethereum", "solana", "binancecoin", "ripple"],
            deserialize_json=True
        )
        print(f"Danh sách các coin cần quét: {danh_sach}")
        return danh_sach

    # Task này sẽ được nhân bản tự động cho từng đồng coin!
    @task()
    def lay_gia_tung_coin(coin_id: str):
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
        response = requests.get(url, timeout=10)
        data = response.json()
        gia = data.get(coin_id, {}).get('usd', 0)
        print(f"[{coin_id.upper()}] Giá hiện tại: ${gia}")
        return {"coin": coin_id, "price": gia}

    @task()
    def tong_hop_bao_cao(tat_ca_ket_qua: list):
        """Nhận kết quả từ TẤT CẢ các task song song gom lại"""
        print("====== BÁO CÁO TỔNG HỢP THỊ TRƯỜNG ======")
        for item in tat_ca_ket_qua:
            print(f"- {item['coin'].capitalize()}: ${item['price']}")
        print(f"Tổng số đồng coin đã quét: {len(tat_ca_ket_qua)}")

    # Thiết lập luồng chạy:
    danh_sach_coin = lay_danh_sach_coin()
    
    # ĐIỂM SÁNG GIÁ NHẤT: Cú pháp .expand() thay cho gọi hàm thông thường ()
    # Airflow sẽ tự sinh ra 5 task chạy song song!
    ket_qua_cac_coin = lay_gia_tung_coin.expand(coin_id=danh_sach_coin)
    
    # Task tổng hợp nhận vào toàn bộ kết quả gom lại
    tong_hop_bao_cao(ket_qua_cac_coin)

crypto_dynamic_pipeline()