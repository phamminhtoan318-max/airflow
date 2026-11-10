from datetime import datetime, timedelta
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id='crypto_price_etl',
    default_args=default_args,
    description='Pipeline lay gia Bitcoin tu API va luu vao Postgres',
    start_date=datetime(2026, 1, 1),
    schedule=None,            # Chạy bằng tay
    catchup=False,
    tags=['etl', 'crypto'],
)
def crypto_price_pipeline():

    @task()
    def tao_bang_neu_chua_co():
        """Tạo bảng crypto_prices trong Postgres nếu chưa tồn tại"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id SERIAL PRIMARY KEY,
            dong_tien VARCHAR(20),
            gia_usd NUMERIC(15, 2),
            gia_vnd NUMERIC(18, 2),
            thoi_gian TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        hook.run(sql)
        print("Đã kiểm tra/tạo bảng crypto_prices thành công!")

    @task()
    def lay_gia_tu_api():
        """Gọi API CoinGecko lấy giá Bitcoin mới nhất"""
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd,vnd"
        response = requests.get(url, timeout=10)
        data = response.json()
        print(f"Dữ liệu gốc từ API: {data}")
        return data

    @task()
    def lam_sach_du_lieu(raw_data: dict):
        """Trích xuất giá USD và VND"""
        bitcoin_data = raw_data.get('bitcoin', {})
        gia_usd = bitcoin_data.get('usd', 0)
        gia_vnd = bitcoin_data.get('vnd', 0)
        
        ket_qua = {
            'dong_tien': 'bitcoin',
            'gia_usd': gia_usd,
            'gia_vnd': gia_vnd
        }
        print(f"Dữ liệu sau khi làm sạch: {ket_qua}")
        return ket_qua

    @task()
    def luu_vao_database(data: dict):
        """Lưu bản ghi vào PostgreSQL"""
        hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = """
        INSERT INTO crypto_prices (dong_tien, gia_usd, gia_vnd)
        VALUES (%s, %s, %s);
        """
        hook.run(sql, parameters=(data['dong_tien'], data['gia_usd'], data['gia_vnd']))
        print("Đã chèn dữ liệu vào bảng crypto_prices thành công!")

    # Thiết lập thứ tự chạy:
    # 1. Tạo bảng trước
    # 2. Lấy API -> Làm sạch -> Lưu vào DB
    tao_bang = tao_bang_neu_chua_co()
    raw = lay_gia_tu_api()
    clean = lam_sach_du_lieu(raw)
    luu_db = luu_vao_database(clean)

    # Đảm bảo bảng được tạo trước khi bắt đầu lấy và lưu dữ liệu
    tao_bang >> raw

crypto_price_pipeline()