from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os

# استخدام الـ DNS الداخلي للدوكر والبورت الافتراضي للـ WebHDFS للـ NameNode
LOCAL_DATA_PATH = "/opt/airflow/data"
NAMENODE = "http://namenode:9870/webhdfs/v1"

default_args = {
    "owner": "mathe",
    "retries": 2,
}

# ---------------------------
# WebHDFS Helpers
# ---------------------------

# 1. ضفنا باراميتر الـ user.name=root في آخر الـ URL الأساسي
# بس خليه يقف عند v1 عشان الـ paths تركب صح
NAMENODE = "http://namenode:9870/webhdfs/v1"
# ---------------------------
# WebHDFS Helpers
# ---------------------------

def create_hdfs_dir(path):
    # بنضيف الـ user.name=root مع الـ operation
    url = f"{NAMENODE}{path}?op=MKDIRS&user.name=root"
    r = requests.put(url)
    r.raise_for_status()
    return r.json()


def upload_file(local_path, hdfs_path):
    """
    WebHDFS 2-step upload:
    1. Request redirect URL
    2. Upload file content
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"الـ CSV ده مش موجود في مسار الآيرفلو: {local_path}")

    # ضفنا برضه الـ user.name=root هنا في الخطوة الأولى
    url = f"{NAMENODE}{hdfs_path}?op=CREATE&overwrite=true&user.name=root"
    r = requests.put(url, allow_redirects=False)

    if "Location" not in r.headers:
        raise Exception(f"No redirect URL returned for {hdfs_path}. Response: {r.text}")

    redirect_url = r.headers["Location"]

    # الخطوة 2 مش محتاجة يوزر لأن الـ Redirect URL بيبقى جواه الـ Token والصلاحية خلاص
    with open(local_path, "rb") as f:
        upload = requests.put(redirect_url, data=f)
        upload.raise_for_status()

# ---------------------------
# DAG Definition
# ---------------------------

with DAG(
    dag_id="bronze_webhdfs_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["hdfs", "webhdfs", "bronze", "ecommerce"],
) as dag:

    # 1. Create directories
    create_dirs = PythonOperator(
        task_id="create_hdfs_dirs",
        python_callable=lambda: [
            create_hdfs_dir("/ecommerce/bronze/customers"),
            create_hdfs_dir("/ecommerce/bronze/orders"),
            create_hdfs_dir("/ecommerce/bronze/order_items"),
            create_hdfs_dir("/ecommerce/bronze/products"),
            create_hdfs_dir("/ecommerce/bronze/payments"),
            create_hdfs_dir("/ecommerce/bronze/reviews"),
            create_hdfs_dir("/ecommerce/bronze/sellers"),
            create_hdfs_dir("/ecommerce/bronze/geolocation"),
        ],
    )

    # ---------------------------
    # 2. Upload tasks (Modified with actual Olist filenames)
    # ---------------------------

    upload_customers = PythonOperator(
        task_id="upload_customers",
        python_callable=lambda: upload_file(
            f"{LOCAL_DATA_PATH}/olist_customers_dataset.csv",
            "/ecommerce/bronze/customers/customers.csv"
        ),
    )

    upload_orders = PythonOperator(
        task_id="upload_orders",
        python_callable=lambda: upload_file(
            f"{LOCAL_DATA_PATH}/olist_orders_dataset.csv",
            "/ecommerce/bronze/orders/orders.csv"
        ),
    )

    upload_order_items = PythonOperator(
        task_id="upload_order_items",
        python_callable=lambda: upload_file(
            f"{LOCAL_DATA_PATH}/olist_order_items_dataset.csv",
            "/ecommerce/bronze/order_items/order_items.csv"
        ),
    )

    upload_products = PythonOperator(
        task_id="upload_products",
        python_callable=lambda: upload_file(
            f"{LOCAL_DATA_PATH}/olist_products_dataset.csv",
            "/ecommerce/bronze/products/products.csv"
        ),
    )

    upload_payments = PythonOperator(
        task_id="upload_payments",
        python_callable=lambda: upload_file(
            f"{LOCAL_DATA_PATH}/olist_order_payments_dataset.csv",
            "/ecommerce/bronze/payments/payments.csv"
        ),
    )

    upload_reviews = PythonOperator(
        task_id="upload_reviews",
        python_callable=lambda: upload_file(
            f"{LOCAL_DATA_PATH}/olist_order_reviews_dataset.csv",
            "/ecommerce/bronze/reviews/reviews.csv"
        ),
    )

    upload_sellers = PythonOperator(
        task_id="upload_sellers",
        python_callable=lambda: upload_file(
            f"{LOCAL_DATA_PATH}/olist_sellers_dataset.csv",
            "/ecommerce/bronze/sellers/sellers.csv"
        ),
    )

    upload_geolocation = PythonOperator(
        task_id="upload_geolocation",
        python_callable=lambda: upload_file(
            f"{LOCAL_DATA_PATH}/olist_geolocation_dataset.csv",
            "/ecommerce/bronze/geolocation/geolocation.csv"
        ),
    )

    # 3. Verification
    def verify():
        # تعديل الـ URL هنا عشان الـ path يجي في مكانة المظبوط
        url = f"{NAMENODE}/ecommerce/bronze?op=LISTSTATUS"
        r = requests.get(url)
        print(r.json())
        return r.json()

    verify_task = PythonOperator(
        task_id="verify_hdfs",
        python_callable=verify
    )

    # Dependencies
    create_dirs >> [
        upload_customers,
        upload_orders,
        upload_order_items,
        upload_products,
        upload_payments,
        upload_reviews,
        upload_sellers,
        upload_geolocation
    ] >> verify_task