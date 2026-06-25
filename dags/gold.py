from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "mathe",
}

with DAG(
    dag_id="silver_to_gold",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:
    
    # 1. Create Folders Task
    create_gold_folders = BashOperator(
        task_id="create_gold_folders",
        bash_command="""
        docker exec namenode hdfs dfs -mkdir -p /ecommerce/gold/dim_date
        docker exec namenode hdfs dfs -mkdir -p /ecommerce/gold/dim_customers
        docker exec namenode hdfs dfs -mkdir -p /ecommerce/gold/dim_products
        docker exec namenode hdfs dfs -mkdir -p /ecommerce/gold/dim_sellers
        docker exec namenode hdfs dfs -mkdir -p /ecommerce/gold/dim_payments
        docker exec namenode hdfs dfs -mkdir -p /ecommerce/gold/dim_status
        docker exec namenode hdfs dfs -mkdir -p /ecommerce/gold/fact_order_items

        docker exec namenode hdfs dfs -chmod -R 777 /ecommerce/gold
        """
    )

    # 2. Dimension Table Tasks
    dim_date = BashOperator(
        task_id="dim_date",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/silver_gold/dim_date.py
        """
    )
    
    dim_customer = BashOperator(
        task_id="dim_customer",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/silver_gold/dim_customers.py
        """
    )

    dim_product = BashOperator(
        task_id="dim_product",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/silver_gold/dim_products.py
        """
    )

    dim_seller = BashOperator(
        task_id="dim_seller",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/silver_gold/dim_sellers.py
        """
    )

    dim_payment_type = BashOperator(
        task_id="dim_payment_type",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/silver_gold/dim_payments.py
        """
    )

    dim_order_status = BashOperator(
        task_id="dim_order_status",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/silver_gold/dim_status.py
        """
    )

    # 3. Fact Table Task
    fact_order_items = BashOperator(
        task_id="fact_order_items",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/silver_gold/fact_order_items.py
        """
    )

    # 4. Set Dependencies (List of dimensions)
    dimensions = [
        dim_date,
        dim_customer,
        dim_product,
        dim_seller,
        dim_payment_type,
        dim_order_status,
    ]

    # Clean and readable dependency chaining
    create_gold_folders >> dimensions >> fact_order_items