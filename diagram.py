from diagrams import Cluster, Diagram
from diagrams.onprem.analytics import Airflow, Spark, Hive
from diagrams.onprem.storage import HDFS
from diagrams.custom import Custom  # لو عايز تحط أيقونة للـ CSV مثلاً

with Diagram("Olist Data Engineering Architecture", show=False, direction="TB"):
    
    # Source
    source = Custom("Olist Dataset\n(CSV)", "./csv_icon.png") # أو استخدم مربع عادي
    
    # Orchestration
    orchestrator = Airflow("Airflow\n(Orchestration)")
    
    with Cluster("Hadoop Ecosystem & Spark Processing"):
        # Layers
        bronze = HDFS("Bronze Layer\n(Raw CSV)")
        spark_clean = Spark("PySpark\n(Data Cleaning)")
        
        silver = HDFS("Silver Layer\n(Cleaned Parquet)")
        spark_model = Spark("PySpark\n(Star Schema)")
        
        gold = HDFS("Gold Layer\n(Fact & Dimensions)")
        
        warehouse = Hive("Apache Hive\n(External Tables)")

    # Data Flow
    source >> orchestrator >> bronze >> spark_clean >> silver >> spark_model >> gold >> warehouse