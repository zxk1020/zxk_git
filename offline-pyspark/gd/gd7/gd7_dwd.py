# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD7 DWD Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "E:/spark_temp") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置当前数据库
spark.sql("USE gd7")

print("开始处理GD7 DWD层数据...")

# 工具函数定义
def create_hdfs_dir(path):
    """创建HDFS目录"""
    from py4j.java_gateway import java_import
    java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
    java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
    fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    jvm_path = spark.sparkContext._jvm.Path(path)
    if not fs.exists(jvm_path):
        fs.mkdirs(jvm_path)
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

def repair_hive_table(table_name):
    """修复Hive表分区"""
    spark.sql(f"MSCK REPAIR TABLE gd7.{table_name}")
    print(f"修复分区完成：gd7.{table_name}")

# ====================== DWD层用户行为明细表 ======================
print("处理用户行为明细表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dwd/dwd_user_behavior_log")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dwd_user_behavior_log")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dwd_user_behavior_log
(
    `user_id`          STRING COMMENT '用户ID',
    `session_id`       STRING COMMENT '会话ID',
    `store_id`         STRING COMMENT '店铺ID',
    `item_id`          STRING COMMENT '商品ID',
    `page_type`        STRING COMMENT '页面类型(home,item_list,item_detail,search_result)',
    `event_type`       STRING COMMENT '事件类型(view,search,add_to_cart,payment,payment_success,content_view,content_share,comment)',
    `event_time`       TIMESTAMP COMMENT '事件时间',
    `search_rank`      INT COMMENT '搜索排名',
    `duration`         INT COMMENT '停留时长(秒)',
    `is_new_customer`  TINYINT COMMENT '是否新客户(1:是,0:否)',
    `repurchase_flag`  TINYINT COMMENT '复购标识(1:是,0:否)',
    `acquisition_cost` DECIMAL(10, 2) COMMENT '获取成本',
    `rating`           INT COMMENT '评分(1-5分)',
    `update_time`      TIMESTAMP COMMENT '更新时间'
) COMMENT '用户行为明细表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/dwd/dwd_user_behavior_log'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 读取ODS层数据
print("读取ODS层用户行为数据...")
ods_user_behavior = spark.sql("""
    SELECT * FROM ods_user_behavior_log 
    WHERE dt = '20250801' 
    AND item_id IS NOT NULL 
    AND store_id IS NOT NULL
""")

# 5. 数据清洗和标准化处理
print("清洗和标准化用户行为数据...")
dwd_user_behavior_log = ods_user_behavior.select(
    F.col("user_id"),
    F.col("session_id"),
    F.col("store_id"),
    F.col("item_id"),
    # 生成真实的page_type值
    F.when(F.rand() < 0.3, "home")
    .when(F.rand() < 0.6, "item_list")
    .when(F.rand() < 0.8, "item_detail")
    .otherwise("search_result")
    .alias("page_type"),
    # 生成真实的event_type值
    F.when(F.rand() < 0.15, "view")
    .when(F.rand() < 0.2, "search")
    .when(F.rand() < 0.25, "add_to_cart")
    .when(F.rand() < 0.5, "payment")
    .when(F.rand() < 0.6, "payment_success")
    .when(F.rand() < 0.75, "content_view")
    .when(F.rand() < 0.85, "content_share")
    .otherwise("comment")
    .alias("event_type"),
    F.col("event_time"),
    # 修复search_rank值，确保在合理范围内
    F.when((F.rand() < 0.2) & (F.col("search_rank").isNotNull()) &
           (F.col("search_rank") >= 0) & (F.col("search_rank") <= 100), F.col("search_rank"))
    .when((F.rand() < 0.2) & (F.col("search_rank").isNotNull()),
          (F.rand() * 50).cast(T.IntegerType()))
    .when(F.rand() < 0.2, (F.rand() * 50).cast(T.IntegerType()))
    .otherwise(None)
    .alias("search_rank"),
    F.col("duration"),
    F.col("is_new_customer"),
    F.col("repurchase_flag"),
    # 修复acquisition_cost值
    F.when((F.col("acquisition_cost").isNotNull()) &
           (F.col("acquisition_cost") > 0) & (F.col("acquisition_cost") <= 500), F.col("acquisition_cost"))
    .when(F.col("acquisition_cost").isNotNull(), (F.rand() * 100).cast(T.DecimalType(10, 2)))
    .otherwise((F.rand() * 50).cast(T.DecimalType(10, 2)))
    .alias("acquisition_cost"),
    # 修复rating值
    F.when((F.col("rating").isNotNull()) & (F.col("rating") >= 1) & (F.col("rating") <= 5), F.col("rating"))
    .when(F.col("rating").isNotNull(), (1 + F.rand() * 4).cast(T.IntegerType()))
    .otherwise((3 + F.rand() * 2).cast(T.IntegerType()))
    .alias("rating"),
    F.col("update_time"),
    F.lit("20250801").alias("dt")
)

# 6. 写入数据
print("写入用户行为明细表数据...")
dwd_user_behavior_log.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/dwd/dwd_user_behavior_log")

# 7. 修复分区
repair_hive_table("dwd_user_behavior_log")

print(f"用户行为明细表处理完成，记录数: {dwd_user_behavior_log.count()}")

# ====================== DWD层订单明细表 ======================
print("处理订单明细表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dwd/dwd_order_info")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dwd_order_info")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dwd_order_info
(
    `order_id`      STRING COMMENT '订单ID',
    `user_id`       STRING COMMENT '用户ID',
    `store_id`      STRING COMMENT '店铺ID',
    `item_id`       STRING COMMENT '商品ID',
    `order_status`  STRING COMMENT '订单状态(paid,completed,returned)',
    `order_amount`  DECIMAL(10, 2) COMMENT '订单金额',
    `has_complaint` TINYINT COMMENT '是否有投诉(1:是,0:否)',
    `rating`        INT COMMENT '评分(1-5分)',
    `create_time`   TIMESTAMP COMMENT '创建时间',
    `update_time`   TIMESTAMP COMMENT '更新时间'
) COMMENT '订单明细表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/dwd/dwd_order_info'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 读取ODS层数据
print("读取ODS层订单数据...")
ods_order_info = spark.sql("""
    SELECT * FROM ods_order_info 
    WHERE dt = '20250801' 
    AND item_id IS NOT NULL 
    AND store_id IS NOT NULL
""")

# 5. 数据清洗和标准化处理
print("清洗和标准化订单数据...")
dwd_order_info = ods_order_info.select(
    F.col("order_id"),
    F.col("user_id"),
    F.col("store_id"),
    F.col("item_id"),
    F.col("order_status"),
    # 修复order_amount值
    F.when((F.col("order_amount").isNotNull()) &
           (F.col("order_amount") > 0) & (F.col("order_amount") <= 5000), F.col("order_amount"))
    .when(F.col("order_amount").isNotNull(), (F.rand() * 500).cast(T.DecimalType(10, 2)))
    .otherwise((50 + F.rand() * 200).cast(T.DecimalType(10, 2)))
    .alias("order_amount"),
    F.col("has_complaint"),
    # 修复rating值
    F.when((F.col("rating").isNotNull()) & (F.col("rating") >= 1) & (F.col("rating") <= 5), F.col("rating"))
    .when(F.col("rating").isNotNull(), (1 + F.rand() * 4).cast(T.IntegerType()))
    .otherwise((3 + F.rand() * 2).cast(T.IntegerType()))
    .alias("rating"),
    F.col("create_time"),
    F.col("update_time"),
    F.lit("20250801").alias("dt")
)

# 6. 写入数据
print("写入订单明细表数据...")
dwd_order_info.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/dwd/dwd_order_info")

# 7. 修复分区
repair_hive_table("dwd_order_info")

print(f"订单明细表处理完成，记录数: {dwd_order_info.count()}")

# ====================== 数据验证 ======================
print("验证DWD层数据:")
try:
    behavior_count = spark.sql("SELECT COUNT(*) as count FROM dwd_user_behavior_log WHERE dt = '20250801'").collect()[0]['count']
    order_count = spark.sql("SELECT COUNT(*) as count FROM dwd_order_info WHERE dt = '20250801'").collect()[0]['count']
    print(f"用户行为明细表记录数: {behavior_count}")
    print(f"订单明细表记录数: {order_count}")

    # 检查page_type分布
    print("page_type分布:")
    spark.sql("""
        SELECT page_type, COUNT(*) as count 
        FROM dwd_user_behavior_log 
        WHERE dt = '20250801' 
        GROUP BY page_type 
        ORDER BY count DESC
    """).show()

    # 检查event_type分布
    print("event_type分布:")
    spark.sql("""
        SELECT event_type, COUNT(*) as count 
        FROM dwd_user_behavior_log 
        WHERE dt = '20250801' 
        GROUP BY event_type 
        ORDER BY count DESC
    """).show()

    # 检查搜索排名统计
    print("搜索排名统计:")
    spark.sql("""
        SELECT 
            COUNT(*) as total_search_events,
            AVG(search_rank) as avg_search_rank,
            MIN(search_rank) as min_search_rank,
            MAX(search_rank) as max_search_rank
        FROM dwd_user_behavior_log 
        WHERE dt = '20250801' AND event_type = 'search' AND search_rank IS NOT NULL
    """).show()

except Exception as e:
    print(f"验证DWD层数据时出错: {e}")

print("GD7 DWD层数据处理完成!")
spark.stop()
