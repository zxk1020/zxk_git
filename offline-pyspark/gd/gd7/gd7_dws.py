# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD7 DWS Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "E:/spark_temp") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置日志级别
spark.sparkContext.setLogLevel("WARN")

# 设置当前数据库
spark.sql("USE gd7")

# 设置分区日期
dt = '20250730'

print(f"开始处理GD7 DWS层数据，日期分区: {dt}")

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

def print_data_count(df, table_name):
    """打印数据量用于验证"""
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# ====================== 商品流量获取日汇总表 ======================
print("处理商品流量获取日汇总表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_product_traffic_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dws_product_traffic_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dws_product_traffic_d
(
    `store_id`             STRING COMMENT '店铺ID',
    `item_id`              STRING COMMENT '商品ID',
    `store_total_clicks`   BIGINT COMMENT '店铺总点击数',
    `item_visits`          BIGINT COMMENT '商品访问次数',
    `item_click_rate`      DECIMAL(5, 2) COMMENT '商品点击率(%)',
    `search_rank_score`    DECIMAL(5, 2) COMMENT '搜索排名得分',
    `detail_click_rate`    DECIMAL(5, 2) COMMENT '详情页点击率(%)',
    `update_time`          TIMESTAMP COMMENT '更新时间'
) COMMENT '商品流量获取日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/dws/dws_product_traffic_d'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
# 读取DWD层用户行为数据
user_behavior_log = spark.sql(f"SELECT * FROM dwd_user_behavior_log WHERE dt = '{dt}' AND store_id IS NOT NULL AND item_id IS NOT NULL")

# 计算流量获取指标
traffic_metrics = user_behavior_log.groupBy("store_id", "item_id").agg(
    # 店铺总点击数：统计店铺首页(session_id去重)的总点击次数
    F.countDistinct(F.when(F.col("page_type") == "home", F.col("session_id"))).cast("long").alias("store_total_clicks"),

    # 商品访问次数：统计商品相关页面的访问次数
    F.count(F.when(F.col("page_type").like("item%"), F.lit(1))).cast("long").alias("item_visits"),

    # 商品点击率：商品访问次数占店铺总点击数的比例
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("page_type") == "home", F.col("session_id"))) != 0,
            F.count(F.when(F.col("page_type").like("item%"), F.lit(1))) * 100.0 /
            F.countDistinct(F.when(F.col("page_type") == "home", F.col("session_id")))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("item_click_rate"),

    # 搜索排名得分：根据商品在搜索结果中的排名计算得分，排名越靠前得分越高
    F.coalesce(
        F.max(F.when(F.col("event_type") == "search", 100 - F.coalesce(F.col("search_rank"), F.lit(0))).otherwise(0)),
        F.lit(0)
    ).cast("decimal(5,2)").alias("search_rank_score"),

    # 详情页点击率：详情页访问次数占商品访问次数的比例
    F.round(
        F.when(
            F.count(F.when(F.col("page_type").like("item%"), F.lit(1))) != 0,
            F.count(F.when(F.col("page_type") == "item_detail", F.lit(1))) * 100.0 /
            F.count(F.when(F.col("page_type").like("item%"), F.lit(1)))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("detail_click_rate")
)

# 添加更新时间和分区字段
traffic_metrics = traffic_metrics \
    .withColumn("update_time", F.current_timestamp()) \
    .withColumn("dt", F.lit(dt))

# 验证数据量
print_data_count(traffic_metrics, "dws_product_traffic_d")

# 写入数据
traffic_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/dws/dws_product_traffic_d")

# 修复分区
repair_hive_table("dws_product_traffic_d")

# ====================== 商品转化日汇总表 ======================
print("处理商品转化日汇总表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_product_conversion_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dws_product_conversion_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dws_product_conversion_d
(
    `store_id`             STRING COMMENT '店铺ID',
    `item_id`              STRING COMMENT '商品ID',
    `conversion_rate`      DECIMAL(5, 2) COMMENT '转化率(%)',
    `cart_conversion_rate` DECIMAL(5, 2) COMMENT '加购转化率(%)',
    `payment_success_rate` DECIMAL(5, 2) COMMENT '支付成功率(%)',
    `update_time`          TIMESTAMP COMMENT '更新时间'
) COMMENT '商品转化日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/dws/dws_product_conversion_d'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
# 计算转化指标
conversion_metrics = user_behavior_log.groupBy("store_id", "item_id").agg(
    # 转化率：支付用户数占详情页访问用户数的比例
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("page_type").like("item%"), F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))) * 100.0 /
            F.countDistinct(F.when(F.col("page_type").like("item%"), F.col("user_id")))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("conversion_rate"),

    # 加购转化率：加购用户数占详情页访问用户数的比例
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("page_type").like("item%"), F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "add_to_cart", F.col("user_id"))) * 100.0 /
            F.countDistinct(F.when(F.col("page_type").like("item%"), F.col("user_id")))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("cart_conversion_rate"),

    # 支付成功率：支付成功用户数占支付用户数的比例
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "payment_success", F.col("user_id"))) * 100.0 /
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id")))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("payment_success_rate")
)

# 添加更新时间和分区字段
conversion_metrics = conversion_metrics \
    .withColumn("update_time", F.current_timestamp()) \
    .withColumn("dt", F.lit(dt))

# 验证数据量
print_data_count(conversion_metrics, "dws_product_conversion_d")

# 写入数据
conversion_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/dws/dws_product_conversion_d")

# 修复分区
repair_hive_table("dws_product_conversion_d")

# ====================== 商品内容营销日汇总表 ======================
print("处理商品内容营销日汇总表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_product_content_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dws_product_content_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dws_product_content_d
(
    `store_id`                 STRING COMMENT '店铺ID',
    `item_id`                  STRING COMMENT '商品ID',
    `avg_content_duration`     DECIMAL(10, 2) COMMENT '平均内容观看时长(秒)',
    `content_share_rate`       DECIMAL(5, 2) COMMENT '内容分享率(%)',
    `comment_interaction_rate` DECIMAL(5, 2) COMMENT '评论互动率(%)',
    `update_time`              TIMESTAMP COMMENT '更新时间'
) COMMENT '商品内容营销日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/dws/dws_product_content_d'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
# 计算内容营销指标
content_metrics = user_behavior_log.groupBy("store_id", "item_id").agg(
    # 平均内容观看时长：用户观看内容的平均时长(秒)
    F.round(
        F.coalesce(
            F.avg(F.when(F.col("event_type") == "content_view", F.coalesce(F.col("duration"), F.lit(0)))),
            F.lit(0)
        ),
        2
    ).cast("decimal(10,2)").alias("avg_content_duration"),

    # 内容分享率：分享内容的用户数占观看内容用户数的比例
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "content_share", F.col("user_id"))) * 100.0 /
            F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id")))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("content_share_rate"),

    # 评论互动率：评论用户数占观看内容用户数的比例
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id"))) != 0,
            F.countDistinct(F.when(F.col("event_type") == "comment", F.col("user_id"))) * 100.0 /
            F.countDistinct(F.when(F.col("event_type") == "content_view", F.col("user_id")))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("comment_interaction_rate")
)

# 添加更新时间和分区字段
content_metrics = content_metrics \
    .withColumn("update_time", F.current_timestamp()) \
    .withColumn("dt", F.lit(dt))

# 验证数据量
print_data_count(content_metrics, "dws_product_content_d")

# 写入数据
content_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/dws/dws_product_content_d")

# 修复分区
repair_hive_table("dws_product_content_d")

# ====================== 客户拉新日汇总表 ======================
print("处理客户拉新日汇总表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_customer_acquisition_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dws_customer_acquisition_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dws_customer_acquisition_d
(
    `store_id`                     STRING COMMENT '店铺ID',
    `item_id`                      STRING COMMENT '商品ID',
    `new_customer_purchase_ratio`  DECIMAL(5, 2) COMMENT '新客购买占比(%)',
    `new_customer_repurchase_rate` DECIMAL(5, 2) COMMENT '新客复购率(%)',
    `acquisition_cost_score`       DECIMAL(5, 2) COMMENT '获取成本得分',
    `update_time`                  TIMESTAMP COMMENT '更新时间'
) COMMENT '客户拉新日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/dws/dws_customer_acquisition_d'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
# 计算客户拉新指标
acquisition_metrics = user_behavior_log.groupBy("store_id", "item_id").agg(
    # 新客购买占比：新客支付用户数占总支付用户数的比例
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id"))) != 0,
            F.countDistinct(F.when((F.col("is_new_customer") == 1) & (F.col("event_type") == "payment"), F.col("user_id"))) * 100.0 /
            F.countDistinct(F.when(F.col("event_type") == "payment", F.col("user_id")))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("new_customer_purchase_ratio"),

    # 新客复购率：新客复购用户数占新客用户数的比例
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id"))) != 0,
            F.countDistinct(F.when((F.col("is_new_customer") == 1) & (F.col("repurchase_flag") == 1), F.col("user_id"))) * 100.0 /
            F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id")))
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("new_customer_repurchase_rate"),

    # 获取成本得分：基于平均获取成本计算的得分，成本越低得分越高
    F.round(
        F.when(
            F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id"))) != 0,
            100 - (
                    F.sum(F.when(F.col("is_new_customer") == 1, F.coalesce(F.col("acquisition_cost"), F.lit(0))).otherwise(0)) /
                    F.countDistinct(F.when(F.col("is_new_customer") == 1, F.col("user_id")))
            ) * 10
        ).otherwise(F.lit(0)),
        2
    ).cast("decimal(5,2)").alias("acquisition_cost_score")
)

# 添加更新时间和分区字段
acquisition_metrics = acquisition_metrics \
    .withColumn("update_time", F.current_timestamp()) \
    .withColumn("dt", F.lit(dt))

# 验证数据量
print_data_count(acquisition_metrics, "dws_customer_acquisition_d")

# 写入数据
acquisition_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/dws/dws_customer_acquisition_d")

# 修复分区
repair_hive_table("dws_customer_acquisition_d")

# ====================== 商品服务日汇总表 ======================
print("处理商品服务日汇总表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd7/dws/dws_product_service_d")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dws_product_service_d")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dws_product_service_d
(
    `store_id`               STRING COMMENT '店铺ID',
    `item_id`                STRING COMMENT '商品ID',
    `return_rate_score`      DECIMAL(5, 2) COMMENT '退货率得分',
    `complaint_rate_score`   DECIMAL(5, 2) COMMENT '投诉率得分',
    `positive_feedback_rate` DECIMAL(5, 2) COMMENT '好评率(%)',
    `update_time`            TIMESTAMP COMMENT '更新时间'
) COMMENT '商品服务日汇总表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd7/dws/dws_product_service_d'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 数据处理
# 读取DWD层订单数据
order_info = spark.sql(f"SELECT * FROM dwd_order_info WHERE dt = '{dt}' AND store_id IS NOT NULL AND item_id IS NOT NULL")

# 计算服务指标
service_metrics = order_info.groupBy("store_id", "item_id").agg(
    # 退货率得分：基于退货订单数计算的得分，退货率越低得分越高
    F.round(
        F.when(
            F.count(F.when(F.col("order_status").isin(["completed", "paid"]), F.lit(1))) != 0,
            100 - (
                    F.count(F.when(F.col("order_status") == "returned", F.lit(1))) * 100.0 /
                    F.count(F.when(F.col("order_status").isin(["completed", "paid"]), F.lit(1)))
            )
        ).otherwise(F.lit(100)),
        2
    ).cast("decimal(5,2)").alias("return_rate_score"),

    # 投诉率得分：基于投诉订单数计算的得分，投诉率越低得分越高
    F.round(
        F.when(
            F.count(F.when(F.col("order_status").isin(["completed", "paid"]), F.lit(1))) != 0,
            100 - (
                    F.count(F.when(F.col("has_complaint") == 1, F.lit(1))) * 100.0 /
                    F.count(F.when(F.col("order_status").isin(["completed", "paid"]), F.lit(1)))
            )
        ).otherwise(F.lit(100)),
        2
    ).cast("decimal(5,2)").alias("complaint_rate_score"),

    # 好评率：基于用户评分计算的得分，评分越高得分越高
    F.round(
        F.coalesce(
            F.avg(F.when(F.col("rating").isNotNull(), F.col("rating")).otherwise(0)) * 20,
            F.lit(0)
        ),
        2
    ).cast("decimal(5,2)").alias("positive_feedback_rate")
)

# 添加更新时间和分区字段
service_metrics = service_metrics \
    .withColumn("update_time", F.current_timestamp()) \
    .withColumn("dt", F.lit(dt))

# 验证数据量
print_data_count(service_metrics, "dws_product_service_d")

# 写入数据
service_metrics.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd7/dws/dws_product_service_d")

# 修复分区
repair_hive_table("dws_product_service_d")

# ====================== 验证DWS层数据 ======================
print("验证DWS层数据:")
try:
    traffic_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_traffic_d WHERE dt = '{dt}'").collect()[0]['count']
    conversion_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_conversion_d WHERE dt = '{dt}'").collect()[0]['count']
    content_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_content_d WHERE dt = '{dt}'").collect()[0]['count']
    acquisition_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_customer_acquisition_d WHERE dt = '{dt}'").collect()[0]['count']
    service_count = spark.sql(f"SELECT COUNT(*) as count FROM dws_product_service_d WHERE dt = '{dt}'").collect()[0]['count']

    print(f"Traffic表记录数: {traffic_count}")
    print(f"Conversion表记录数: {conversion_count}")
    print(f"Content表记录数: {content_count}")
    print(f"Acquisition表记录数: {acquisition_count}")
    print(f"Service表记录数: {service_count}")

except Exception as e:
    print(f"验证DWS层数据时出错: {e}")

print("GD7 DWS层数据处理完成!")
spark.stop()
