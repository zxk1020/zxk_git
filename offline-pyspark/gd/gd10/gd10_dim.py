# -*- coding: utf-8 -*-
# 工单编号：大数据-电商数仓-10-流量主题页面分析看板

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD10 DIM Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "E:/spark_temp") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置当前数据库
spark.sql("USE gd10")

print("开始处理GD10 DIM层数据...")

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
    spark.sql(f"MSCK REPAIR TABLE gd10.{table_name}")
    print(f"修复分区完成：gd10.{table_name}")

# ====================== DIM层商品维度表 ======================
print("处理商品维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/dim/dim_product")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dim_product")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dim_product
(
    product_id    STRING COMMENT '商品ID',
    product_name  STRING COMMENT '商品名称',
    category_id   STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    brand         STRING COMMENT '品牌',
    price         DECIMAL(10, 2) COMMENT '价格',
    create_time   TIMESTAMP COMMENT '创建时间',
    update_time   TIMESTAMP COMMENT '更新时间'
) COMMENT '商品维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd10/dim/dim_product'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入商品维度表数据
print("插入商品维度表数据...")
ods_product_info = spark.sql("SELECT * FROM ods_product_info WHERE dt = '20250801'")

dim_product = ods_product_info.select(
    F.col("product_id"),
    F.col("product_name"),
    F.col("category_id"),
    F.col("category_name"),
    F.col("brand"),
    F.col("price"),
    F.col("create_time"),
    F.col("update_time"),
    F.lit("20250801").alias("dt")
)

# 写入数据
dim_product.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd10/dim/dim_product")

# 修复分区
repair_hive_table("dim_product")

print(f"商品维度表处理完成，记录数: {dim_product.count()}")

# ====================== DIM层店铺维度表 ======================
print("处理店铺维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/dim/dim_store")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dim_store")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dim_store
(
    store_id      STRING COMMENT '店铺ID',
    store_name    STRING COMMENT '店铺名称',
    merchant_id   STRING COMMENT '商户ID',
    merchant_name STRING COMMENT '商户名称',
    region        STRING COMMENT '地区',
    create_time   TIMESTAMP COMMENT '创建时间',
    update_time   TIMESTAMP COMMENT '更新时间'
) COMMENT '店铺维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd10/dim/dim_store'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入店铺维度表数据
print("插入店铺维度表数据...")
# 从用户行为日志提取店铺信息
store_ids_from_behavior = spark.sql("""
    SELECT DISTINCT store_id 
    FROM ods_user_behavior_log 
    WHERE dt = '20250801'
""")

# 从行为日志中提取店铺相关信息
store_info_from_behavior = spark.sql("""
    SELECT 
        store_id,
        FIRST(create_time) as create_time,
        FIRST(update_time) as update_time
    FROM ods_user_behavior_log 
    WHERE dt = '20250801' AND store_id IS NOT NULL
    GROUP BY store_id
""")

# 创建店铺维度数据
dim_store = store_info_from_behavior.join(store_ids_from_behavior, "store_id").select(
    F.col("store_id"),
    F.col("store_id").alias("store_name"),  # 直接使用店铺ID作为店铺名称
    F.col("store_id").alias("merchant_id"),  # 直接使用店铺ID作为商户ID
    F.col("store_id").alias("merchant_name"),  # 直接使用店铺ID作为商户名称
    # 根据店铺ID生成地区信息
    F.when(F.col("store_id").cast("int").isNotNull(),
           F.substring(F.col("store_id"), 1, 2))
    .otherwise(F.substring(F.col("store_id"), 1, 2))
    .alias("region"),
    F.col("create_time"),
    F.col("update_time"),
    F.lit("20250801").alias("dt")
)

# 写入数据
dim_store.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd10/dim/dim_store")

# 修复分区
repair_hive_table("dim_store")

print(f"店铺维度表处理完成，记录数: {dim_store.count()}")

# ====================== DIM层用户维度表 ======================
print("处理用户维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd10/dim/dim_user")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dim_user")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dim_user
(
    user_id       STRING COMMENT '用户ID',
    user_name     STRING COMMENT '用户名称',
    user_level    STRING COMMENT '用户等级',
    register_time TIMESTAMP COMMENT '注册时间',
    last_login_time TIMESTAMP COMMENT '最后登录时间',
    gender        STRING COMMENT '性别',
    age_group     STRING COMMENT '年龄段'
) COMMENT '用户维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd10/dim/dim_user'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入用户维度表数据
print("插入用户维度表数据...")
# 从用户行为日志提取用户信息
user_ids_from_behavior = spark.sql("""
    SELECT DISTINCT user_id 
    FROM ods_user_behavior_log 
    WHERE dt = '20250801'
""")

# 从行为日志中提取用户相关信息
user_info_from_behavior = spark.sql("""
    SELECT 
        user_id,
        MIN(event_time) as register_time,
        MAX(event_time) as last_login_time
    FROM ods_user_behavior_log 
    WHERE dt = '20250801' AND user_id IS NOT NULL
    GROUP BY user_id
""")

# 创建用户维度数据
dim_user = user_info_from_behavior.join(user_ids_from_behavior, "user_id").select(
    F.col("user_id"),
    F.col("user_id").alias("user_name"),  # 直接使用用户ID作为用户名
    # 根据用户ID生成用户等级
    F.when(F.col("user_id").cast("int") % 10 == 0, "VIP会员")
    .when(F.col("user_id").cast("int") % 5 == 0, "钻石会员")
    .when(F.col("user_id").cast("int") % 3 == 0, "金牌会员")
    .when(F.col("user_id").cast("int") % 2 == 0, "银牌会员")
    .otherwise("普通会员")
    .alias("user_level"),
    F.col("register_time"),
    F.col("last_login_time"),
    # 根据用户ID生成性别
    F.when(F.col("user_id").cast("int") % 2 == 0, "男")
    .otherwise("女")
    .alias("gender"),
    # 根据用户ID生成年龄段
    F.when(F.col("user_id").cast("int") % 10 < 3, "18-25岁")
    .when(F.col("user_id").cast("int") % 10 < 6, "26-35岁")
    .when(F.col("user_id").cast("int") % 10 < 8, "36-45岁")
    .otherwise("46-55岁")
    .alias("age_group"),
    F.lit("20250801").alias("dt")
)

# 写入数据
dim_user.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd10/dim/dim_user")

# 修复分区
repair_hive_table("dim_user")

print(f"用户维度表处理完成，记录数: {dim_user.count()}")

# 验证结果
print("验证DIM层数据:")
try:
    product_count = spark.sql("SELECT COUNT(*) as count FROM dim_product WHERE dt = '20250801'").collect()[0]['count']
    store_count = spark.sql("SELECT COUNT(*) as count FROM dim_store WHERE dt = '20250801'").collect()[0]['count']
    user_count = spark.sql("SELECT COUNT(*) as count FROM dim_user WHERE dt = '20250801'").collect()[0]['count']
    print(f"商品维度表记录数: {product_count}")
    print(f"店铺维度表记录数: {store_count}")
    print(f"用户维度表记录数: {user_count}")

except Exception as e:
    print(f"验证DIM层数据时出错: {e}")

print("GD10 DIM层数据处理完成!")
spark.stop()
