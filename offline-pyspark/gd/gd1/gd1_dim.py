# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("GD1 DIM Layer") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .config("spark.local.dir", "E:/spark_temp") \
    .enableHiveSupport() \
    .getOrCreate()

# 设置当前数据库
spark.sql("USE gd1")

print("开始处理GD1 DIM层数据...")

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
    spark.sql(f"MSCK REPAIR TABLE gd1.{table_name}")
    print(f"修复分区完成：gd1.{table_name}")

# ====================== DIM层商品维度表 ======================
print("处理商品维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd1/dim/dim_product")

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
    leaf_category_id   STRING COMMENT '叶类目ID',
    leaf_category_name STRING COMMENT '叶类目名称',
    brand         STRING COMMENT '品牌',
    price         DECIMAL(10, 2) COMMENT '价格',
    create_time   TIMESTAMP COMMENT '创建时间',
    update_time   TIMESTAMP COMMENT '更新时间'
) COMMENT '商品维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd1/dim/dim_product'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入商品维度表数据
print("插入商品维度表数据...")
ods_product_info = spark.sql("SELECT * FROM ods_product_info WHERE dt = '20250801'")

# 直接使用ODS层的数据，不做额外修改
dim_product = ods_product_info.select(
    F.col("product_id"),
    F.col("product_name"),
    F.col("category_id"),
    F.col("category_name"),
    F.col("leaf_category_id"),
    F.col("leaf_category_name"),
    F.col("brand"),
    F.col("price"),
    F.col("create_time"),
    F.col("update_time"),
    F.lit("20250801").alias("dt")
)

# 写入数据
dim_product.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd1/dim/dim_product")

# 修复分区
repair_hive_table("dim_product")

print(f"商品维度表处理完成，记录数: {dim_product.count()}")

# ====================== DIM层类目维度表 ======================
print("处理类目维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd1/dim/dim_category")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dim_category")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dim_category
(
    category_id   STRING COMMENT '类目ID',
    category_name STRING COMMENT '类目名称',
    parent_category_id STRING COMMENT '父类目ID',
    level         INT COMMENT '类目层级',
    is_leaf       TINYINT COMMENT '是否叶类目(0:否 1:是)'
) COMMENT '类目维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd1/dim/dim_category'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入类目维度表数据
print("插入类目维度表数据...")
# 从商品信息中提取类目信息
category_info = spark.sql("""
    SELECT DISTINCT 
        category_id,
        category_name,
        '0' as parent_category_id,
        1 as level,
        0 as is_leaf
    FROM ods_product_info 
    WHERE dt = '20250801' AND category_id IS NOT NULL
    UNION
    SELECT DISTINCT 
        leaf_category_id as category_id,
        leaf_category_name as category_name,
        category_id as parent_category_id,
        2 as level,
        1 as is_leaf
    FROM ods_product_info 
    WHERE dt = '20250801' AND leaf_category_id IS NOT NULL
""")

dim_category = category_info.select(
    F.col("category_id"),
    F.col("category_name"),
    F.col("parent_category_id"),
    F.col("level"),
    F.col("is_leaf"),
    F.lit("20250801").alias("dt")
)

# 写入数据
dim_category.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd1/dim/dim_category")

# 修复分区
repair_hive_table("dim_category")

print(f"类目维度表处理完成，记录数: {dim_category.count()}")

# ====================== DIM层时间维度表 ======================
print("处理时间维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd1/dim/dim_date")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dim_date")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dim_date
(
    date_id       STRING COMMENT '日期ID',
    year          INT COMMENT '年',
    month         INT COMMENT '月',
    day           INT COMMENT '日',
    week_of_year  INT COMMENT '年中第几周',
    day_of_week   INT COMMENT '周中第几天',
    is_weekend    TINYINT COMMENT '是否周末(0:否 1:是)'
) COMMENT '时间维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd1/dim/dim_date'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入时间维度表数据
print("插入时间维度表数据...")
# 生成2025年8月1日的时间维度数据
dim_date = spark.createDataFrame([
    ("2025-08-01", 2025, 8, 1, 31, 6, 0)
], ["date_id", "year", "month", "day", "week_of_year", "day_of_week", "is_weekend"])

dim_date = dim_date.select(
    F.col("date_id"),
    F.col("year"),
    F.col("month"),
    F.col("day"),
    F.col("week_of_year"),
    F.col("day_of_week"),
    F.col("is_weekend"),
    F.lit("20250801").alias("dt")
)

# 写入数据
dim_date.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd1/dim/dim_date")

# 修复分区
repair_hive_table("dim_date")

print(f"时间维度表处理完成，记录数: {dim_date.count()}")

# ====================== DIM层用户维度表 ======================
print("处理用户维度表...")

# 1. 创建HDFS目录
create_hdfs_dir("/warehouse/gd1/dim/dim_user")

# 2. 删除旧表
spark.sql("DROP TABLE IF EXISTS dim_user")

# 3. 创建外部表
spark.sql("""
CREATE EXTERNAL TABLE dim_user
(
    user_id       STRING COMMENT '用户ID',
    user_name     STRING COMMENT '用户名称',
    gender        STRING COMMENT '性别',
    age_group     STRING COMMENT '年龄段',
    register_time TIMESTAMP COMMENT '注册时间'
) COMMENT '用户维度表'
PARTITIONED BY (dt string COMMENT '统计日期')
STORED AS PARQUET
LOCATION '/warehouse/gd1/dim/dim_user'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY')
""")

# 4. 插入用户维度表数据
print("插入用户维度表数据...")
# 从用户行为日志和订单信息提取用户信息
user_ids_from_behavior = spark.sql("""
    SELECT DISTINCT user_id 
    FROM ods_user_behavior_log 
    WHERE dt = '20250801'
""")

user_ids_from_orders = spark.sql("""
    SELECT DISTINCT user_id 
    FROM ods_order_info 
    WHERE dt = '20250801'
""")

# 合并所有用户ID
all_user_ids = user_ids_from_behavior.union(user_ids_from_orders).distinct()

# 从行为日志中提取用户相关信息
user_info_from_behavior = spark.sql("""
    SELECT 
        user_id,
        MIN(event_time) as register_time
    FROM ods_user_behavior_log 
    WHERE dt = '20250801' AND user_id IS NOT NULL
    GROUP BY user_id
""")

# 从订单信息中提取用户相关信息
user_info_from_orders = spark.sql("""
    SELECT 
        user_id,
        MIN(create_time) as register_time
    FROM ods_order_info 
    WHERE dt = '20250801' AND user_id IS NOT NULL
    GROUP BY user_id
""")

# 合并用户信息
all_user_info = user_info_from_behavior.union(user_info_from_orders).groupBy("user_id").agg(
    F.min("register_time").alias("register_time")
)

# 创建用户维度数据
dim_user = all_user_info.join(all_user_ids, "user_id").select(
    F.col("user_id"),
    F.col("user_id").alias("user_name"),  # 直接使用用户ID作为用户名
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
    F.col("register_time"),
    F.lit("20250801").alias("dt")
)

# 写入数据
dim_user.write.mode("overwrite") \
    .partitionBy("dt") \
    .parquet("/warehouse/gd1/dim/dim_user")

# 修复分区
repair_hive_table("dim_user")

print(f"用户维度表处理完成，记录数: {dim_user.count()}")

# 验证结果
print("验证DIM层数据:")
try:
    product_count = spark.sql("SELECT COUNT(*) as count FROM dim_product WHERE dt = '20250801'").collect()[0]['count']
    category_count = spark.sql("SELECT COUNT(*) as count FROM dim_category WHERE dt = '20250801'").collect()[0]['count']
    date_count = spark.sql("SELECT COUNT(*) as count FROM dim_date WHERE dt = '20250801'").collect()[0]['count']
    user_count = spark.sql("SELECT COUNT(*) as count FROM dim_user WHERE dt = '20250801'").collect()[0]['count']
    print(f"商品维度表记录数: {product_count}")
    print(f"类目维度表记录数: {category_count}")
    print(f"时间维度表记录数: {date_count}")
    print(f"用户维度表记录数: {user_count}")

    # 展示部分数据样本
    print("商品维度表样本数据:")
    spark.sql("SELECT * FROM dim_product WHERE dt = '20250801' LIMIT 5").show(truncate=False)

    print("类目维度表样本数据:")
    spark.sql("SELECT * FROM dim_category WHERE dt = '20250801' LIMIT 5").show(truncate=False)

    print("时间维度表样本数据:")
    spark.sql("SELECT * FROM dim_date WHERE dt = '20250801' LIMIT 5").show(truncate=False)

    print("用户维度表样本数据:")
    spark.sql("SELECT * FROM dim_user WHERE dt = '20250801' LIMIT 5").show(truncate=False)

except Exception as e:
    print(f"验证DIM层数据时出错: {e}")

print("GD1 DIM层数据处理完成!")
spark.stop()
