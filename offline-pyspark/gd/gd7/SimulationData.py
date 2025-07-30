# -*- coding: utf-8 -*-

import pymysql
import random
from datetime import datetime, timedelta
from decimal import Decimal
import time

# 直接在代码中定义数据库名称
MYSQL_DB = "gd7"  # 你可以在这里修改为你要使用的数据库名称

# MySQL配置
MYSQL_CONFIG = {
    "host": "192.168.142.130",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": MYSQL_DB,
    "charset": "utf8mb4"
}


# 获取过去30天内的随机时间
def get_random_time():
    start = datetime.now() - timedelta(days=30)
    end = datetime.now()
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


# 连接数据库
def get_db_connection():
    return pymysql.connect(
        host=MYSQL_CONFIG["host"],
        port=MYSQL_CONFIG["port"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"],
        charset=MYSQL_CONFIG["charset"]
    )


# 1. 模拟商品评分主表数据 product_score_main，10万条数据
def generate_and_insert_main_table_batch(batch_num, batch_size, start_id):
    # 生成数据
    data = []
    store_count = 50  # 店铺数量
    item_count = 2000  # 商品数量

    for i in range(batch_size):
        # 基本信息
        record_id = start_id + i
        store_id = f"store_{random.randint(1, store_count):04d}"
        item_id = f"item_{random.randint(1, item_count):06d}"
        random_dt = get_random_time()
        dt = random_dt.strftime("%Y-%m-%d")
        bdp_day = dt

        # 各维度得分（转换为Decimal类型）
        traffic = round(random.uniform(40, 98), 2)
        conversion = round(random.uniform(30, 95), 2)
        content = round(random.uniform(20, 90), 2)
        acquisition = round(random.uniform(35, 92), 2)
        service = round(random.uniform(50, 99), 2)

        # 计算总分
        total = round(
            traffic * 0.25 +
            conversion * 0.3 +
            content * 0.2 +
            acquisition * 0.15 +
            service * 0.1, 2
        )

        # 等级划分
        if total >= 85:
            grade = "A"
        elif total >= 70:
            grade = "B"
        elif total >= 50:
            grade = "C"
        else:
            grade = "D"

        # 时间字段
        create_time = random_dt.strftime("%Y-%m-%d %H:%M:%S")
        update_time = get_random_time().strftime("%Y-%m-%d %H:%M:%S")

        data.append((
            record_id, dt, store_id, item_id, traffic, conversion, content,
            acquisition, service, total, grade, update_time, bdp_day, create_time
        ))

    # 插入数据库
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO product_score_main 
            (id, dt, store_id, item_id, traffic_score, conversion_score, content_score, 
             acquisition_score, service_score, total_score, grade, update_time, bdp_day, create_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, data)
            connection.commit()
            print(f"批次 {batch_num}: 成功插入 {len(data)} 条商品评分主表数据")
    except Exception as e:
        print(f"批次 {batch_num}: 插入商品评分主表数据失败: {str(e)}")
        connection.rollback()
    finally:
        connection.close()


# 2. 模拟商品竞品对比表 product_diagnosis_compare，10万条数据
def generate_and_insert_compare_table_batch(batch_num, batch_size, start_id):
    # 先从数据库获取一些主表数据用于关联
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # 随机获取一些主表数据用于生成对比数据
            cursor.execute("""
                SELECT id, item_id, traffic_score, conversion_score, content_score, 
                       acquisition_score, service_score, total_score 
                FROM product_score_main 
                ORDER BY RAND() 
                LIMIT 5000
            """)
            item_scores = cursor.fetchall()

        if not item_scores:
            print(f"批次 {batch_num}: 未找到主表数据用于生成对比数据")
            return

    except Exception as e:
        print(f"批次 {batch_num}: 获取主表数据失败: {str(e)}")
        connection.close()
        return
    finally:
        connection.close()

    # 生成对比表数据
    data = []
    for i in range(batch_size):
        # 随机选择一个主表记录
        item_row = random.choice(item_scores)
        record_id = start_id + i
        product_score_id = item_row[0]
        item_id = item_row[1]

        # 各字段值
        traffic_score = float(item_row[2])
        conversion_score = float(item_row[3])
        content_score = float(item_row[4])
        acquisition_score = float(item_row[5])
        service_score = float(item_row[6])
        total_score = float(item_row[7])

        # 生成竞品数据
        competitor_total = round(total_score + random.uniform(-15, 15), 2)
        score_gap = round(total_score - competitor_total, 2)

        # 各维度差值
        traffic_gap = round(traffic_score - (traffic_score + random.uniform(-10, 10)), 2)
        conversion_gap = round(conversion_score - (conversion_score + random.uniform(-10, 10)), 2)
        content_gap = round(content_score - (content_score + random.uniform(-10, 10)), 2)
        acquisition_gap = round(acquisition_score - (acquisition_score + random.uniform(-10, 10)), 2)
        service_gap = round(service_score - (service_score + random.uniform(-10, 10)), 2)

        # 时间字段
        random_dt = get_random_time()
        dt = random_dt.strftime("%Y-%m-%d")
        bdp_day = dt
        create_time = random_dt.strftime("%Y-%m-%d %H:%M:%S")
        update_time = get_random_time().strftime("%Y-%m-%d %H:%M:%S")

        data.append((
            record_id, dt, item_id, product_score_id, competitor_total, score_gap,
            traffic_gap, conversion_gap, content_gap,
            acquisition_gap, service_gap, update_time, bdp_day, create_time
        ))

    # 插入数据库
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO product_diagnosis_compare 
            (id, dt, item_id, product_score_id, competitor_avg_score, score_gap, 
             traffic_gap, conversion_gap, content_gap, acquisition_gap, service_gap, 
             update_time, bdp_day, create_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(sql, data)
            connection.commit()
            print(f"批次 {batch_num}: 成功插入 {len(data)} 条商品竞品对比表数据")
    except Exception as e:
        print(f"批次 {batch_num}: 插入商品竞品对比表数据失败: {str(e)}")
        connection.rollback()
    finally:
        connection.close()


# 显示数据示例
def show_sample_data():
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            # 显示主表示例数据
            print("\n===== 商品评分主表示例数据（前5条） =====")
            cursor.execute("""
                SELECT id, item_id, total_score, grade, create_time, update_time 
                FROM product_score_main 
                ORDER BY id 
                LIMIT 5
            """)
            main_rows = cursor.fetchall()
            for row in main_rows:
                print(f"ID: {row[0]}, ItemID: {row[1]}, Score: {row[2]}, Grade: {row[3]}, Create: {row[4]}, Update: {row[5]}")

            # 显示对比表示例数据
            print("\n===== 商品竞品对比表示例数据（前5条） =====")
            cursor.execute("""
                SELECT id, item_id, product_score_id, competitor_avg_score, score_gap, create_time 
                FROM product_diagnosis_compare 
                ORDER BY id 
                LIMIT 5
            """)
            compare_rows = cursor.fetchall()
            for row in compare_rows:
                print(f"ID: {row[0]}, ItemID: {row[1]}, MainID: {row[2]}, CompetitorAvg: {row[3]}, ScoreGap: {row[4]}, Create: {row[5]}")

    except Exception as e:
        print(f"查询示例数据失败: {str(e)}")
    finally:
        connection.close()


# 主函数
def main():
    # 设置批次参数
    total_records = 100000
    batch_size = 5000  # 每批次5000条数据
    num_batches = total_records // batch_size

    print(f"开始生成数据，总共 {total_records} 条记录，分 {num_batches} 批次处理，每批次 {batch_size} 条记录")
    print(f"目标数据库: {MYSQL_DB}")

    # 分批生成并写入商品评分主表数据
    print("\n开始生成并写入商品评分主表数据...")
    for batch_num in range(num_batches):
        start_id = batch_num * batch_size + 1
        generate_and_insert_main_table_batch(batch_num + 1, batch_size, start_id)
        # 添加小延迟避免数据库压力过大
        time.sleep(0.1)

    print("商品评分主表数据生成并写入完成")

    # 分批生成并写入商品竞品对比表数据
    print("\n开始生成并写入商品竞品对比表数据...")
    for batch_num in range(num_batches):
        start_id = batch_num * batch_size + 1
        generate_and_insert_compare_table_batch(batch_num + 1, batch_size, start_id)
        # 添加小延迟避免数据库压力过大
        time.sleep(0.1)

    print("商品竞品对比表数据生成并写入完成")

    # 显示示例数据
    show_sample_data()

    print(f"\n数据生成完成，已写入数据库 {MYSQL_DB} 中每张表 {total_records} 条数据")


if __name__ == "__main__":
    main()
