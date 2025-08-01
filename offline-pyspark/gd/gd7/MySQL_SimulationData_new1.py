# -*- coding: utf-8 -*-
import pymysql
import random
from datetime import datetime, timedelta
from decimal import Decimal
import time
import re
from collections import defaultdict

# ==================== 动态配置字段（请在此处修改）====================
# 数据库配置
MYSQL_DB = "gd10"  # 目标数据库名
# 动态数据生成配置
TOTAL_RECORDS = 3000  # 总记录数
BATCH_SIZE = 1000  # 每批次记录数
DAY_TIME_RANGE = 1  # 生成天数
# MySQL配置
MYSQL_CONFIG = {
    "host": "192.168.142.130",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": MYSQL_DB,
    "charset": "utf8mb4"
}

# ==================== 动态建表语句列表（请在此处传入建表语句）====================
CREATE_TABLE_SQL_LIST = [
    """
    CREATE TABLE user_behavior_log (
    `id` BIGINT COMMENT '主键ID',
    `user_id` VARCHAR(64) COMMENT '用户ID',
    `session_id` VARCHAR(64) COMMENT '会话ID',
    `store_id` VARCHAR(64) COMMENT '店铺ID',
    `item_id` VARCHAR(64) COMMENT '商品ID',
    `page_type` VARCHAR(32) COMMENT '页面类型(home,item_list,item_detail,search_result)',
    `event_type` VARCHAR(32) COMMENT '事件类型(view,search,add_to_cart,payment,payment_success,content_view,content_share,comment)',
    `event_time` DATETIME COMMENT '事件时间',
    `search_rank` INT COMMENT '搜索排名',
    `duration` INT COMMENT '停留时长(秒)',
    `is_new_customer` TINYINT COMMENT '是否新客户(1:是,0:否)',
    `repurchase_flag` TINYINT COMMENT '复购标识(1:是,0:否)',
    `acquisition_cost` DECIMAL(10,2) COMMENT '获取成本',
    `rating` INT COMMENT '评分(1-5分)',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_user_id (user_id),
    INDEX idx_store_id (store_id),
    INDEX idx_item_id (item_id),
    INDEX idx_page_type (page_type),
    INDEX idx_event_type (event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层用户行为日志表，存储原始用户行为数据';
    """,
    """
    CREATE TABLE order_info (
    `id` BIGINT  COMMENT '主键ID',
    `order_id` VARCHAR(64) COMMENT '订单ID',
    `user_id` VARCHAR(64) COMMENT '用户ID',
    `store_id` VARCHAR(64) COMMENT '店铺ID',
    `item_id` VARCHAR(64) COMMENT '商品ID',
    `order_status` VARCHAR(32) COMMENT '订单状态(paid,completed,returned)',
    `order_amount` DECIMAL(10,2) COMMENT '订单金额',
    `has_complaint` TINYINT COMMENT '是否有投诉(1:是,0:否)',
    `rating` INT COMMENT '评分(1-5分)',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_user_id (user_id),
    INDEX idx_store_id (store_id),
    INDEX idx_item_id (item_id),
    INDEX idx_order_status (order_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层订单信息表，存储原始订单数据';

    """,
    """
    CREATE TABLE product_info (
    `id` BIGINT  COMMENT '主键ID',
    `product_id` VARCHAR(64) COMMENT '商品ID',
    `product_name` VARCHAR(255) COMMENT '商品名称',
    `category_id` VARCHAR(64) COMMENT '类目ID',
    `category_name` VARCHAR(255) COMMENT '类目名称',
    `brand` VARCHAR(128) COMMENT '品牌',
    `price` DECIMAL(10,2) COMMENT '价格',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_product_id (product_id),
    INDEX idx_category_id (category_id),
    INDEX idx_brand (brand)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层商品信息表，存储商品基础信息';
    """
]

# ==================== 业务规则和数据关联配置 ====================
# 商品类别映射
CATEGORIES = {
    '电子产品': ['手机', '电脑', '平板', '耳机'],
    '服装': ['T恤', '牛仔裤', '连衣裙', '外套'],
    '家居': ['沙发', '床', '餐桌', '椅子'],
    '图书': ['小说', '教材', '工具书', '杂志']
}

# 品牌映射
BRANDS = {
    '电子产品': ['苹果', '华为', '小米', '联想'],
    '服装': ['耐克', '阿迪达斯', '优衣库', 'ZARA'],
    '家居': ['宜家', '红星美凯龙', '顾家家居', '全友'],
    '图书': ['人民邮电', '机械工业', '清华大学', '电子工业']
}

# 用户池，用于保持数据一致性
user_pool = []
store_pool = []
item_pool = []
product_pool = []

# ==================== 核心功能代码 ====================
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

# 解析建表语句，提取表名和字段信息（增强自增属性识别）
def parse_create_table_sql(create_sql):
    # 提取表名
    table_name_match = re.search(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?\s*[({]',
        create_sql,
        re.IGNORECASE | re.DOTALL
    )
    if not table_name_match:
        raise ValueError("无法从建表语句中提取表名")
    table_name = table_name_match.group(1)

    # 提取字段信息，重点标记自增字段
    field_info = []
    field_pattern = r'`(\w+)`\s+([^,\n]+?)(?:\s+COMMENT\s+[\'\"](.*?)[\'\"])?(?:,|\s*\))'
    fields = re.findall(field_pattern, create_sql, re.IGNORECASE | re.DOTALL)

    for field in fields:
        field_name = field[0].strip()
        field_type = field[1].strip()
        field_comment = field[2].strip() if len(field) > 2 else ""

        # 标记自增属性（用于后续过滤）
        is_auto_increment = 'auto_increment' in field_type.lower()

        if field_name.upper() not in [
            'PRIMARY', 'KEY', 'INDEX', 'UNIQUE', 'CONSTRAINT', 'REFERENCES', 'ENGINE', 'DEFAULT'
        ]:
            field_info.append({
                'name': field_name,
                'type': field_type,
                'comment': field_comment,
                'is_auto_increment': is_auto_increment  # 新增：标记是否自增
            })
    return table_name, field_info

# 获取过去指定天数内的随机时间
def get_random_time():
    start = datetime.now() - timedelta(days=DAY_TIME_RANGE)
    end = datetime.now()
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# 生成商品相关信息
def generate_product_info():
    category = random.choice(list(CATEGORIES.keys()))
    sub_category = random.choice(CATEGORIES[category])
    brand = random.choice(BRANDS[category])
    product_name = f"{brand}{random.randint(1, 9999)}{sub_category}"
    price = round(random.uniform(10, 9999), 2)
    return category, sub_category, brand, product_name, price

# 根据字段类型和业务规则生成随机值
def generate_random_value(field_type, field_name, context=None):
    field_type = field_type.lower()

    # 根据业务上下文生成相关联的数据
    if context is None:
        context = {}

    if 'int' in field_type:
        if 'tinyint' in field_type:
            # 特殊处理业务字段
            if field_name in ['is_new_customer', 'repurchase_flag', 'has_complaint']:
                return random.choice([0, 1])
            elif field_name == 'rating':
                return random.randint(1, 5)
            return random.randint(0, 127)
        elif 'smallint' in field_type:
            return random.randint(0, 32767)
        elif 'bigint' in field_type:
            return random.randint(1, 1000000)
        else:
            return random.randint(1, 10000)
    elif 'decimal' in field_type or 'float' in field_type or 'double' in field_type:
        decimal_match = re.search(r'\((\d+),(\d+)\)', field_type)
        if decimal_match:
            precision = int(decimal_match.group(1))
            scale = int(decimal_match.group(2))
            max_val = 10 ** (precision - scale) - 1
            return round(random.uniform(0, min(max_val, 100)), scale)
        else:
            # 特殊处理订单金额，与商品价格相关
            if field_name == 'order_amount' and 'price' in context:
                return round(context['price'] * random.uniform(0.8, 1.2), 2)
            return round(random.uniform(0, 100), 2)
    elif 'varchar' in field_type or 'char' in field_type or 'text' in field_type:
        if 'id' in field_name.lower():
            if 'store' in field_name.lower():
                if store_pool and random.random() < 0.7:  # 70%概率复用已有店铺
                    return random.choice(store_pool)
                else:
                    store_id = f"store_{random.randint(1, 1000):04d}"
                    store_pool.append(store_id)
                    return store_id
            elif 'item' in field_name.lower():
                if item_pool and random.random() < 0.5:  # 50%概率复用已有商品
                    return random.choice(item_pool)
                else:
                    item_id = f"item_{random.randint(1, 10000):06d}"
                    item_pool.append(item_id)
                    return item_id
            elif 'product' in field_name.lower():
                if product_pool and random.random() < 0.5:  # 50%概率复用已有商品
                    return random.choice(product_pool)
                else:
                    product_id = f"product_{random.randint(1, 100000):08d}"
                    product_pool.append(product_id)
                    return product_id
            elif 'user' in field_name.lower():
                if user_pool and random.random() < 0.6:  # 60%概率复用已有用户
                    return random.choice(user_pool)
                else:
                    user_id = f"user_{random.randint(1, 10000):05d}"
                    user_pool.append(user_id)
                    return user_id
            elif 'order' in field_name.lower():
                return f"order_{random.randint(1, 999999):08d}"
            elif 'session' in field_name.lower():
                return f"session_{random.randint(1, 999999):08d}"
            else:
                return f"{field_name}_{random.randint(1, 10000)}"
        elif 'page_type' in field_name.lower():
            return random.choice(['home', 'item_list', 'item_detail', 'search_result'])
        elif 'event_type' in field_name.lower():
            return random.choice(['view', 'search', 'add_to_cart', 'payment', 'payment_success',
                                  'content_view', 'content_share', 'comment'])
        elif 'order_status' in field_name.lower():
            return random.choice(['paid', 'completed', 'returned'])
        elif 'category_name' in field_name.lower():
            return context.get('category_name', random.choice(list(CATEGORIES.keys())))
        elif 'brand' in field_name.lower():
            return context.get('brand', random.choice(list(BRANDS.get(context.get('category_name', '电子产品'), BRANDS['电子产品']))))
        elif 'product_name' in field_name.lower():
            return context.get('product_name', f"Product_{random.randint(1, 9999)}")
        elif field_name in ['category_id', 'category_name']:
            return context.get('category_name', random.choice(list(CATEGORIES.keys())))
        else:
            return f"value_{random.randint(1, 100000)}"
    elif 'datetime' in field_type or 'timestamp' in field_type:
        return get_random_time().strftime("%Y-%m-%d %H:%M:%S")
    elif 'date' in field_type:
        return get_random_time().strftime("%Y-%m-%d")
    else:
        return f"default_{random.randint(1, 1000)}"

# 动态创建表
def create_table_if_not_exists(create_sql):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(create_sql)
            connection.commit()
            print(f"表创建成功")
            return True
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"表已存在，无需重复创建")
            return True
        else:
            print(f"创建表失败: {str(e)}")
            return False
    finally:
        connection.close()

# 生成商品信息数据
def generate_product_info_data(fields, count):
    data = []
    non_auto_fields = [f for f in fields if not f['is_auto_increment']]

    for i in range(count):
        record_data = []
        # 生成商品相关信息上下文
        category_name, sub_category, brand, product_name, price = generate_product_info()
        context = {
            'category_name': category_name,
            'sub_category': sub_category,
            'brand': brand,
            'product_name': product_name,
            'price': price
        }

        for field in non_auto_fields:
            if field['name'] == 'category_name':
                value = category_name
            elif field['name'] == 'category_id':
                value = category_name
            elif field['name'] == 'brand':
                value = brand
            elif field['name'] == 'product_name':
                value = product_name
            elif field['name'] == 'price':
                value = price
            else:
                value = generate_random_value(field['type'], field['name'], context)
            record_data.append(value)
        data.append(tuple(record_data))
    return data

# 生成用户行为日志数据
def generate_user_behavior_data(fields, count):
    data = []
    non_auto_fields = [f for f in fields if not f['is_auto_increment']]

    for i in range(count):
        record_data = []
        context = {}

        # 为行为日志创建上下文
        if random.random() < 0.3:  # 30%概率是新用户
            user_id = f"user_{random.randint(1, 10000):05d}"
            user_pool.append(user_id)
            context['is_new_customer'] = 1
        else:
            context['is_new_customer'] = 0

        for field in non_auto_fields:
            if field['name'] == 'is_new_customer':
                value = context['is_new_customer']
            elif field['name'] == 'duration':
                # 停留时长根据页面类型调整
                page_type = context.get('page_type', '')
                if page_type in ['item_detail', 'content_view']:
                    value = random.randint(30, 300)  # 商品详情页停留时间较长
                else:
                    value = random.randint(5, 60)   # 其他页面停留时间较短
            elif field['name'] == 'event_type':
                value = random.choice(['view', 'search', 'add_to_cart', 'payment', 'payment_success',
                                       'content_view', 'content_share', 'comment'])
                context['event_type'] = value
            elif field['name'] == 'page_type':
                value = random.choice(['home', 'item_list', 'item_detail', 'search_result'])
                context['page_type'] = value
            else:
                value = generate_random_value(field['type'], field['name'], context)

            # 保存关键字段到上下文
            if field['name'] in ['user_id', 'item_id', 'store_id']:
                context[field['name']] = value

            record_data.append(value)
        data.append(tuple(record_data))
    return data

# 生成订单信息数据
def generate_order_info_data(fields, count):
    data = []
    non_auto_fields = [f for f in fields if not f['is_auto_increment']]

    for i in range(count):
        record_data = []
        context = {}

        for field in non_auto_fields:
            if field['name'] == 'order_status':
                value = random.choice(['paid', 'completed', 'returned'])
                context['order_status'] = value
            elif field['name'] == 'has_complaint':
                # 退货订单更可能有投诉
                if context.get('order_status') == 'returned':
                    value = random.choice([0, 1]) if random.random() < 0.7 else 1
                else:
                    value = random.choice([0, 1]) if random.random() < 0.95 else 1
            elif field['name'] == 'order_amount':
                # 订单金额与商品价格相关
                base_price = random.uniform(10, 1000)
                context['price'] = base_price
                value = round(base_price * random.uniform(0.9, 1.1), 2)
            else:
                value = generate_random_value(field['type'], field['name'], context)

            # 保存关键字段到上下文
            if field['name'] in ['user_id', 'item_id', 'store_id']:
                context[field['name']] = value

            record_data.append(value)
        data.append(tuple(record_data))
    return data

# 动态生成并插入数据
def generate_and_insert_data_batch(table_name, fields, batch_num, batch_size, start_id):
    data = []
    # 根据自增标记过滤字段，彻底排除id
    non_auto_fields = [f for f in fields if not f['is_auto_increment']]
    field_names = [field['name'] for field in non_auto_fields]
    print(f"字段信息: {field_names}")

    # 根据表名采用不同的数据生成策略
    if table_name == 'product_info':
        data = generate_product_info_data(fields, batch_size)
    elif table_name == 'user_behavior_log':
        data = generate_user_behavior_data(fields, batch_size)
    elif table_name == 'order_info':
        data = generate_order_info_data(fields, batch_size)
    else:
        # 默认数据生成方式
        for i in range(batch_size):
            record_data = []
            context = {}
            for field in non_auto_fields:
                value = generate_random_value(field['type'], field['name'], context)
                record_data.append(value)
                # 保存关键字段到上下文
                if field['name'] in ['user_id', 'item_id', 'store_id', 'product_id']:
                    context[field['name']] = value
            data.append(tuple(record_data))

    # 构造不含id的插入SQL
    placeholders = ', '.join(['%s'] * len(field_names))
    field_names_str = ', '.join([f'`{name}`' for name in field_names])
    insert_sql = f"INSERT INTO `{table_name}` ({field_names_str}) VALUES ({placeholders})"
    print(f"插入SQL: {insert_sql}")

    # 插入数据库
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.executemany(insert_sql, data)
            connection.commit()
            print(f"批次 {batch_num}: 成功插入 {len(data)} 条数据到表 {table_name}")
    except Exception as e:
        print(f"批次 {batch_num}: 插入数据失败: {str(e)}")
        connection.rollback()
        raise
    finally:
        connection.close()

# 显示表数据示例
def show_sample_data(table_name, limit=5):
    connection = get_db_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM `{table_name}` LIMIT {limit}")
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            print(f"\n===== {table_name}表数据示例（前{limit}条） =====")
            print(" | ".join(column_names))
            print("-" * (len(" | ".join(column_names)) + 10))
            for row in rows:
                print(" | ".join([str(item) if item is not None else "NULL" for item in row]))
    except Exception as e:
        print(f"查询示例数据失败: {str(e)}")
    finally:
        connection.close()

# 主函数
def main():
    try:
        print(f"开始处理数据库: {MYSQL_DB}")
        print(f"目标总记录数: {TOTAL_RECORDS}")
        print(f"每批次记录数: {BATCH_SIZE}")
        for i, create_sql in enumerate(CREATE_TABLE_SQL_LIST):
            print(f"\n========== 处理第 {i + 1} 个表 ==========")
            table_name, fields = parse_create_table_sql(create_sql)
            print(f"解析到表名: {table_name}")
            print("解析到字段:")
            for field in fields:
                print(f"  - {field['name']} ({field['type']}) - {field['comment']}")
            print(f"\n开始创建表 {table_name}...")
            if not create_table_if_not_exists(create_sql):
                print(f"跳过表 {table_name} 的数据生成")
                continue
            num_batches = TOTAL_RECORDS // BATCH_SIZE
            if TOTAL_RECORDS % BATCH_SIZE > 0:
                num_batches += 1
            print(f"\n开始生成数据，总共 {TOTAL_RECORDS} 条记录，分 {num_batches} 批次处理...")
            for batch_num in range(num_batches):
                start_id = batch_num * BATCH_SIZE + 1
                current_batch_size = min(BATCH_SIZE, TOTAL_RECORDS - (batch_num * BATCH_SIZE))
                generate_and_insert_data_batch(table_name, fields, batch_num + 1, current_batch_size, start_id)
                time.sleep(0.1)
            print(f"\n表 {table_name} 数据生成完成，共 {TOTAL_RECORDS} 条记录")
            show_sample_data(table_name)
        print(f"\n所有表数据生成完成")
    except Exception as e:
        print(f"执行过程出错：{str(e)}")
        raise

if __name__ == "__main__":
    main()
