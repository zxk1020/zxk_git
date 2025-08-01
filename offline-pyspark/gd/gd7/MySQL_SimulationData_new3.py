# -*- coding: utf-8 -*-
import pymysql
import random
from datetime import datetime, timedelta
from decimal import Decimal
import time
import re
from collections import defaultdict
import json

# ==================== 动态配置字段（请在此处修改）====================
# 数据库配置
MYSQL_DB = "gd10"  # 目标数据库名
# 动态数据生成配置
TOTAL_RECORDS = 3000  # 总记录数
BATCH_SIZE = 1000  # 每批次记录数
DAY_TIME_RANGE = 30  # 生成天数范围（扩大时间范围以增加真实性）

# MySQL配置
MYSQL_CONFIG = {
    "host": "192.168.142.130",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": MYSQL_DB,
    "charset": "utf8mb4"
}

# ==================== 业务规则配置 ====================
# 实体池，用于维护数据一致性
ENTITY_POOLS = {
    'user_id': [],
    'store_id': [],
    'item_id': [],
    'product_id': [],
    'category_id': [],
    'brand': [],
    'activity_id': [],
    'sku_id': []
}

# 时间序列配置
TIME_CONTEXT = {
    'base_time': datetime.now() - timedelta(days=DAY_TIME_RANGE)
}

# 字段关联规则（定义字段间的依赖关系）
FIELD_RELATIONSHIPS = {
    # 商品相关信息
    'category_name': ['category_id'],
    'brand': ['category_name'],
    'price': ['category_name', 'brand'],
    # 用户行为相关
    'duration': ['page_type'],
    'event_type': ['page_type'],
    # 订单相关
    'order_amount': ['item_id'],
    'order_status': ['create_time'],
    'has_complaint': ['order_status'],
    # 活动相关
    'end_time': ['start_time'],
    'operate_time': ['create_time']
}

# 字段值约束规则
FIELD_CONSTRAINTS = {
    'page_type': ['home', 'item_list', 'item_detail', 'search_result'],
    'event_type': ['view', 'search', 'add_to_cart', 'payment', 'payment_success',
                   'content_view', 'content_share', 'comment'],
    'order_status': ['paid', 'completed', 'returned'],
    'rating': (1, 5),  # 评分范围1-5
    'is_new_customer': [0, 1],
    'repurchase_flag': [0, 1],
    'has_complaint': [0, 1],
    'activity_type': ['1', '2'],  # 1：满减，2：折扣
}

# 字段长度限制
FIELD_LENGTH_LIMITS = {
    'activity_type': 10,
    'activity_name': 200,
    'activity_desc': 2000
}

# ==================== 动态建表语句列表（请在此处传入建表语句）====================
CREATE_TABLE_SQL_LIST = [
    """
    CREATE TABLE `activity_info` (
  `id` bigint(20) NOT NULL COMMENT '活动id',
  `activity_name` varchar(200) DEFAULT NULL COMMENT '活动名称',
  `activity_type` varchar(10) DEFAULT NULL COMMENT '活动类型（1：满减，2：折扣）',
  `activity_desc` varchar(2000) DEFAULT NULL COMMENT '活动描述',
  `start_time` datetime DEFAULT NULL COMMENT '开始时间',
  `end_time` datetime DEFAULT NULL COMMENT '结束时间',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='活动表';
    """,
    """
    CREATE TABLE `activity_rule` (
  `id` int(11) NOT NULL COMMENT '编号',
  `activity_id` int(11) DEFAULT NULL COMMENT '活动id',
  `activity_type` varchar(20) DEFAULT NULL COMMENT '活动类型',
  `condition_amount` decimal(16,2) DEFAULT NULL COMMENT '满减金额',
  `condition_num` bigint(20) DEFAULT NULL COMMENT '满减件数',
  `benefit_amount` decimal(16,2) DEFAULT NULL COMMENT '优惠金额',
  `benefit_discount` decimal(10,2) DEFAULT NULL COMMENT '优惠折扣',
  `benefit_level` bigint(20) DEFAULT NULL COMMENT '优惠级别',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='活动规则表';

    """,
    """
    CREATE TABLE `activity_sku` (
  `id` bigint(20) NOT NULL COMMENT '编号',
  `activity_id` bigint(20) DEFAULT NULL COMMENT '活动id ',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'sku_id',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='活动商品关联表';
    """
]

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

# 解析建表语句，提取表名和字段信息
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

    # 提取字段信息
    field_info = []
    field_pattern = r'`(\w+)`\s+([^,\n]+?)(?:\s+COMMENT\s+[\'\"](.*?)[\'\"])?(?:,|\s*\))'
    fields = re.findall(field_pattern, create_sql, re.IGNORECASE | re.DOTALL)

    for field in fields:
        field_name = field[0].strip()
        field_type = field[1].strip()
        field_comment = field[2].strip() if len(field) > 2 else ""

        # 标记自增属性
        is_auto_increment = 'auto_increment' in field_type.lower()

        # 识别字段语义
        field_semantic = identify_field_semantic(field_name, field_comment)

        # 提取字段长度信息
        length_info = extract_field_length(field_type)

        # 判断是否为数字ID字段
        is_numeric_id = is_numeric_id_field(field_name, field_type)

        if field_name.upper() not in [
            'PRIMARY', 'KEY', 'INDEX', 'UNIQUE', 'CONSTRAINT', 'REFERENCES', 'ENGINE', 'DEFAULT'
        ]:
            field_info.append({
                'name': field_name,
                'type': field_type,
                'comment': field_comment,
                'is_auto_increment': is_auto_increment,
                'semantic': field_semantic,
                'length': length_info,
                'is_numeric_id': is_numeric_id
            })
    return table_name, field_info

# 判断是否为数字ID字段
def is_numeric_id_field(field_name, field_type):
    field_name = field_name.lower()
    field_type = field_type.lower()

    # 如果字段名包含id且类型为int或bigint，则认为是数字ID
    if 'id' in field_name and ('int' in field_type):
        return True
    return False

# 提取字段长度信息
def extract_field_length(field_type):
    # 匹配 varchar(n), char(n) 等长度信息
    length_match = re.search(r'(?:varchar|char)\((\d+)\)', field_type, re.IGNORECASE)
    if length_match:
        return int(length_match.group(1))
    return None

# 识别字段语义（用于更智能的数据生成）
def identify_field_semantic(field_name, field_comment):
    field_name = field_name.lower()
    field_comment = field_comment.lower() if field_comment else ""

    # ID类字段
    if 'id' in field_name:
        return field_name.replace('_id', '') + '_id'

    # 时间类字段
    if any(keyword in field_name for keyword in ['time', 'date']):
        return 'datetime'

    # 金额类字段
    if any(keyword in field_name for keyword in ['amount', 'price', 'cost']):
        return 'amount'

    # 状态类字段
    if 'status' in field_name:
        return 'status'

    # 类型类字段
    if 'type' in field_name:
        return 'type'

    # 标志类字段
    if any(keyword in field_name for keyword in ['flag', 'is_']):
        return 'flag'

    # 评分类字段
    if 'rating' in field_name or 'score' in field_name:
        return 'rating'

    # 名称类字段
    if 'name' in field_name:
        return 'name'

    # 描述类字段
    if 'desc' in field_name or 'description' in field_name:
        return 'description'

    return 'general'

# 获取随机时间（支持时间上下文）
def get_random_time(context=None, days_range=None, after_time=None):
    if days_range is None:
        days_range = DAY_TIME_RANGE

    if after_time:
        # 生成在指定时间之后的时间
        start = after_time
        end = after_time + timedelta(days=days_range)
    else:
        start = datetime.now() - timedelta(days=days_range)
        end = datetime.now()

    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)

# 智能生成字段值（基于字段语义和上下文）
def generate_smart_value(field, context=None):
    if context is None:
        context = {}

    field_name = field['name']
    field_type = field['type'].lower()
    field_semantic = field['semantic']
    field_length = field.get('length')

    # 如果上下文中已有该字段值，有一定概率复用
    if field_name in context and random.random() < 0.3:
        return context[field_name]

    # 根据约束规则生成值
    if field_name in FIELD_CONSTRAINTS:
        constraint = FIELD_CONSTRAINTS[field_name]
        if isinstance(constraint, list):
            value = random.choice(constraint)
        elif isinstance(constraint, tuple) and len(constraint) == 2:
            value = random.randint(constraint[0], constraint[1])
        else:
            value = constraint
        context[field_name] = value
        return value

    # 根据字段类型生成值
    if 'int' in field_type:
        if 'tinyint' in field_type:
            # 根据字段语义生成合理值
            if 'flag' in field_semantic or field_name.startswith('is_'):
                value = random.choice([0, 1])
            elif 'rating' in field_semantic:
                value = random.randint(1, 5)
            else:
                value = random.randint(0, 127)
        elif 'smallint' in field_type:
            value = random.randint(0, 32767)
        elif 'bigint' in field_type:
            # 对于数字ID字段，使用专门的生成函数
            if field.get('is_numeric_id', False):
                value = generate_id_numeric_value(field_name, context)
            else:
                value = random.randint(1, 1000000)
        else:
            # 对于int类型的ID字段，使用专门的生成函数
            if field.get('is_numeric_id', False):
                value = generate_id_numeric_value(field_name, context)
            else:
                value = random.randint(1, 10000)

    elif 'decimal' in field_type or 'float' in field_type or 'double' in field_type:
        decimal_match = re.search(r'\((\d+),(\d+)\)', field_type)
        if decimal_match:
            precision = int(decimal_match.group(1))
            scale = int(decimal_match.group(2))
            max_val = 10 ** (precision - scale) - 1
            value = round(random.uniform(0, min(max_val, 1000)), scale)
        else:
            # 根据字段语义生成合理值
            if 'amount' in field_semantic or 'price' in field_semantic or 'cost' in field_semantic:
                # 活动规则相关金额
                if 'condition' in field_name:
                    value = round(random.uniform(50, 1000), 2)
                elif 'benefit' in field_name:
                    if 'discount' in field_name:
                        value = round(random.uniform(0.5, 0.9), 2)  # 折扣率
                    else:
                        value = round(random.uniform(5, 100), 2)    # 优惠金额
                else:
                    value = round(random.uniform(10, 10000), 2)
            else:
                value = round(random.uniform(0, 100), 2)

    elif 'varchar' in field_type or 'char' in field_type or 'text' in field_type:
        # ID类字段 (varchar类型)
        if 'id' in field_name and ('varchar' in field_type or 'char' in field_type):
            value = generate_id_string_value(field_name, context)
        # 类型和状态字段
        elif 'type' in field_name or 'status' in field_name:
            if field_name in FIELD_CONSTRAINTS:
                value = random.choice(FIELD_CONSTRAINTS[field_name])
            else:
                value = f"{random.randint(1, 5)}"
        # 名称类字段
        elif 'name' in field_name:
            if 'activity' in field_name:
                activities = ['春节大促', '618年中大促', '双11狂欢', '品牌日', '会员日', '清仓特卖', '新品首发']
                value = random.choice(activities)
            elif 'product' in field_name:
                value = f"商品_{random.randint(1, 99999)}"
            elif 'category' in field_name:
                categories = ['电子产品', '服装', '家居', '图书', '运动']
                value = random.choice(categories)
            else:
                value = f"{field_name}_{random.randint(1, 9999)}"
        # 描述类字段
        elif 'desc' in field_name or 'description' in field_name:
            descriptions = [
                '年度最大优惠活动',
                '限时特价，错过再等一年',
                '品质保证，售后无忧',
                '新品上市，抢先体验',
                '会员专享，尊贵服务'
            ]
            value = random.choice(descriptions)
        else:
            value = f"value_{random.randint(1, 100000)}"

        # 应用长度限制
        max_length = FIELD_LENGTH_LIMITS.get(field_name, field_length)
        if max_length and len(str(value)) > max_length:
            value = str(value)[:max_length]

    elif 'datetime' in field_type or 'timestamp' in field_type:
        # 根据上下文生成合理时间
        if field_name == 'end_time' and 'start_time' in context:
            # 结束时间应该在开始时间之后
            start_time_str = context['start_time']
            start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
            value = get_random_time(context, days_range=10, after_time=start_time).strftime("%Y-%m-%d %H:%M:%S")
        elif field_name == 'operate_time' and 'create_time' in context:
            # 操作时间应该在创建时间之后
            create_time_str = context['create_time']
            create_time = datetime.strptime(create_time_str, "%Y-%m-%d %H:%M:%S")
            value = get_random_time(context, days_range=5, after_time=create_time).strftime("%Y-%m-%d %H:%M:%S")
        else:
            value = get_random_time(context).strftime("%Y-%m-%d %H:%M:%S")

    elif 'date' in field_type:
        value = get_random_time(context).strftime("%Y-%m-%d")

    else:
        value = f"default_{random.randint(1, 1000)}"

    context[field_name] = value
    return value

# 生成数字ID值（维护实体池一致性）
def generate_id_numeric_value(field_name, context):
    pool_key = field_name  # 使用字段名作为池的键

    # 有一定概率从现有池中选择
    if ENTITY_POOLS.get(pool_key) and len(ENTITY_POOLS[pool_key]) > 0 and random.random() < 0.7:
        # 确保从池中获取的是数字类型
        selected_value = random.choice(ENTITY_POOLS[pool_key])
        if isinstance(selected_value, str):
            # 如果是字符串，尝试转换为数字
            try:
                return int(selected_value)
            except ValueError:
                # 转换失败则生成新的数字ID
                pass
        else:
            return selected_value

    # 生成新的数字ID
    if 'activity' in field_name and 'id' in field_name:
        value = random.randint(10000, 99999)
        ENTITY_POOLS['activity_id'].append(value)
    elif 'sku' in field_name and 'id' in field_name:
        value = random.randint(100000, 999999)
        ENTITY_POOLS['sku_id'].append(value)
    else:
        value = random.randint(1, 100000)

    # 添加到实体池
    if pool_key in ENTITY_POOLS and 'activity' not in field_name and 'sku' not in field_name:
        ENTITY_POOLS[pool_key].append(value)
        # 限制池大小
        if len(ENTITY_POOLS[pool_key]) > 1000:
            ENTITY_POOLS[pool_key] = ENTITY_POOLS[pool_key][-500:]

    return value

# 生成字符串ID值（维护实体池一致性）
def generate_id_string_value(field_name, context):
    pool_key = field_name  # 使用字段名作为池的键

    # 有一定概率从现有池中选择
    if ENTITY_POOLS.get(pool_key) and len(ENTITY_POOLS[pool_key]) > 0 and random.random() < 0.7:
        # 确保从池中获取的是字符串类型
        selected_value = random.choice(ENTITY_POOLS[pool_key])
        if isinstance(selected_value, int):
            # 如果是数字，转换为字符串
            return str(selected_value)
        else:
            return selected_value

    # 生成新的字符串ID
    if 'user' in field_name:
        value = f"user_{random.randint(1, 5000):05d}"
    elif 'store' in field_name:
        value = f"store_{random.randint(1, 500):04d}"
    elif 'item' in field_name:
        value = f"item_{random.randint(1, 10000):06d}"
    elif 'product' in field_name:
        value = f"product_{random.randint(1, 20000):08d}"
    elif 'order' in field_name:
        value = f"order_{random.randint(1, 50000):08d}"
    elif 'session' in field_name:
        value = f"session_{random.randint(1, 100000):08d}"
    elif 'category' in field_name:
        value = f"category_{random.randint(1, 100):03d}"
    else:
        value = f"{field_name}_{random.randint(1, 10000)}"

    # 添加到实体池
    if pool_key in ENTITY_POOLS:
        ENTITY_POOLS[pool_key].append(value)
        # 限制池大小
        if len(ENTITY_POOLS[pool_key]) > 1000:
            ENTITY_POOLS[pool_key] = ENTITY_POOLS[pool_key][-500:]

    return value

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

# 智能生成并插入数据批次
def generate_and_insert_data_batch(table_name, fields, batch_num, batch_size, start_id):
    data = []
    # 过滤自增字段
    non_auto_fields = [f for f in fields if not f['is_auto_increment']]
    field_names = [field['name'] for field in non_auto_fields]
    print(f"字段信息: {field_names}")

    # 为每个表生成特定的数据
    for i in range(batch_size):
        record_data = []
        context = {}  # 用于维护记录内字段间的关系

        # 按照字段在表中的实际顺序生成字段值
        for field in non_auto_fields:
            value = generate_smart_value(field, context)
            # 确保返回的是正确类型
            if field.get('is_numeric_id', False) and isinstance(value, str):
                try:
                    value = int(value)
                except ValueError:
                    value = random.randint(1, 100000)
            elif not field.get('is_numeric_id', False) and isinstance(value, int) and 'time' not in field['semantic']:
                # 如果不是数字ID字段但生成了数字值，则转换为字符串
                value = str(value)
            record_data.append(value)

        data.append(tuple(record_data))

    # 构造插入SQL
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
        print(f"错误数据示例: {data[0] if data else '无数据'}")
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
        print(f"时间范围: {DAY_TIME_RANGE} 天")

        for i, create_sql in enumerate(CREATE_TABLE_SQL_LIST):
            print(f"\n========== 处理第 {i + 1} 个表 ==========")
            table_name, fields = parse_create_table_sql(create_sql)
            print(f"解析到表名: {table_name}")
            print("解析到字段:")
            for field in fields:
                length_info = f" (长度: {field['length']})" if field['length'] else ""
                id_type = " (数字ID)" if field.get('is_numeric_id') else ""
                print(f"  - {field['name']} ({field['type']}) - {field['comment']} (语义: {field['semantic']}){length_info}{id_type}")

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
                time.sleep(0.05)  # 减少延迟

            print(f"\n表 {table_name} 数据生成完成，共 {TOTAL_RECORDS} 条记录")
            show_sample_data(table_name)

        print(f"\n所有表数据生成完成")
        print("实体池统计:")
        for pool_name, pool_data in ENTITY_POOLS.items():
            print(f"  {pool_name}: {len(pool_data)} 个实体")

    except Exception as e:
        print(f"执行过程出错：{str(e)}")
        raise

if __name__ == "__main__":
    main()
