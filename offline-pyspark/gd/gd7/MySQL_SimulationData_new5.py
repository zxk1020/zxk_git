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

# ==================== 通用数据生成规则 ====================
# 通用字段语义识别规则
FIELD_SEMANTIC_RULES = [
    # (语义名称, 匹配模式列表)
    ('id', [r'(id|_id)$']),
    ('datetime', [r'(time|date|create|modify|update)']),
    ('amount', [r'(amount|price|cost|fee|salary)']),
    ('status', [r'status']),
    ('type', [r'type']),
    ('flag', [r'(flag|is_|has_|can_)']),
    ('rating', [r'(rating|score|level)']),
    ('name', [r'(name|title)']),
    ('description', [r'(desc|description|remark|comment)']),
    ('email', [r'email']),
    ('phone', [r'(phone|mobile|tel)']),
    ('url', [r'url']),
    ('address', [r'(address|addr)']),
    ('code', [r'code']),
    ('gender', [r'gender']),
    ('category', [r'category']),
    ('brand', [r'brand']),
    ('count', [r'(num|count|quantity|qty)'])
]

# 通用值生成规则
VALUE_GENERATION_RULES = {
    'id': lambda field, context: generate_id_value(field, context),
    'datetime': lambda field, context: generate_datetime_value(field, context),
    'amount': lambda field, context: round(random.uniform(10, 10000), 2),
    'status': lambda field, context: random.choice(['active', 'inactive', 'pending', 'completed', 'cancelled']),
    'type': lambda field, context: str(random.randint(1, 5)),
    'flag': lambda field, context: random.choice([0, 1]),
    'rating': lambda field, context: random.randint(1, 5),
    'name': lambda field, context: f"{field['name']}_{random.randint(1, 99999)}",
    'description': lambda field, context: random.choice([
        '年度最大优惠活动',
        '限时特价，错过再等一年',
        '品质保证，售后无忧',
        '新品上市，抢先体验',
        '会员专享，尊贵服务'
    ]),
    'email': lambda field, context: f"user{random.randint(1, 99999)}@example.com",
    'phone': lambda field, context: f"1{random.randint(3, 9)}{random.randint(0, 9)}{random.randint(10000000, 99999999)}",
    'url': lambda field, context: f"https://www.example{random.randint(1, 999)}.com",
    'address': lambda field, context: f"地址{random.randint(1, 9999)}号",
    'code': lambda field, context: f"CODE{random.randint(10000, 99999)}",
    'gender': lambda field, context: random.choice(['M', 'F']),
    'category': lambda field, context: random.choice(['电子产品', '服装', '家居', '图书', '运动']),
    'brand': lambda field, context: random.choice(['品牌A', '品牌B', '品牌C', '品牌D', '品牌E']),
    'count': lambda field, context: random.randint(1, 100),
    'general': lambda field, context: generate_general_value(field, context)
}

# 实体池，用于维护数据一致性
ENTITY_POOLS = defaultdict(list)

# 表关系映射（自动从外键约束中提取）
TABLE_RELATIONSHIPS = {}

# ==================== 动态建表语句列表（请在此处传入建表语句）====================
CREATE_TABLE_SQL_LIST = [
    """
    CREATE TABLE `sku_info` (
  `id` bigint(20) NOT NULL COMMENT 'skuid',
  `spu_id` bigint(20) DEFAULT NULL COMMENT 'spuid',
  `price` decimal(10,0) DEFAULT NULL COMMENT '价格',
  `sku_name` varchar(200) DEFAULT NULL COMMENT 'sku名称',
  `sku_desc` varchar(2000) DEFAULT NULL COMMENT '商品规格描述',
  `weight` decimal(10,2) DEFAULT NULL COMMENT '重量',
  `tm_id` bigint(20) DEFAULT NULL COMMENT '品牌id(冗余)',
  `category3_id` bigint(20) DEFAULT NULL COMMENT '三级品类id（冗余)',
  `sku_default_img` varchar(300) DEFAULT NULL COMMENT '默认显示图片地址(冗余)',
  `is_sale` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否在售（1：是 0：否）',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='sku表';
    """,
    """
    CREATE TABLE `order_detail` (
  `id` bigint(20) NOT NULL COMMENT '编号',
  `order_id` bigint(20) DEFAULT NULL COMMENT '订单id',
  `sku_id` bigint(20) DEFAULT NULL COMMENT 'sku_id',
  `sku_name` varchar(200) DEFAULT NULL COMMENT 'sku名称（冗余)',
  `img_url` varchar(200) DEFAULT NULL COMMENT '图片链接（冗余)',
  `order_price` decimal(10,2) DEFAULT NULL COMMENT '购买价格(下单时sku价格）',
  `sku_num` bigint(20) DEFAULT NULL COMMENT '购买个数',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `split_total_amount` decimal(16,2) DEFAULT NULL,
  `split_activity_amount` decimal(16,2) DEFAULT NULL,
  `split_coupon_amount` decimal(16,2) DEFAULT NULL,
  `operate_time` datetime DEFAULT NULL COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='订单明细表';
    """,
    """
    CREATE TABLE `user_info` (
  `id` bigint(20) NOT NULL COMMENT '编号',
  `login_name` varchar(200) DEFAULT NULL COMMENT '用户名称',
  `nick_name` varchar(200) DEFAULT NULL COMMENT '用户昵称',
  `passwd` varchar(200) DEFAULT NULL COMMENT '用户密码',
  `name` varchar(200) DEFAULT NULL COMMENT '用户姓名',
  `phone_num` varchar(200) DEFAULT NULL COMMENT '手机号',
  `email` varchar(200) DEFAULT NULL COMMENT '邮箱',
  `head_img` varchar(200) DEFAULT NULL COMMENT '头像',
  `user_level` varchar(200) DEFAULT NULL COMMENT '用户级别',
  `birthday` date DEFAULT NULL COMMENT '用户生日',
  `gender` varchar(1) DEFAULT NULL COMMENT '性别 M男,F女',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '修改时间',
  `status` varchar(200) DEFAULT NULL COMMENT '状态'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='用户表';

    """,
    """
    CREATE TABLE `order_info` (
  `id` bigint(20) NOT NULL COMMENT '编号',
  `consignee` varchar(100) DEFAULT NULL COMMENT '收货人',
  `consignee_tel` varchar(20) DEFAULT NULL COMMENT '收件人电话',
  `total_amount` decimal(10,2) DEFAULT NULL COMMENT '总金额',
  `order_status` varchar(20) DEFAULT NULL COMMENT '订单状态',
  `user_id` bigint(20) DEFAULT NULL COMMENT '用户id',
  `payment_way` varchar(20) DEFAULT NULL COMMENT '付款方式',
  `delivery_address` varchar(1000) DEFAULT NULL COMMENT '送货地址',
  `order_comment` varchar(200) DEFAULT NULL COMMENT '订单备注',
  `out_trade_no` varchar(50) DEFAULT NULL COMMENT '订单交易编号（第三方支付用)',
  `trade_body` varchar(200) DEFAULT NULL COMMENT '订单描述(第三方支付用)',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '操作时间',
  `expire_time` datetime DEFAULT NULL COMMENT '失效时间',
  `process_status` varchar(20) DEFAULT NULL COMMENT '进度状态',
  `tracking_no` varchar(100) DEFAULT NULL COMMENT '物流单编号',
  `parent_order_id` bigint(20) DEFAULT NULL COMMENT '父订单编号',
  `img_url` varchar(200) DEFAULT NULL COMMENT '图片链接',
  `province_id` int(11) DEFAULT NULL COMMENT '省份id',
  `activity_reduce_amount` decimal(16,2) DEFAULT NULL COMMENT '活动减免金额',
  `coupon_reduce_amount` decimal(16,2) DEFAULT NULL COMMENT '优惠券减免金额',
  `original_total_amount` decimal(16,2) DEFAULT NULL COMMENT '原始总金额',
  `feight_fee` decimal(16,2) DEFAULT NULL COMMENT '运费金额',
  `feight_fee_reduce` decimal(16,2) DEFAULT NULL COMMENT '运费减免金额',
  `refundable_time` datetime DEFAULT NULL COMMENT '可退款时间（签收后30天）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='订单表';

    """,
    """
    CREATE TABLE `spu_info` (
  `id` bigint(20) NOT NULL COMMENT 'spu_id',
  `spu_name` varchar(200) DEFAULT NULL COMMENT 'spu名称',
  `description` varchar(1000) DEFAULT NULL COMMENT '描述信息',
  `category3_id` bigint(20) DEFAULT NULL COMMENT '三级品类id',
  `tm_id` bigint(20) DEFAULT NULL COMMENT '品牌id',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `operate_time` datetime DEFAULT NULL COMMENT '修改时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='spu表';
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
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\'"]?(\w+)[`\'"]?\s*[({]',
        create_sql,
        re.IGNORECASE | re.DOTALL
    )
    if not table_name_match:
        raise ValueError("无法从建表语句中提取表名")
    table_name = table_name_match.group(1)

    # 提取字段信息 - 更准确的正则表达式
    field_info = []
    # 匹配字段定义，排除索引等非字段内容
    field_pattern = r'`(\w+)`\s+([^,\n]+?)(?:\s+COMMENT\s+[\'\"](.*?)[\'\"])?(?=,|\s*\)[^,]*?(?:ENGINE|COMMENT|DEFAULT|KEY|INDEX|UNIQUE|CONSTRAINT|PRIMARY))'
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

        # 提取精度信息（用于decimal类型）
        precision_info = extract_precision(field_type)

        # 判断是否为整数类型字段
        is_integer = is_integer_field(field_type)

        # 排除常见的非字段关键字
        excluded_keywords = ['PRIMARY', 'KEY', 'INDEX', 'UNIQUE', 'CONSTRAINT', 'REFERENCES', 'ENGINE', 'DEFAULT', 'USING']
        if field_name.upper() not in excluded_keywords:
            field_info.append({
                'name': field_name,
                'type': field_type,
                'comment': field_comment,
                'is_auto_increment': is_auto_increment,
                'semantic': field_semantic,
                'length': length_info,
                'precision': precision_info,
                'is_numeric_id': is_numeric_id,
                'is_integer': is_integer
            })

    # 提取外键关系
    extract_foreign_keys(create_sql, table_name)

    return table_name, field_info

# 判断是否为数字ID字段
def is_numeric_id_field(field_name, field_type):
    field_name = field_name.lower()
    field_type = field_type.lower()

    # 如果字段名包含id且类型为int或bigint，则认为是数字ID
    if 'id' in field_name and ('int' in field_type):
        return True
    return False

# 判断是否为整数类型字段
def is_integer_field(field_type):
    field_type = field_type.lower()
    return 'int' in field_type and 'int' in field_type

# 提取字段长度信息
def extract_field_length(field_type):
    # 匹配 varchar(n), char(n) 等长度信息
    length_match = re.search(r'(?:varchar|char|text)\((\d+)\)', field_type, re.IGNORECASE)
    if length_match:
        return int(length_match.group(1))
    return None

# 提取精度信息（用于decimal类型）
def extract_precision(field_type):
    # 匹配 decimal(m,n) 等精度信息
    precision_match = re.search(r'decimal\((\d+),(\d+)\)', field_type, re.IGNORECASE)
    if precision_match:
        return (int(precision_match.group(1)), int(precision_match.group(2)))
    return None

# 识别字段语义（用于更智能的数据生成）
def identify_field_semantic(field_name, field_comment):
    field_name = field_name.lower()
    field_comment = field_comment.lower() if field_comment else ""

    # 根据字段名模式匹配语义
    for semantic, patterns in FIELD_SEMANTIC_RULES:
        for pattern in patterns:
            if re.search(pattern, field_name):
                return semantic

    return 'general'

# 提取外键关系
def extract_foreign_keys(create_sql, table_name):
    # 匹配外键约束
    fk_pattern = r'CONSTRAINT\s+`?\w+`?\s+FOREIGN\s+KEY\s*\(?\s*`?(\w+)`?\s*\)?\s+REFERENCES\s+`?(\w+)`?\s*\(?\s*`?(\w+)`?\s*\)?'
    foreign_keys = re.findall(fk_pattern, create_sql, re.IGNORECASE)

    for fk_column, ref_table, ref_column in foreign_keys:
        if table_name not in TABLE_RELATIONSHIPS:
            TABLE_RELATIONSHIPS[table_name] = []
        TABLE_RELATIONSHIPS[table_name].append({
            'column': fk_column,
            'ref_table': ref_table,
            'ref_column': ref_column
        })

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

# 生成ID值
def generate_id_value(field, context):
    field_name = field['name']
    pool_key = field_name

    # 有一定概率从现有池中选择
    if ENTITY_POOLS.get(pool_key) and len(ENTITY_POOLS[pool_key]) > 0 and random.random() < 0.7:
        return random.choice(ENTITY_POOLS[pool_key])

    # 生成新的ID值
    if 'activity' in field_name:
        value = random.randint(10000, 99999)
    elif 'sku' in field_name:
        value = random.randint(100000, 999999)
    elif 'user' in field_name:
        value = random.randint(1, 5000)
    elif 'store' in field_name:
        value = random.randint(1, 500)
    elif 'item' in field_name or 'product' in field_name:
        value = random.randint(1, 20000)
    elif 'order' in field_name:
        value = random.randint(1, 50000)
    else:
        value = random.randint(1, 100000)

    # 添加到实体池
    ENTITY_POOLS[pool_key].append(value)
    # 限制池大小
    if len(ENTITY_POOLS[pool_key]) > 1000:
        ENTITY_POOLS[pool_key] = ENTITY_POOLS[pool_key][-500:]

    return value

# 生成时间值
def generate_datetime_value(field, context):
    field_name = field['name']

    # 根据上下文生成合理时间
    if field_name == 'end_time' and 'start_time' in context:
        # 结束时间应该在开始时间之后
        start_time_str = context['start_time']
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
        return get_random_time(context, days_range=10, after_time=start_time).strftime("%Y-%m-%d %H:%M:%S")
    elif field_name == 'operate_time' and 'create_time' in context:
        # 操作时间应该在创建时间之后
        create_time_str = context['create_time']
        create_time = datetime.strptime(create_time_str, "%Y-%m-%d %H:%M:%S")
        return get_random_time(context, days_range=5, after_time=create_time).strftime("%Y-%m-%d %H:%M:%S")
    else:
        return get_random_time(context).strftime("%Y-%m-%d %H:%M:%S")

# 生成日期值
def generate_date_value(field, context):
    # 生成随机日期
    random_time = get_random_time()
    return random_time.strftime("%Y-%m-%d")

# 生成通用值
def generate_general_value(field, context):
    field_type = field['type'].lower()
    field_length = field.get('length')

    # 根据字段类型生成值
    if 'int' in field_type:
        # 整数类型字段生成整数值
        return random.randint(1, 1000)
    elif 'decimal' in field_type or 'float' in field_type or 'double' in field_type:
        # 浮点数类型字段生成浮点数值
        return round(random.uniform(0, 1000), 2)
    elif 'varchar' in field_type or 'char' in field_type or 'text' in field_type:
        # 字符串类型字段生成字符串值
        value = f"value_{random.randint(1, 100000)}"
        # 应用长度限制
        if field_length and len(str(value)) > field_length:
            value = str(value)[:field_length]
        return value
    elif 'date' in field_type:
        # 日期类型字段生成日期值
        return generate_date_value(field, context)
    else:
        # 默认生成字符串值
        return f"default_{random.randint(1, 100000)}"

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

    # 根据字段语义生成值
    if field_semantic in VALUE_GENERATION_RULES:
        value = VALUE_GENERATION_RULES[field_semantic](field, context)
    else:
        # 根据字段类型生成值
        if 'int' in field_type:
            if 'tinyint' in field_type:
                value = random.randint(0, 127)
            elif 'smallint' in field_type:
                value = random.randint(0, 32767)
            elif 'bigint' in field_type:
                if field.get('is_numeric_id', False):
                    value = generate_id_value(field, context)
                else:
                    value = random.randint(1, 1000000)
            else:
                if field.get('is_numeric_id', False):
                    value = generate_id_value(field, context)
                else:
                    value = random.randint(1, 10000)

        elif 'decimal' in field_type or 'float' in field_type or 'double' in field_type:
            precision = field.get('precision')
            if precision:
                precision_val, scale = precision
                max_val = 10 ** (precision_val - scale) - 1
                value = round(random.uniform(0, min(max_val, 1000)), scale)
            else:
                value = round(random.uniform(0, 100), 2)

        elif 'varchar' in field_type or 'char' in field_type or 'text' in field_type:
            # 默认生成通用字符串
            value = f"value_{random.randint(1, 100000)}"

            # 应用长度限制
            if field_length and len(str(value)) > field_length:
                value = str(value)[:field_length]

        elif 'datetime' in field_type or 'timestamp' in field_type:
            value = generate_datetime_value(field, context)

        elif 'date' in field_type:
            value = generate_date_value(field, context)

        else:
            value = f"default_{random.randint(1, 1000)}"

    context[field_name] = value
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
                precision_info = f" (精度: {field['precision']})" if field['precision'] else ""
                id_type = " (数字ID)" if field.get('is_numeric_id') else ""
                integer_type = " (整数)" if field.get('is_integer') else ""
                print(f"  - {field['name']} ({field['type']}) - {field['comment']} (语义: {field['semantic']}){length_info}{precision_info}{id_type}{integer_type}")

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

        print("表关系映射:")
        for table_name, relationships in TABLE_RELATIONSHIPS.items():
            print(f"  {table_name}:")
            for rel in relationships:
                print(f"    {rel['column']} -> {rel['ref_table']}.{rel['ref_column']}")

    except Exception as e:
        print(f"执行过程出错：{str(e)}")
        raise

if __name__ == "__main__":
    main()
