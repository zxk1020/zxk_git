# -*- coding: utf-8 -*-
import pymysql
import random
from datetime import datetime, timedelta
from decimal import Decimal
import time
import re
from collections import defaultdict
import json
from functools import wraps

# ==================== 动态配置字段（请在此处修改）====================
# 数据库配置
MYSQL_DB = "gd10"  # 目标数据库名
# 动态数据生成配置
TOTAL_RECORDS = 3000  # 总记录数
BATCH_SIZE = 1000  # 每批次记录数
DAY_TIME_RANGE = 0  # 生成天数范围（扩大时间范围以增加真实性）

# ==================== 动态建表语句列表（请在此处传入建表语句）====================
CREATE_TABLE_SQL_LIST = [
    """
    CREATE TABLE user_behavior_log (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    `user_id` VARCHAR(64) NOT NULL COMMENT '用户ID',
    `session_id` VARCHAR(64) NOT NULL COMMENT '会话ID',
    `store_id` VARCHAR(64) NOT NULL COMMENT '店铺ID',
    `item_id` VARCHAR(64) COMMENT '商品ID',
    `page_type` VARCHAR(32) NOT NULL COMMENT '页面类型(home-首页,item_list-商品列表,item_detail-商品详情,search_result-搜索结果)',
    `event_type` VARCHAR(32) NOT NULL COMMENT '事件类型(view-浏览,search-搜索,add_to_cart-加购,payment-支付,payment_success-支付成功,content_view-内容浏览,content_share-内容分享,comment-评论)',
    `event_time` DATETIME NOT NULL COMMENT '事件时间',
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
    INDEX idx_event_type (event_type),
    INDEX idx_event_time (event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层用户行为日志表，存储原始用户行为数据';
    """,
    """
    CREATE TABLE product_info (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    `product_id` VARCHAR(64) NOT NULL COMMENT '商品ID',
    `product_name` VARCHAR(255) NOT NULL COMMENT '商品名称',
    `category_id` VARCHAR(64) NOT NULL COMMENT '类目ID',
    `category_name` VARCHAR(255) NOT NULL COMMENT '类目名称',
    `brand` VARCHAR(128) NOT NULL COMMENT '品牌',
    `price` DECIMAL(10,2) NOT NULL COMMENT '价格',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_product_id (product_id),
    INDEX idx_category_id (category_id),
    INDEX idx_brand (brand)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层商品信息表，存储商品基础信息';
    """,
    """
    CREATE TABLE user_info (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    `user_id` VARCHAR(64) NOT NULL COMMENT '用户ID',
    `user_name` VARCHAR(255) NOT NULL COMMENT '用户名称',
    `user_level` VARCHAR(32) NOT NULL COMMENT '用户等级',
    `register_time` DATETIME NOT NULL COMMENT '注册时间',
    `last_login_time` DATETIME NOT NULL COMMENT '最后登录时间',
    `gender` VARCHAR(8) NOT NULL COMMENT '性别',
    `age_group` VARCHAR(16) NOT NULL COMMENT '年龄段',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层用户信息表，存储用户基础信息';
    """,
    """
    CREATE TABLE store_info (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    `store_id` VARCHAR(64) NOT NULL COMMENT '店铺ID',
    `store_name` VARCHAR(255) NOT NULL COMMENT '店铺名称',
    `merchant_id` VARCHAR(64) NOT NULL COMMENT '商户ID',
    `merchant_name` VARCHAR(255) NOT NULL COMMENT '商户名称',
    `region` VARCHAR(64) NOT NULL COMMENT '地区',
    `create_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX idx_store_id (store_id),
    INDEX idx_merchant_id (merchant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层店铺信息表，存储店铺基础信息';
    """
]

# MySQL配置
MYSQL_CONFIG = {
    "host": "192.168.142.130",
    "port": 3306,
    "user": "root",
    "password": "root",  # 在生产环境中应该使用环境变量或配置文件
    "database": MYSQL_DB,
    "charset": "utf8mb4",
    "autocommit": True
}

# ==================== 真实数据模板 ====================
# 真实的用户名模板（中文名）
REAL_USER_NAMES = [
    '张伟', '王伟', '王芳', '李伟', '李娜', '张敏', '李静', '王静', '刘伟', '王秀英',
    '张丽', '李秀英', '王丽', '张静', '李军', '王强', '张军', '李娟', '王军', '张勇',
    '李勇', '王艳', '李艳', '张艳', '王磊', '李磊', '张磊', '王琴', '李琴', '张琴',
    '刘洋', '杨欢', '陈晨', '赵磊', '孙涛', '周杰', '吴倩', '郑凯', '冯超', '蒋琳',
    '朱勇', '林霞', '徐丽', '高磊', '马超', '郭敏', '罗强', '梁军', '彭艳', '卢芳',
    '丁娜', '程刚', '袁芳', '唐宇', '邓丽', '许磊', '韩静', '冯丽', '曹强', '彭杰',
    '苏勇', '吕涛', '田军', '江霞', '汪艳', '龚伟', '万丽', '段超', '雷芳', '侯杰',
    '龙军', '白静', '史强', '陶艳', '黎勇', '贺霞', '顾磊', '孟军', '薛芳', '郝杰'
]

# 用户名拼音映射
USER_NAME_PINYIN = {
    '张伟': 'zhangwei', '王伟': 'wangwei', '王芳': 'wangfang', '李伟': 'liwei', '李娜': 'lina',
    '张敏': 'zhangmin', '李静': 'lijing', '王静': 'wangjing', '刘伟': 'liuwei', '王秀英': 'wangxiuying',
    '张丽': 'zhangli', '李秀英': 'lixiuming', '王丽': 'wangli', '张静': 'zhangjing', '李军': 'lijun',
    '王强': 'wangqiang', '张军': 'zhangjun', '李娟': 'lijuan', '王军': 'wangjun', '张勇': 'zhangyong',
    '李勇': 'liyong', '王艳': 'wangyan', '李艳': 'liyan', '张艳': 'zhangyan', '王磊': 'wanglei',
    '李磊': 'lilei', '张磊': 'zhanglei', '王琴': 'wangqin', '李琴': 'liqin', '张琴': 'zhangqin',
    '刘洋': 'liuyang', '杨欢': 'yanghuan', '陈晨': 'chenchen', '赵磊': 'zhaolei', '孙涛': 'suntao',
    '周杰': 'zhoujie', '吴倩': 'wuqian', '郑凯': 'zhengkai', '冯超': 'fengchao', '蒋琳': 'jianglin',
    '朱勇': 'zhuyong', '林霞': 'linxia', '徐丽': 'xuli', '高磊': 'gaolei', '马超': 'machao',
    '郭敏': 'guomin', '罗强': 'luoqiang', '梁军': 'liangjun', '彭艳': 'pengyan', '卢芳': 'lufang',
    '丁娜': 'dingna', '程刚': 'chenggang', '袁芳': 'yuanfang', '唐宇': 'tangyu', '邓丽': 'dengli',
    '许磊': 'xulei', '韩静': 'hanjing', '冯丽': 'fengli', '曹强': 'caoqiang', '彭杰': 'pengjie',
    '苏勇': 'suyong', '吕涛': 'lyutao', '田军': 'tianjun', '江霞': 'jiangxia', '汪艳': 'wangyan',
    '龚伟': 'gongwei', '万丽': 'wanli', '段超': 'duanchao', '雷芳': 'leifang', '侯杰': 'houjie',
    '龙军': 'longjun', '白静': 'baijing', '史强': 'shiqiang', '陶艳': 'taoyan', '黎勇': 'liyong',
    '贺霞': 'hexia', '顾磊': 'gulei', '孟军': 'mengjun', '薛芳': 'xuefang', '郝杰': 'haojie'
}

# 英文用户名模板
ENGLISH_USER_NAMES = [
    'alice', 'bob', 'charlie', 'david', 'emma', 'frank', 'grace', 'henry', 'ivy', 'jack',
    'karen', 'leo', 'mia', 'nathan', 'olivia', 'peter', 'quinn', 'rachel', 'steven', 'tina',
    'umar', 'victor', 'wendy', 'xander', 'yara', 'zack', 'adam', 'bella', 'chris', 'diana',
    'edward', 'fiona', 'george', 'helen', 'ian', 'julia', 'kevin', 'laura', 'michael', 'nina',
    'oscar', 'paula', 'quentin', 'rita', 'samuel', 'tracy', 'ulysses', 'vivian', 'walter', 'xenia',
    'yvonne', 'zane', 'aaron', 'brian', 'cathy', 'derek', 'elaine', 'felix', 'gina', 'howard'
]

# 真实的商品品类和详细信息
PRODUCT_CATEGORIES = {
    '手机数码': {
        'products': [
            'iPhone 15 Pro', 'Samsung Galaxy S24', '华为P70', '小米14', 'OPPO Find X7',
            'vivo X100 Pro', '荣耀Magic6', '一加12', '魅族21 Pro', '努比亚Z60 Ultra',
            'realme GT6', 'iQOO 12', '红米K70', '真我GT Neo6', '摩托罗拉edge 50 Pro',
            '索尼Xperia 1 VI', 'Google Pixel 8 Pro', 'Nothing Phone (2)', '传音Infinix GT 10 Pro',
            '中兴Axon 60 Ultra'
        ],
        'descriptions': [
            '全新未拆封，品质保证', '官方正品，支持七天无理由退换', '高端配置，性能强劲',
            '拍照神器，颜值担当', '旗舰级处理器，运行流畅', '超清摄像系统，捕捉精彩瞬间',
            '长续航设计，告别电量焦虑', '精美工艺，手感出众', '智能系统，操作便捷',
            '多功能集成，满足日常需求'
        ]
    },
    '电脑办公': {
        'products': [
            'MacBook Pro 16寸', 'Dell XPS 13', '华为MateBook X', '联想ThinkPad X1',
            'Surface Pro 9', '华硕灵耀X', '惠普战66', '小米笔记本Pro', '荣耀MagicBook',
            '机械革命蛟龙', '神舟战神Z8', '微星Creator Z17', 'ROG枪神7', '外星人m18',
            '苹果MacBook Air', '戴尔Inspiron 14', '华硕VivoBook 15', '联想小新Pro',
            '宏碁掠夺者战斧', '惠普暗影精灵9'
        ],
        'descriptions': [
            '专业级品质，值得信赖', '高端配置，性能强劲', '轻薄便携，办公利器',
            '商务必备，彰显品味', '高清显示屏，视觉体验佳', '强劲处理器，多任务处理',
            '散热优秀，稳定运行', '接口丰富，扩展性强', '续航持久，移动办公首选',
            '键盘手感舒适，打字体验佳'
        ]
    },
    '家用电器': {
        'products': [
            '美的变频空调', '格力中央空调', '海尔冰箱', '西门子洗衣机', '戴森吸尘器V15',
            '松下微波炉', '飞利浦电动牙刷', '九阳豆浆机', '苏泊尔电饭煲', '老板油烟机',
            '方太燃气灶', '小米扫地机器人', '科沃斯擦窗宝', '奥克斯加湿器', '艾美特取暖器',
            '先锋电风扇', '志高除湿机', 'TCL电视65寸', '海信激光电视', '创维电视55寸'
        ],
        'descriptions': [
            '节能环保，绿色生活', '智能科技，便捷生活', '静音运行，舒适体验',
            '大容量存储，保鲜效果好', '高效清洁，省时省力', '人性化设计，操作简单',
            '品质可靠，经久耐用', '多功能集成，满足家庭需求', '安全防护，使用放心',
            '外观时尚，提升家居品味'
        ]
    },
    '服装鞋帽': {
        'products': [
            'Nike Air Jordan 1', 'Adidas Ultraboost 22', 'New Balance 990v5',
            '优衣库摇粒绒外套', 'ZARA牛仔裤', 'H&M连衣裙', '太平鸟卫衣', '森马夹克',
            '海澜之家衬衫', '李宁跑鞋', '安踏篮球鞋', '特步运动裤', '361°运动T恤',
            '匹克运动背包', '乔丹运动帽', '鸿星尔克运动袜', '贵人鸟运动手套',
            '德尔惠运动护腕', '回力帆布鞋', '飞跃小白鞋'
        ],
        'descriptions': [
            '明星同款，时尚潮流', '经典款式，永不过时', '舒适透气，运动必备',
            '保暖舒适，秋冬必备', '版型修身，展现魅力', '面料优质，穿着舒适',
            '做工精细，品质保证', '色彩丰富，搭配多样', '尺码齐全，适合各类人群',
            '洗涤方便，易于保养'
        ]
    },
    '运动户外': {
        'products': [
            '迪卡侬登山包', '探路者帐篷', '牧高笛睡袋', '挪客野餐垫', '凯乐石登山杖',
            '奥索卡冲锋衣', '土拨鼠防晒衣', '哥伦比亚户外鞋', '始祖鸟背包', '北面冲锋衣',
            '萨洛蒙徒步鞋', '阿迪达斯运动水壶', '耐克运动毛巾', '安德玛运动腰包',
            'Under Armour运动袜', '斯伯丁篮球', '威尔胜网球拍', '尤尼克斯羽毛球拍',
            '红双喜乒乓球拍', '李宁跳绳'
        ],
        'descriptions': [
            '专业运动装备', '舒适透气，运动必备', '经典款式，永不过时',
            '轻便耐用，户外首选', '功能齐全，适应多种环境', '安全可靠，保障运动安全',
            '设计人性化，使用便捷', '材质优良，经久耐用', '品牌保证，值得信赖',
            '性价比高，物超所值'
        ]
    },
    '美妆个护': {
        'products': [
            '兰蔻小黑瓶精华', '雅诗兰黛小棕瓶', 'SK-II神仙水', '资生堂红腰子',
            '欧莱雅复颜抗皱', '玉兰油多效修护', '科颜氏高保湿霜', '倩碧黄油',
            '薇诺娜舒敏保湿', '理肤泉B5修复', '薇姿温泉矿物霜', '雅漾舒护活泉',
            '飞利浦电动牙刷', '欧乐B电动牙刷', '舒客牙膏', '高露洁牙膏',
            '海飞丝洗发水', '潘婷护发素', '沙宣洗护套装', '施华蔻专业护发'
        ],
        'descriptions': [
            '专业护肤，品质保证', '明星产品，口碑之选', '深层滋养，焕发肌肤活力',
            '健康生活，从这里开始', '科学研究配方，效果显著', '温和不刺激，适合敏感肌',
            '多重功效合一，满足护肤需求', '天然成分，安全放心', '使用方便，易于吸收',
            '长期使用，改善肤质'
        ]
    },
    '食品饮料': {
        'products': [
            '星巴克咖啡豆', '三顿半咖啡', '瑞幸咖啡液', '雀巢咖啡', '麦斯威尔咖啡',
            '伊利纯牛奶', '蒙牛特仑苏', '光明优倍', '君乐宝酸奶', '安慕希酸奶',
            '可口可乐', '百事可乐', '雪碧', '芬达', '美年达',
            '康师傅方便面', '统一老坛酸菜', '今麦郎拉面', '白象大骨面', '五谷道场'
        ],
        'descriptions': [
            '品质保证，香醇可口', '精选原料，口感丰富', '提神醒脑，享受时光',
            '便捷冲泡，随时随地享受', '营养丰富，健康美味', '品牌保证，值得信赖',
            '工艺精良，品质稳定', '包装精美，送礼佳品', '保质期长，储存方便',
            '多种口味，满足不同喜好'
        ]
    },
    '游戏装备': {
        'products': [
            '索尼PlayStation 5', '任天堂Switch OLED', '微软Xbox Series X',
            'Steam Deck掌机', 'ROG游戏本', '外星人游戏本', '机械革命游戏本',
            '雷蛇游戏鼠标', '罗技G502', '赛睿鼠标', '海盗船键盘', '雷柏键盘',
            'HyperX耳机', '赛睿耳机', '罗技G933', '北通游戏手柄', '小鸡手柄',
            '8BitDo手柄', '游戏椅DXRacer', '安德玛电竞椅'
        ],
        'descriptions': [
            '家庭娱乐，畅快体验', '高端配置，性能强劲', '游戏爱好者的首选',
            '便携设计，随时随地游戏', '沉浸式体验，身临其境', '画质清晰，流畅运行',
            '操控精准，反应灵敏', '人体工学设计，舒适体验', '兼容性强，支持多平台',
            '品质可靠，经久耐用'
        ]
    },
    '家居用品': {
        'products': [
            '宜家沙发', '顾家家居床', '林氏木业衣柜', '全友家居餐桌', '红苹果茶几',
            '左右沙发', '芝华仕头等舱', '喜临门床垫', '慕思枕头', '富安娜床品',
            '罗莱家纺四件套', '水星家纺被子', '博洋家纺枕套', '洁丽雅毛巾',
            '金号毛巾', '维达纸巾', '清风纸巾', '心相印湿巾', '蓝漂湿巾',
            '得宝纸巾'
        ],
        'descriptions': [
            '舒适家居，品质生活', '设计时尚，提升品味', '材质环保，健康安全',
            '做工精细，经久耐用', '功能实用，满足日常需求', '款式多样，选择丰富',
            '安装简便，使用方便', '清洁容易，维护简单', '尺寸标准，适配性强',
            '品牌保证，值得信赖'
        ]
    },
    '图书音像': {
        'products': [
            '红楼梦', '西游记', '三国演义', '水浒传', '围城',
            '活着', '平凡的世界', '白夜行', '解忧杂货店', '三体',
            '时间简史', '人类简史', '未来简史', '原则', '穷爸爸富爸爸',
            'Kindle电子书', 'iPad阅读器', '掌阅iReader', '当当阅读器', '微信读书'
        ],
        'descriptions': [
            '经典名著，文化传承', '知识丰富，增长见识', '印刷精美，阅读体验佳',
            '内容精彩，引人入胜', '装帧考究，收藏价值高', '正版图书，品质保证',
            '纸张优质，保护视力', '携带方便，随时随地阅读', '种类齐全，满足不同需求',
            '教育意义深远，启发思考'
        ]
    }
}

# 更多真实地址模板
REAL_ADDRESSES = [
    '北京市朝阳区建国路88号', '上海市浦东新区陆家嘴环路1000号',
    '广州市天河区珠江新城华夏路100号', '深圳市南山区科技园南区高新南一道99号',
    '杭州市西湖区文三路200号', '成都市锦江区春熙路100号',
    '武汉市江汉区解放大道690号', '南京市鼓楼区中山路200号',
    '西安市雁塔区长安南路300号', '重庆市渝中区解放碑步行街100号',
    '天津市和平区滨江道188号', '苏州市工业园区星海街58号',
    '青岛市市南区香港中路200号', '大连市中山区人民路88号',
    '厦门市思明区湖滨南路300号', '长沙市芙蓉区五一大道158号',
    '郑州市金水区花园路88号', '济南市历下区泺源大街99号',
    '沈阳市和平区南京北街188号', '昆明市五华区金碧路200号',
    '福州市鼓楼区五四路158号', '合肥市蜀山区长江西路200号',
    '南昌市东湖区八一大道300号', '南宁市青秀区民族大道100号',
    '海口市龙华区国贸大道88号', '贵阳市南明区中华南路200号',
    '兰州市城关区张掖路100号', '西宁市城中区长江路58号',
    '银川市兴庆区解放东街88号', '乌鲁木齐沙依巴克区友好南路200号'
]

# 更多真实邮箱域名
EMAIL_DOMAINS = [
    '163.com', '126.com', 'qq.com', 'sina.com', 'sohu.com',
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
    'aliyun.com', 'foxmail.com', 'live.com', 'msn.com',
    'sina.cn', 'tom.com', '21cn.com', 'yeah.net', '263.net',
    'wo.cn', '139.com', '189.cn', 'hainan.net', 'eyou.com'
]

# 更多真实品牌名称
REAL_BRANDS = [
    '苹果', '三星', '华为', '小米', 'OPPO', 'vivo', '联想', '戴尔', '惠普',
    '耐克', '阿迪达斯', '新百伦', '优衣库', 'ZARA', 'H&M', '太平鸟', '森马',
    '美的', '格力', '海尔', '西门子', '松下', '九阳', '苏泊尔', '老板',
    '兰蔻', '雅诗兰黛', 'SK-II', '资生堂', '欧莱雅', '玉兰油', '科颜氏',
    '可口可乐', '百事可乐', '雀巢', '伊利', '蒙牛', '君乐宝', '安慕希',
    '路易威登', '香奈儿', '迪奥', '古驰', '普拉达', '爱马仕', '卡地亚',
    '奔驰', '宝马', '奥迪', '丰田', '本田', '大众', '福特', '现代',
    '宜家', '无印良品', '名创优品', '网易严选', '小米有品', '京东京造',
    '索尼', '微软', '佳能', '尼康', '富士', '卡西欧', '飞利浦', '松下',
    '迪士尼', '乐高', '孩之宝', '任天堂', '暴雪', '腾讯', '网易', '阿里'
]

# 更真实的图片域名
IMAGE_DOMAINS = [
    'img.examplestore.com', 'images.shopmall.com', 'pic.ecommerce.cn',
    'static.onlineshop.net', 'cdn.retailworld.com', 'assets.shoppingzone.org',
    'image.mystore.com', 'photos.onlinestore.cn', 'media.ecommerce.net'
]

# 图片路径模板
IMAGE_PATHS = [
    '/products/main.jpg', '/products/detail.png', '/items/gallery.webp',
    '/goods/preview.jpg', '/catalog/display.png', '/inventory/photo.webp',
    '/uploads/products/front.jpg', '/images/items/back.png', '/media/goods/side.webp'
]

# 真实的订单状态
ORDER_STATUSES = ['待付款', '待发货', '已发货', '已完成', '已取消', '退款中', '已退款']

# 真实的支付方式
PAYMENT_WAYS = ['支付宝', '微信支付', '银行卡', '货到付款', 'Apple Pay', '银联支付', '京东支付', '花呗']

# 真实的用户级别
USER_LEVELS = ['普通会员', '银牌会员', '金牌会员', '钻石会员', 'VIP会员']

# 密码字符集
PASSWORD_CHARS = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*'

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
    ('count', [r'(num|count|quantity|qty)']),
    ('image', [r'(img|image)']),
    ('password', [r'(passwd|password)']),
    ('level', [r'level']),
    ('way', [r'way']),
    ('birthday', [r'(birth|birthday)'])
]

# 通用值生成规则
VALUE_GENERATION_RULES = {
    'id': lambda field, context: generate_id_value(field, context),
    'datetime': lambda field, context: generate_datetime_value(field, context),
    'amount': lambda field, context: round(random.uniform(10, 10000), 2),
    'status': lambda field, context: generate_status_value(field, context),
    'type': lambda field, context: str(random.randint(1, 5)),
    'flag': lambda field, context: random.choice([0, 1]),
    'rating': lambda field, context: random.randint(1, 5),
    'name': lambda field, context: generate_realistic_name(field, context),
    'description': lambda field, context: generate_product_description(field, context),
    'email': lambda field, context: generate_realistic_email(field, context),
    'phone': lambda field, context: f"1{random.randint(3, 9)}{random.randint(0, 9)}{random.randint(10000000, 99999999)}",
    'url': lambda field, context: generate_realistic_image_url(field, context),
    'address': lambda field, context: random.choice(REAL_ADDRESSES),
    'code': lambda field, context: f"CODE{random.randint(10000, 99999)}",
    'gender': lambda field, context: random.choice(['M', 'F']),
    'category': lambda field, context: random.choice(list(PRODUCT_CATEGORIES.keys())),
    'brand': lambda field, context: random.choice(REAL_BRANDS),
    'count': lambda field, context: random.randint(1, 100),
    'image': lambda field, context: generate_realistic_image_url(field, context),
    'password': lambda field, context: generate_realistic_password(field, context),
    'level': lambda field, context: random.choice(USER_LEVELS),
    'way': lambda field, context: random.choice(PAYMENT_WAYS),
    'birthday': lambda field, context: generate_birthday_value(field, context),
    'general': lambda field, context: generate_realistic_general_value(field, context)
}

# 实体池，用于维护数据一致性
ENTITY_POOLS = defaultdict(list)

# 表关系映射（自动从外键约束中提取）
TABLE_RELATIONSHIPS = {}

# 已生成的SPU信息，用于SKU关联
SPU_INFO_POOL = []

# 已生成的用户信息，用于订单关联
USER_INFO_POOL = []

# 已生成的订单信息，用于订单详情关联
ORDER_INFO_POOL = []

# 已生成的SKU信息，用于订单详情关联
SKU_INFO_POOL = []

# 数据库连接池
class DatabaseConnectionPool:
    def __init__(self, config, max_connections=10):
        self.config = config
        self.max_connections = max_connections
        self.connections = []
        self.used_connections = []

    def get_connection(self):
        if self.connections:
            conn = self.connections.pop()
            self.used_connections.append(conn)
            return conn
        elif len(self.used_connections) < self.max_connections:
            conn = pymysql.connect(**self.config)
            self.used_connections.append(conn)
            return conn
        else:
            # 如果达到最大连接数，等待并重试
            time.sleep(0.1)
            return self.get_connection()

    def release_connection(self, conn):
        if conn in self.used_connections:
            self.used_connections.remove(conn)
            self.connections.append(conn)

    def close_all(self):
        for conn in self.connections + self.used_connections:
            try:
                conn.close()
            except:
                pass
        self.connections = []
        self.used_connections = []

# 创建全局连接池
db_pool = DatabaseConnectionPool(MYSQL_CONFIG)



# ==================== 核心功能代码 ====================
# 连接数据库
def get_db_connection():
    return db_pool.get_connection()

# 释放数据库连接
def release_db_connection(conn):
    db_pool.release_connection(conn)

# 生成真实的名称
def generate_realistic_name(field, context):
    field_name = field['name'].lower()

    # 用户真实姓名（直接使用真实姓名）
    if 'name' in field_name and ('user' in field_name or field_name == 'name'):
        return random.choice(REAL_USER_NAMES)

    # 用户登录名（真实姓名拼音+随机种子）
    elif 'login' in field_name:
        # 如果上下文中有真实姓名，使用该姓名的拼音
        if 'name' in context and context['name'] in USER_NAME_PINYIN:
            pinyin = USER_NAME_PINYIN[context['name']]
        else:
            # 否则随机选择一个姓名
            name = random.choice(REAL_USER_NAMES)
            pinyin = USER_NAME_PINYIN.get(name, name.lower())

        # 添加随机后缀，可以是数字或字母组合
        suffix_type = random.choice(['number', 'letters', 'mixed'])
        if suffix_type == 'number':
            suffix = str(random.randint(100, 9999))
        elif suffix_type == 'letters':
            suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=random.randint(3, 5)))
        else:  # mixed
            suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(3, 5)))
        return f"{pinyin}_{suffix}"

    # 用户昵称（可以是英文名或其他）
    elif 'nick' in field_name:
        # 70%概率使用英文名，30%概率使用真实姓名拼音
        if random.random() < 0.7:
            nickname = random.choice(ENGLISH_USER_NAMES)
            suffix = str(random.randint(10, 999))
            return f"{nickname}{suffix}"
        else:
            name = random.choice(REAL_USER_NAMES)
            pinyin = USER_NAME_PINYIN.get(name, name.lower())
            suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(2, 4)))
            return f"{pinyin}{suffix}"

    # 商品相关名称
    elif 'sku' in field_name or 'spu' in field_name or 'product' in field_name:
        # 随机选择一个品类，然后从中选择商品
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        product = random.choice(PRODUCT_CATEGORIES[category]['products'])
        return product

    # 收货人名称
    elif 'consignee' in field_name:
        return random.choice(REAL_USER_NAMES)

    # 通用名称
    else:
        name = random.choice(REAL_USER_NAMES)
        pinyin = USER_NAME_PINYIN.get(name, name.lower())
        suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(3, 6)))
        return f"{pinyin}_{suffix}"

# 生成生日值
def generate_birthday_value(field, context):
    # 生成合理的生日日期（18-60年前）
    end_date = datetime.now() - timedelta(days=18*365)
    start_date = datetime.now() - timedelta(days=60*365)

    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    birthday = start_date + timedelta(days=random_days)
    return birthday.strftime("%Y-%m-%d")

# 生成真实的邮箱
def generate_realistic_email(field, context):
    # 优先使用上下文中的姓名
    if 'name' in context and context['name'] in USER_NAME_PINYIN:
        pinyin = USER_NAME_PINYIN[context['name']]
    else:
        name = random.choice(REAL_USER_NAMES)
        pinyin = USER_NAME_PINYIN.get(name, name.lower())

    number = random.randint(1, 9999)
    domain = random.choice(EMAIL_DOMAINS)
    return f"{pinyin}{number}@{domain}"

# 生成状态值
def generate_status_value(field, context):
    field_name = field['name'].lower()

    # 订单状态
    if 'order' in field_name:
        return random.choice(ORDER_STATUSES)

    # 用户状态
    elif 'user' in field_name:
        return random.choice(['active', 'inactive', 'pending', 'suspended'])

    # 通用状态
    else:
        return random.choice(['active', 'inactive', 'pending', 'completed', 'cancelled'])

# 生成商品描述
def generate_product_description(field, context):
    # 如果上下文中有商品名称，则根据商品名称所属品类生成匹配的描述
    if 'sku_name' in context:
        product_name = context['sku_name']
        # 查找商品所属品类
        for category, info in PRODUCT_CATEGORIES.items():
            if product_name in info['products']:
                return random.choice(info['descriptions'])
        # 如果没找到对应品类，使用默认描述
        return random.choice(['全新未拆封，品质保证', '官方正品，支持七天无理由退换', '高端配置，性能强劲'])
    elif 'spu_name' in context:
        product_name = context['spu_name']
        # 查找商品所属品类
        for category, info in PRODUCT_CATEGORIES.items():
            if product_name in info['products']:
                return random.choice(info['descriptions'])
        # 如果没找到对应品类，使用默认描述
        return random.choice(['全新未拆封，品质保证', '官方正品，支持七天无理由退换', '高端配置，性能强劲'])
    else:
        # 随机选择一个品类并生成对应描述
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        return random.choice(PRODUCT_CATEGORIES[category]['descriptions'])

# 生成真实的图片URL
def generate_realistic_image_url(field, context):
    domain = random.choice(IMAGE_DOMAINS)
    path = random.choice(IMAGE_PATHS)
    # 添加随机参数避免重复
    param = random.randint(100000, 999999)
    return f"https://{domain}{path}?v={param}"

# 生成真实的密码
def generate_realistic_password(field, context):
    # 生成8-16位的随机密码
    length = random.randint(8, 16)
    # 确保至少包含一个数字、一个小写字母、一个大写字母和一个特殊字符
    password = [
        random.choice('0123456789'),
        random.choice('abcdefghijklmnopqrstuvwxyz'),
        random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ'),
        random.choice('!@#$%^&*')
    ]

    # 填充剩余长度
    for _ in range(length - 4):
        password.append(random.choice(PASSWORD_CHARS))

    # 打乱顺序
    random.shuffle(password)
    return ''.join(password)

# 生成真实的通用值
def generate_realistic_general_value(field, context):
    field_name = field['name'].lower()
    field_type = field['type'].lower()
    field_length = field.get('length')

    # 根据字段名生成特定类型的值
    if 'img' in field_name or 'image' in field_name:
        return generate_realistic_image_url(field, context)
    elif 'passwd' in field_name or 'password' in field_name:
        return generate_realistic_password(field, context)
    elif 'level' in field_name:
        return random.choice(USER_LEVELS)
    elif 'way' in field_name:
        return random.choice(PAYMENT_WAYS)
    elif 'comment' in field_name or 'remark' in field_name:
        return generate_product_description(field, context)
    elif 'head' in field_name:
        domain = random.choice(IMAGE_DOMAINS)
        return f"https://{domain}/avatars/avatar{random.randint(1, 200)}.jpg"
    elif 'url' in field_name:
        return generate_realistic_image_url(field, context)
    elif 'code' in field_name:
        prefix = random.choice(['CODE', 'ITEM', 'PROD', 'SKU'])
        return f"{prefix}{random.randint(100000, 999999)}"
    elif 'trade' in field_name:
        return f"TRADE{random.randint(1000000000, 9999999999)}"
    elif 'tracking' in field_name:
        prefix = random.choice(['TRACK', 'SF', 'YT', 'ZTO', 'STO', 'JD', 'EMS'])
        return f"{prefix}{random.randint(1000000000, 9999999999)}"
    else:
        # 根据字段类型生成值
        if 'int' in field_type:
            return random.randint(1, 10000)
        elif 'decimal' in field_type or 'float' in field_type or 'double' in field_type:
            return round(random.uniform(0, 10000), 2)
        elif 'varchar' in field_type or 'char' in field_type or 'text' in field_type:
            # 生成更有意义的字符串
            name = random.choice(REAL_USER_NAMES)
            pinyin = USER_NAME_PINYIN.get(name, name.lower())
            suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(3, 6)))
            value = f"{pinyin}_{suffix}"
        elif 'date' in field_type:
            if 'birth' in field_name:
                return generate_birthday_value(field, context)
            else:
                return generate_date_value(field, context)
        else:
            name = random.choice(REAL_USER_NAMES)
            pinyin = USER_NAME_PINYIN.get(name, name.lower())
            suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(3, 6)))
            value = f"{pinyin}_{suffix}"

    # 应用长度限制
    if field_length and len(str(value)) > field_length:
        value = str(value)[:field_length]

    return value

# 解析建表语句，提取表名和字段信息
def parse_create_table_sql(create_sql):
    # 提取表名 - 更健壮的正则表达式
    table_name_match = re.search(
        r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\'"]?(\w+)[`\'"]?\s*[({]',
        create_sql,
        re.IGNORECASE | re.DOTALL
    )
    if not table_name_match:
        # 尝试另一种模式，处理可能包含链接的情况
        table_name_match = re.search(
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?[`\'"]?([^\s\(]+)[`\'"]?\s*[({]',
            create_sql,
            re.IGNORECASE | re.DOTALL
        )

    if not table_name_match:
        raise ValueError("无法从建表语句中提取表名")
    table_name = table_name_match.group(1)

    # 提取字段信息
    field_info = []
    # 更精确的字段匹配模式，排除索引等非字段定义
    field_pattern = r'`(\w+)`\s+([^,\n]+?)(?:\s+COMMENT\s+[\'\"](.*?)[\'\"])?(?:,|\s*\)[^,]*?(?:ENGINE|COMMENT|DEFAULT))'
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

# 自动推断表间关系
def infer_table_relationships(table_names_and_fields):
    """
    根据字段名自动推断表间关系
    """
    relationships = {}

    # 收集所有表的ID字段信息
    table_ids = {}
    for table_name, fields in table_names_and_fields.items():
        for field in fields:
            # 如果字段是ID字段且是主键或自增字段
            if field['is_numeric_id'] and field['is_auto_increment']:
                table_ids[table_name] = field['name']
                break
        # 如果没有找到明确的ID字段，使用默认的'id'
        if table_name not in table_ids:
            table_ids[table_name] = 'id'

    # 推断外键关系
    for table_name, fields in table_names_and_fields.items():
        relationships[table_name] = []
        for field in fields:
            field_name = field['name']
            # 检查是否为外键字段（以_id结尾但不是主键）
            if field_name.endswith('_id') and not field['is_auto_increment']:
                # 尝试推断引用的表名
                ref_table_candidate = field_name[:-3]  # 去掉'_id'后缀

                # 查找匹配的表
                matched_table = None
                for table in table_names_and_fields.keys():
                    # 精确匹配
                    if table == ref_table_candidate:
                        matched_table = table
                        break
                    # 模糊匹配（表名包含候选名）
                    elif ref_table_candidate in table:
                        matched_table = table
                        break

                # 如果找到匹配的表
                if matched_table and matched_table in table_ids:
                    relationships[table_name].append({
                        'column': field_name,
                        'ref_table': matched_table,
                        'ref_column': table_ids[matched_table]
                    })

                # 特殊处理常见的关联字段
                if not matched_table:
                    special_mappings = {
                        'user_id': 'user_info',
                        'order_id': 'order_info',
                        'sku_id': 'sku_info',
                        'spu_id': 'spu_info',
                        'category_id': 'category_info',
                        'brand_id': 'brand_info',
                        'product_id': 'product_info',
                        'parent_order_id': 'order_info'
                    }

                    if field_name in special_mappings:
                        ref_table = special_mappings[field_name]
                        # 检查这个表是否存在
                        if ref_table in table_ids:
                            relationships[table_name].append({
                                'column': field_name,
                                'ref_table': ref_table,
                                'ref_column': table_ids[ref_table]
                            })

    return relationships

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

    # 生成新的ID值
    if 'activity' in field_name:
        value = random.randint(100000, 999999)
    elif 'sku' in field_name:
        value = random.randint(1000000, 9999999)
    elif 'user' in field_name:
        value = random.randint(10000, 99999)
    elif 'store' in field_name:
        value = random.randint(1000, 9999)
    elif 'item' in field_name or 'product' in field_name:
        value = random.randint(100000, 999999)
    elif 'order' in field_name:
        value = random.randint(1000000, 9999999)
    else:
        value = random.randint(1, 1000000)

    # 添加到实体池
    ENTITY_POOLS[pool_key].append(value)
    # 限制池大小
    if len(ENTITY_POOLS[pool_key]) > 2000:
        ENTITY_POOLS[pool_key] = ENTITY_POOLS[pool_key][-1000:]

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
    elif field_name == 'expire_time' and 'create_time' in context:
        # 失效时间应该在创建时间之后
        create_time_str = context['create_time']
        create_time = datetime.strptime(create_time_str, "%Y-%m-%d %H:%M:%S")
        return get_random_time(context, days_range=30, after_time=create_time).strftime("%Y-%m-%d %H:%M:%S")
    elif field_name == 'refundable_time' and 'create_time' in context:
        # 可退款时间应该在创建时间之后30天
        create_time_str = context['create_time']
        create_time = datetime.strptime(create_time_str, "%Y-%m-%d %H:%M:%S")
        refund_time = create_time + timedelta(days=30)
        return refund_time.strftime("%Y-%m-%d %H:%M:%S")
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
        return random.randint(1, 10000)
    elif 'decimal' in field_type or 'float' in field_type or 'double' in field_type:
        # 浮点数类型字段生成浮点数值
        return round(random.uniform(0, 10000), 2)
    elif 'varchar' in field_type or 'char' in field_type or 'text' in field_type:
        # 字符串类型字段生成字符串值
        name = random.choice(REAL_USER_NAMES)
        pinyin = USER_NAME_PINYIN.get(name, name.lower())
        suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(3, 6)))
        value = f"{pinyin}_{suffix}"
        # 应用长度限制
        if field_length and len(str(value)) > field_length:
            value = str(value)[:field_length]
        return value
    elif 'date' in field_type:
        if 'birth' in field['name'].lower():
            return generate_birthday_value(field, context)
        else:
            return generate_date_value(field, context)
    else:
        # 默认生成字符串值
        name = random.choice(REAL_USER_NAMES)
        pinyin = USER_NAME_PINYIN.get(name, name.lower())
        suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(3, 6)))
        return f"{pinyin}_{suffix}"

# 智能生成字段值（基于字段语义和上下文）
def generate_smart_value(field, context=None):
    if context is None:
        context = {}

    field_name = field['name']
    field_type = field['type'].lower()
    field_semantic = field['semantic']
    field_length = field.get('length')

    # 处理外键关联字段
    # SPU与SKU关联
    if field_name == 'spu_id' and SPU_INFO_POOL:
        return random.choice(SPU_INFO_POOL)['id']

    # 订单与订单详情关联
    if field_name == 'order_id' and ORDER_INFO_POOL:
        return random.choice(ORDER_INFO_POOL)['id']

    # SKU与订单详情关联
    if field_name == 'sku_id' and SKU_INFO_POOL:
        selected_sku = random.choice(SKU_INFO_POOL)
        # 同步sku_name
        context['sku_name'] = selected_sku['sku_name']
        return selected_sku['id']

    # 用户与订单关联
    if field_name == 'user_id' and USER_INFO_POOL:
        return random.choice(USER_INFO_POOL)['id']

    # 父订单ID关联
    if field_name == 'parent_order_id' and ORDER_INFO_POOL:
        # 90%概率返回None，10%概率返回一个已存在的订单ID
        if random.random() < 0.9:
            return None
        else:
            return random.choice(ORDER_INFO_POOL)['id']

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
                    value = random.randint(1, 10000000)
            else:
                if field.get('is_numeric_id', False):
                    value = generate_id_value(field, context)
                else:
                    value = random.randint(1, 100000)

        elif 'decimal' in field_type or 'float' in field_type or 'double' in field_type:
            precision = field.get('precision')
            if precision:
                precision_val, scale = precision
                max_val = 10 ** (precision_val - scale) - 1
                value = round(random.uniform(0, min(max_val, 100000)), scale)
            else:
                value = round(random.uniform(0, 10000), 2)

        elif 'varchar' in field_type or 'char' in field_type or 'text' in field_type:
            # 默认生成通用字符串
            name = random.choice(REAL_USER_NAMES)
            pinyin = USER_NAME_PINYIN.get(name, name.lower())
            suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(3, 6)))
            value = f"{pinyin}_{suffix}"

            # 应用长度限制
            if field_length and len(str(value)) > field_length:
                value = str(value)[:field_length]

        elif 'datetime' in field_type or 'timestamp' in field_type:
            value = generate_datetime_value(field, context)

        elif 'date' in field_type:
            if 'birth' in field_name:
                value = generate_birthday_value(field, context)
            else:
                value = generate_date_value(field, context)

        else:
            name = random.choice(REAL_USER_NAMES)
            pinyin = USER_NAME_PINYIN.get(name, name.lower())
            suffix = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=random.randint(3, 6)))
            value = f"{pinyin}_{suffix}"

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
        release_db_connection(connection)

# 智能生成并插入数据批次
def generate_and_insert_data_batch(table_name, fields, batch_num, batch_size, start_id):
    global SPU_INFO_POOL, USER_INFO_POOL, ORDER_INFO_POOL, SKU_INFO_POOL

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

        # 保存关联数据到对应的池中
        record_dict = dict(zip(field_names, record_data))
        if table_name == 'spu_info':
            SPU_INFO_POOL.append(record_dict)
            # 限制池大小
            if len(SPU_INFO_POOL) > 1000:
                SPU_INFO_POOL = SPU_INFO_POOL[-500:]
        elif table_name == 'user_info':
            USER_INFO_POOL.append(record_dict)
            # 限制池大小
            if len(USER_INFO_POOL) > 1000:
                USER_INFO_POOL = USER_INFO_POOL[-500:]
        elif table_name == 'order_info':
            ORDER_INFO_POOL.append(record_dict)
            # 限制池大小
            if len(ORDER_INFO_POOL) > 1000:
                ORDER_INFO_POOL = ORDER_INFO_POOL[-500:]
        elif table_name == 'sku_info':
            SKU_INFO_POOL.append(record_dict)
            # 限制池大小
            if len(SKU_INFO_POOL) > 1000:
                SKU_INFO_POOL = SKU_INFO_POOL[-500:]

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
        release_db_connection(connection)

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
        release_db_connection(connection)

# 主函数
def main():
    global SPU_INFO_POOL, USER_INFO_POOL, ORDER_INFO_POOL, SKU_INFO_POOL

    try:
        print(f"开始处理数据库: {MYSQL_DB}")
        print(f"目标总记录数: {TOTAL_RECORDS}")
        print(f"每批次记录数: {BATCH_SIZE}")
        print(f"时间范围: {DAY_TIME_RANGE} 天")

        # 清空关联池
        SPU_INFO_POOL.clear()
        USER_INFO_POOL.clear()
        ORDER_INFO_POOL.clear()
        SKU_INFO_POOL.clear()

        # 存储所有表名和字段信息，用于推断关系
        all_tables_info = {}

        for i, create_sql in enumerate(CREATE_TABLE_SQL_LIST):
            print(f"\n========== 处理第 {i + 1} 个表 ==========")
            table_name, fields = parse_create_table_sql(create_sql)
            all_tables_info[table_name] = fields
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
                time.sleep(0.01)  # 减少延迟

            print(f"\n表 {table_name} 数据生成完成，共 {TOTAL_RECORDS} 条记录")
            show_sample_data(table_name)

        print(f"\n所有表数据生成完成")
        print("实体池统计:")
        for pool_name, pool_data in ENTITY_POOLS.items():
            print(f"  {pool_name}: {len(pool_data)} 个实体")

        print("\n表关系映射:")
        # 首先显示从SQL中提取的外键关系
        has_fk_relationships = False
        for table_name, relationships in TABLE_RELATIONSHIPS.items():
            if relationships:
                has_fk_relationships = True
                print(f"  {table_name}:")
                for rel in relationships:
                    print(f"    {rel['column']} -> {rel['ref_table']}.{rel['ref_column']}")

        # 如果没有从SQL中提取到外键关系，则使用自动推断的关系
        if not has_fk_relationships:
            print("(未从SQL中检测到外键约束，以下为基于字段名自动推断的关系)")
            inferred_relationships = infer_table_relationships(all_tables_info)
            for table_name, relationships in inferred_relationships.items():
                if relationships:
                    print(f"  {table_name}:")
                    for rel in relationships:
                        print(f"    {rel['column']} -> {rel['ref_table']}.{rel['ref_column']}")

    except Exception as e:
        print(f"执行过程出错：{str(e)}")
        raise
    finally:
        # 关闭所有数据库连接
        db_pool.close_all()

if __name__ == "__main__":
    main()
