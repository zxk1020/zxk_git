
CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_a_template_city_distance (
    `id` BIGINT,
    `city_no1` BIGINT,
    `city_no2` BIGINT,
    `distance` BIGINT,
    `remark` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_a_template_city_distance/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_complex (
    `id` BIGINT,
    `complex_name` STRING,
    `province_id` BIGINT,
    `city_id` BIGINT,
    `district_id` BIGINT,
    `district_name` STRING,
    `create_time` STRING,
    `update_time` STRING,
    `is_deleted` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_complex/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_dic (
    `id` BIGINT COMMENT 'id',
    `parent_id` BIGINT COMMENT '上级id',
    `name` STRING COMMENT '名称',
    `dict_code` STRING COMMENT '编码',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` INT COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '数据字典'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_dic/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_organ (
    `id` BIGINT,
    `org_name` STRING COMMENT '机构名称',
    `org_level` BIGINT COMMENT '行政级别',
    `region_id` BIGINT COMMENT '区域id，1级机构为city ,2级机构为district',
    `org_parent_id` BIGINT COMMENT '上级机构id',
    `points` STRING COMMENT '多边形经纬度坐标集合',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '机构范围表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_organ/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_province (
    `id` BIGINT COMMENT 'id',
    `name` STRING COMMENT '省名称',
    `region_id` STRING COMMENT '大区id',
    `area_code` STRING COMMENT '行政区位码',
    `iso_code` STRING COMMENT '国际编码',
    `iso_3166_2` STRING COMMENT 'ISO3166编码',
    `create_time` STRING,
    `operate_time` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_province/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_base_region_info (
    `id` BIGINT COMMENT 'id',
    `parent_id` BIGINT COMMENT '上级id',
    `name` STRING COMMENT '名称',
    `dict_code` STRING COMMENT '编码',
    `short_name` STRING COMMENT '简称',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` INT COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '数据字典'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_base_region_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_employee_info (
    `id` BIGINT COMMENT 'id',
    `username` STRING COMMENT '名称',
    `password` STRING COMMENT '密码',
    `real_name` STRING COMMENT '姓名',
    `id_card` STRING COMMENT '身份证号',
    `phone` STRING COMMENT '手机',
    `birthday` STRING COMMENT '生日',
    `gender` STRING COMMENT '性别',
    `address` STRING COMMENT '地址',
    `employment_date` STRING COMMENT '入职日期',
    `graduation_date` STRING COMMENT '毕业时间',
    `education` STRING COMMENT '学历',
    `position_type` STRING COMMENT '岗位类别',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '职员表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_employee_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_express_courier (
    `id` BIGINT,
    `emp_id` BIGINT COMMENT '职员id',
    `org_id` BIGINT COMMENT '机构id',
    `working_phone` STRING COMMENT '工作电话',
    `express_type` STRING COMMENT '快递类型',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '快递员表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_express_courier/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_express_courier_complex (
    `id` BIGINT,
    `courier_emp_id` BIGINT COMMENT '快递员id',
    `complex_id` BIGINT COMMENT '小区',
    `create_time` STRING,
    `update_time` STRING,
    `is_deleted` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_express_courier_complex/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_express_task_collect (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '订单id',
    `status` STRING COMMENT '任务状态，1为待执行（对应 待上门和须交接）、2 进行中 3已揽收 ， 4为已完成、5为已取消  ',
    `org_id` BIGINT COMMENT '网点ID',
    `org_name` STRING COMMENT '网点名称',
    `courier_emp_id` BIGINT COMMENT '快递员ID',
    `courier_name` STRING COMMENT '快递员名称',
    `estimated_collected_time` STRING COMMENT '预计揽收时间',
    `estimated_commit_time` STRING COMMENT '预计揽收时间',
    `actual_collected_time` STRING COMMENT '实际揽收时间',
    `actual_commit_time` STRING COMMENT '实际提交时间',
    `cancel_time` STRING COMMENT '取消时间',
    `remark` STRING COMMENT '备注',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '取件、派件任务信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_express_task_collect/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_express_task_delivery (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '订单id',
    `status` STRING COMMENT '任务状态',
    `org_id` BIGINT COMMENT '网点ID',
    `org_name` STRING COMMENT '网点名称',
    `courier_emp_id` BIGINT COMMENT '快递员ID',
    `courier_name` STRING COMMENT '快递员名称',
    `estimated_end_time` STRING COMMENT '预计完成时间',
    `start_delivery_time` STRING COMMENT '实际开始时间',
    `delivered_time` STRING COMMENT '完成时间',
    `is_rejected` STRING COMMENT '是否拒收运货',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '取件、派件任务信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_express_task_delivery/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_line_base_info (
    `id` BIGINT,
    `name` STRING,
    `line_no` STRING COMMENT '线路编号',
    `line_level` STRING COMMENT '级别',
    `org_id` BIGINT COMMENT '所属机构',
    `transport_line_type_id` STRING COMMENT '线路类型',
    `start_org_id` BIGINT COMMENT '起始地机构id',
    `start_org_name` STRING,
    `end_org_id` BIGINT,
    `end_org_name` STRING,
    `pair_line_id` BIGINT COMMENT '一来一回的路线',
    `distance` DECIMAL(10,2),
    `cost` DECIMAL(10,2),
    `estimated_time` INT COMMENT '预计时间（分钟）',
    `status` STRING COMMENT '状态 0：禁用 1：正常',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '线路基本表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_line_base_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_line_base_shift (
    `id` BIGINT,
    `line_id` BIGINT COMMENT '线路id',
    `start_time` STRING COMMENT '班次开始时间',
    `driver1_emp_id` BIGINT COMMENT '第一司机',
    `driver2_emp_id` BIGINT COMMENT '第二司机',
    `truck_id` BIGINT COMMENT '卡车',
    `pair_shift_id` BIGINT COMMENT '配对班次(同一辆车一去一回的另一班次)',
    `is_enabled` STRING COMMENT '状态 0：禁用 1：正常',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '线路基本表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_line_base_shift/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_order_cargo (
    `id` BIGINT,
    `order_id` STRING COMMENT '订单id',
    `cargo_type` STRING COMMENT '货物类型id',
    `volume_length` INT COMMENT '长cm',
    `volume_width` INT COMMENT '宽cm',
    `volume_height` INT COMMENT '高cm',
    `weight` DECIMAL(16,2) COMMENT '重量 kg',
    `remark` STRING COMMENT '备注',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '订单明细'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_order_cargo/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_order_info (
    `id` BIGINT COMMENT 'id',
    `order_no` STRING COMMENT '订单号',
    `status` STRING COMMENT '订单状态',
    `collect_type` STRING COMMENT '取件类型，1为网点自寄，2为上门取件',
    `user_id` BIGINT COMMENT '客户id',
    `receiver_complex_id` BIGINT COMMENT '收件人小区id',
    `receiver_province_id` STRING COMMENT '收件人省份id',
    `receiver_city_id` STRING COMMENT '收件人城市id',
    `receiver_district_id` STRING COMMENT '收件人区县id',
    `receiver_address` STRING COMMENT '收件人详细地址',
    `receiver_name` STRING COMMENT '收件人姓名',
    `receiver_phone` STRING COMMENT '收件人电话',
    `receive_location` STRING COMMENT '起始点经纬度',
    `sender_complex_id` BIGINT COMMENT '发件人小区id',
    `sender_province_id` STRING COMMENT '发件人省份id',
    `sender_city_id` STRING COMMENT '发件人城市id',
    `sender_district_id` STRING COMMENT '发件人区县id',
    `sender_address` STRING COMMENT '发件人详细地址',
    `sender_name` STRING COMMENT '发件人姓名',
    `sender_phone` STRING COMMENT '发件人电话',
    `send_location` STRING COMMENT '发件人坐标',
    `payment_type` STRING COMMENT '支付方式',
    `cargo_num` INT COMMENT '货物个数',
    `amount` DECIMAL(32,2) COMMENT '金额',
    `estimate_arrive_time` STRING COMMENT '预计到达时间',
    `distance` DECIMAL(10,2) COMMENT '距离，单位：公里',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '物流单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_order_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_order_org_bound (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '订单主键',
    `org_id` BIGINT COMMENT '机构id',
    `status` STRING COMMENT '状态 出库 入库',
    `inbound_time` STRING COMMENT '入库时间',
    `inbound_emp_id` BIGINT COMMENT '入库人员',
    `sort_time` STRING COMMENT '分拣时间',
    `sorter_emp_id` BIGINT COMMENT '分拣人员',
    `outbound_time` STRING COMMENT '出库时间',
    `outbound_emp_id` BIGINT COMMENT '出库人员',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '修改时间',
    `is_deleted` STRING COMMENT '删除标志'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_order_org_bound/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_order_trace_log (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '订单编号',
    `trace_desc` STRING COMMENT '进度描述',
    `create_time` STRING COMMENT '创建时间',
    `cur_task_id` BIGINT COMMENT '来源快递任务id',
    `task_type` STRING COMMENT '任务类型:1  揽收 2 运输 3 中转 4派送 ',
    `charge_emp_id` BIGINT COMMENT '负责人id',
    `remark` STRING COMMENT '备注',
    `is_rollback` STRING COMMENT '是否回退中',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '物流单追踪信息'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_order_trace_log/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_sorter_info (
    `id` BIGINT,
    `emp_id` BIGINT COMMENT '职员id',
    `org_id` BIGINT COMMENT '机构id',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '分拣员'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_sorter_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_transport_plan_line_detail (
    `id` BIGINT,
    `order_id` BIGINT COMMENT '订单编号',
    `start_org_id` BIGINT COMMENT '起始机构',
    `end_org_id` BIGINT COMMENT '终点机构',
    `line_base_id` BIGINT COMMENT '业务表线路id',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '运单路线明细表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_transport_plan_line_detail/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_transport_task (
    `id` BIGINT,
    `shift_id` BIGINT COMMENT '车次id',
    `line_id` BIGINT COMMENT '路线id',
    `start_org_id` BIGINT COMMENT '起始机构id',
    `start_org_name` STRING COMMENT '起始机构名称',
    `end_org_id` BIGINT COMMENT '目的机构id',
    `end_org_name` STRING COMMENT '目的机构名称',
    `status` STRING COMMENT '任务状态',
    `order_num` INT COMMENT '运单个数',
    `driver1_emp_id` BIGINT COMMENT '司机1id',
    `driver1_name` STRING COMMENT '司机1名称',
    `driver2_emp_id` BIGINT COMMENT '司机2id',
    `driver2_name` STRING COMMENT '司机2名称',
    `truck_id` BIGINT COMMENT '卡车id',
    `truck_no` STRING COMMENT '卡车号牌',
    `actual_start_time` STRING COMMENT '实际启动时间',
    `actual_end_time` STRING COMMENT '实际到达时间',
    `actual_distance` DECIMAL(16,2) COMMENT '实际行驶距离',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '运输任务表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_transport_task/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_transport_task_detail (
    `id` BIGINT,
    `transport_task_id` BIGINT COMMENT '运输任务id',
    `order_id` BIGINT COMMENT '运单id',
    `sorter_emp_id` BIGINT COMMENT '分拣员id',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '运单与运输任务关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_transport_task_detail/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_transport_task_process (
    `id` BIGINT,
    `transport_task_id` BIGINT COMMENT '任务id',
    `cur_distance` DECIMAL(16,2) COMMENT '当前行驶里程',
    `line_distance` DECIMAL(16,2) COMMENT '里程',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_transport_task_process/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_truck_driver (
    `id` BIGINT,
    `emp_id` BIGINT COMMENT '雇员编号',
    `org_id` BIGINT COMMENT '机构号',
    `team_id` BIGINT COMMENT '车队号',
    `license_type` STRING COMMENT '准驾车型',
    `init_license_date` STRING COMMENT '初次领证日期',
    `expire_date` STRING COMMENT '有效截止日期',
    `license_no` STRING COMMENT '驾驶证号',
    `license_picture_url` STRING COMMENT '驾照图片',
    `is_enabled` INT COMMENT '状态 0：禁用 1：正常',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '司机驾驶证表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_truck_driver/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_truck_info (
    `id` BIGINT COMMENT 'id',
    `team_id` BIGINT COMMENT '所属车队id',
    `truck_no` STRING COMMENT '车牌号码',
    `truck_model_id` STRING COMMENT '型号',
    `device_gps_id` STRING COMMENT 'GPS设备id',
    `engine_no` STRING COMMENT '发动机编码',
    `license_registration_date` STRING COMMENT '注册时间',
    `license_last_check_date` STRING COMMENT '最后年检日期',
    `license_expire_date` STRING COMMENT '失效事件日期',
    `picture_url` STRING COMMENT '图片信息',
    `is_enabled` INT COMMENT '状态 0：禁用 1：正常',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '车辆信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_truck_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_truck_model (
    `id` BIGINT,
    `model_name` STRING COMMENT '型号名称',
    `model_type` STRING COMMENT '类型',
    `model_no` STRING COMMENT '型号编码',
    `brand` STRING COMMENT '品牌',
    `truck_weight` DECIMAL(10,2) COMMENT '整车重量 吨',
    `load_weight` DECIMAL(10,2) COMMENT '额定载重 吨',
    `total_weight` DECIMAL(10,2) COMMENT '总质量 吨',
    `eev` STRING COMMENT '排放标准',
    `boxcar_len` DECIMAL(10,2) COMMENT '货箱长m',
    `boxcar_wd` DECIMAL(10,2) COMMENT '货箱宽m',
    `boxcar_hg` DECIMAL(10,2) COMMENT '货箱高m',
    `max_speed` BIGINT COMMENT '最高时速 千米每时',
    `oil_vol` BIGINT COMMENT '油箱容积 升',
    `create_time` STRING,
    `update_time` STRING,
    `is_deleted` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_truck_model/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_truck_team (
    `id` BIGINT COMMENT 'id',
    `name` STRING COMMENT '车队名称',
    `team_no` STRING COMMENT '车队编号',
    `org_id` BIGINT COMMENT '所属机构',
    `manager_emp_id` BIGINT COMMENT '负责人',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '车队表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_truck_team/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_user_address (
    `id` BIGINT COMMENT 'id',
    `user_id` BIGINT COMMENT '用户id',
    `phone` STRING COMMENT '电话号',
    `province_id` BIGINT COMMENT '所属省份id',
    `city_id` BIGINT COMMENT '所属城市id',
    `district_id` BIGINT COMMENT '所属区县id',
    `complex_id` BIGINT COMMENT '所属小区id',
    `address` STRING COMMENT '详细地址',
    `is_default` INT COMMENT '是否默认 0:否，1:是',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '更新时间',
    `is_deleted` STRING COMMENT '删除标记（0:不可用 1:可用）'
) COMMENT '地址簿'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_user_address/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')



CREATE EXTERNAL TABLE IF NOT EXISTS tms01.ods_user_info (
    `id` BIGINT COMMENT '编号',
    `login_name` STRING COMMENT '用户名称',
    `nick_name` STRING COMMENT '用户昵称',
    `passwd` STRING COMMENT '用户密码',
    `real_name` STRING COMMENT '用户姓名',
    `phone_num` STRING COMMENT '手机号',
    `email` STRING COMMENT '邮箱',
    `head_img` STRING COMMENT '头像',
    `user_level` STRING COMMENT '用户级别',
    `birthday` STRING COMMENT '用户生日',
    `gender` STRING COMMENT '性别 M男,F女',
    `create_time` STRING COMMENT '创建时间',
    `update_time` STRING COMMENT '修改时间',
    `is_deleted` STRING COMMENT '状态'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/tms01/ods/ods_user_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec')
