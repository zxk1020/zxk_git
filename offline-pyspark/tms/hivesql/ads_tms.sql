set hive.exec.mode.local.auto=True;
SET hive.exec.parallel=true; -- 允许并行执行UNION等操作
SET hive.exec.parallel.thread.number=2; -- 并行线程数
SET hive.exec.dynamic.partition.mode=nonstrict;
use tms01;


drop table if exists ads_trans_order_stats;
create external table ads_trans_order_stats
(
    `dt`                    string COMMENT '统计日期',
    `recent_days`           tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `receive_order_count`   bigint COMMENT '接单总数',
    `receive_order_amount`  decimal(16, 2) COMMENT '接单金额',
    `dispatch_order_count`  bigint COMMENT '发单总数',
    `dispatch_order_amount` decimal(16, 2) COMMENT '发单金额'
) comment '运单相关统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_order_stats';

insert overwrite table ads_trans_order_stats
select dt,
       recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from ads_trans_order_stats
union
select '20250713'                                           dt,
       nvl(receive_1d.recent_days, dispatch_1d.recent_days) recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from (select 1                 recent_days,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '2025-07-11') receive_1d
         full outer join
     (select 1            recent_days,
             order_count  dispatch_order_count,
             order_amount dispatch_order_amount
      from dws_trans_dispatch_1d
      where dt = '2025-07-11') dispatch_1d
     on receive_1d.recent_days = dispatch_1d.recent_days
union
select '20250713'                                           dt,
       nvl(receive_nd.recent_days, dispatch_nd.recent_days) recent_days,
       receive_order_count,
       receive_order_amount,
       dispatch_order_count,
       dispatch_order_amount
from (select recent_days,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '2023-01-10'
      group by recent_days) receive_nd
         full outer join
     (select recent_days,
             order_count  dispatch_order_count,
             order_amount dispatch_order_amount
      from dws_trans_dispatch_nd
      where dt = '20250713') dispatch_nd
     on receive_nd.recent_days = dispatch_nd.recent_days;

select *
from ads_trans_order_stats;


drop table if exists ads_trans_stats;
create external table ads_trans_stats
(
    `dt`                    string COMMENT '统计日期',
    `recent_days`           tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `trans_finish_count`    bigint COMMENT '完成运输次数',
    `trans_finish_distance` decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`  bigint COMMENT ' 完成运输时长，单位：秒'
) comment '运输综合统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_stats';

insert overwrite table ads_trans_stats
select dt,
       recent_days,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec
from ads_trans_stats
union
select '20250713'                 dt,
       1                          recent_days,
       sum(trans_finish_count)    trans_finish_count,
       sum(trans_finish_distance) trans_finish_distance,
       sum(trans_finish_dur_sec)  trans_finish_dur_sec
from dws_trans_org_truck_model_type_trans_finish_1d
where dt = '20250713'
union
select '20250713'                 dt,
       recent_days,
       sum(trans_finish_count)    trans_finish_count,
       sum(trans_finish_distance) trans_finish_distance,
       sum(trans_finish_dur_sec)  trans_finish_dur_sec
from dws_trans_shift_trans_finish_nd
where dt = '20250713'
group by recent_days;

select *
from ads_trans_stats;


drop table if exists ads_trans_order_stats_td;
create external table ads_trans_order_stats_td
(
    `dt`                    string COMMENT '统计日期',
    `bounding_order_count`  bigint COMMENT '运输中运单总数',
    `bounding_order_amount` decimal(16, 2) COMMENT '运输中运单金额'
) comment '历史至今运单统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_trans_order_stats_td';


insert overwrite table ads_trans_order_stats_td
select dt,
       bounding_order_count,
       bounding_order_amount
from ads_trans_order_stats_td
union
select dt,
       sum(order_count)  bounding_order_count,
       sum(order_amount) bounding_order_amount
from (select dt,
             order_count,
             order_amount
      from dws_trans_dispatch_td
      where dt = '20250713'
      union
      select dt,
             order_count * (-1),
             order_amount * (-1)
      from dws_trans_bound_finish_td
      where dt = '20250713') new
group by dt;

select *
from ads_trans_order_stats_td;


drop table if exists ads_order_stats;
create external table ads_order_stats
(
    `dt`           string COMMENT '统计日期',
    `recent_days`  tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `order_count`  bigint COMMENT '下单数',
    `order_amount` decimal(16, 2) COMMENT '下单金额'
) comment '运单综合统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_order_stats';


insert overwrite table ads_order_stats
select dt,
       recent_days,
       order_count,
       order_amount
from ads_order_stats
union
select '20250713'        dt,
       1                 recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d
where dt = '2025-07-11'
union
select '20250713'        dt,
       recent_days,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_nd
where dt = '20250713'
group by recent_days;

select *
from ads_order_stats;


drop table if exists ads_order_cargo_type_stats;
create external table ads_order_cargo_type_stats
(
    `dt`              string COMMENT '统计日期',
    `recent_days`     tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `cargo_type`      string COMMENT '货物类型',
    `cargo_type_name` string COMMENT '货物类型名称',
    `order_count`     bigint COMMENT '下单数',
    `order_amount`    decimal(16, 2) COMMENT '下单金额'
) comment '各类型货物运单统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_order_cargo_type_stats';

insert overwrite table ads_order_cargo_type_stats
select dt,
       recent_days,
       cargo_type,
       cargo_type_name,
       order_count,
       order_amount
from ads_order_cargo_type_stats
union
select '20250713'        dt,
       1                 recent_days,
       cargo_type,
       cargo_type_name,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_1d
where dt = '2025-07-11'
group by cargo_type,
         cargo_type_name
union
select '20250713'        dt,
       recent_days,
       cargo_type,
       cargo_type_name,
       sum(order_count)  order_count,
       sum(order_amount) order_amount
from dws_trade_org_cargo_type_order_nd
where dt = '20250713'
group by cargo_type,
         cargo_type_name,
         recent_days;

select *
from ads_order_cargo_type_stats;


drop table if exists ads_city_stats;
create external table ads_city_stats
(
    `dt`                        string COMMENT '统计日期',
    `recent_days`               bigint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `city_id`                   bigint COMMENT '城市ID',
    `city_name`                 string COMMENT '城市名称',
    `order_count`               bigint COMMENT '下单数',
    `order_amount`              decimal COMMENT '下单金额',
    `trans_finish_count`        bigint COMMENT '完成运输次数',
    `trans_finish_distance`     decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`      bigint COMMENT '完成运输时长，单位：秒',
    `avg_trans_finish_distance` decimal(16, 2) COMMENT '平均每次运输里程',
    `avg_trans_finish_dur_sec`  bigint COMMENT '平均每次运输时长，单位：秒'
) comment '城市分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_city_stats';

insert overwrite table ads_city_stats
select dt,
       recent_days,
       city_id,
       city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_city_stats
union
select nvl(city_order_1d.dt, city_trans_1d.dt)                   dt,
       nvl(city_order_1d.recent_days, city_trans_1d.recent_days) recent_days,
       nvl(city_order_1d.city_id, city_trans_1d.city_id)         city_id,
       nvl(city_order_1d.city_name, city_trans_1d.city_name)     city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '20250713'        dt,
             1                 recent_days,
             city_id,
             city_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_1d
      where dt = '2025-07-11'
      group by city_id,
               city_name) city_order_1d
         full outer join
     (select '20250713'                                           dt,
             1                                                    recent_days,
             city_id,
             city_name,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from (select if(org_level = 1, city_for_level1.id, city_for_level2.id)     city_id,
                   if(org_level = 1, city_for_level1.name, city_for_level2.name) city_name,
                   trans_finish_count,
                   trans_finish_distance,
                   trans_finish_dur_sec
            from (select org_id,
                         trans_finish_count,
                         trans_finish_distance,
                         trans_finish_dur_sec
                  from dws_trans_org_truck_model_type_trans_finish_1d
                  where dt = '20250713') trans_origin
                     left join
                 (select id,
                         org_level,
                         region_id
                  from dim_organ_full
                  where dt = '2023-01-10') organ
                 on org_id = organ.id
                     left join
                 (select id,
                         name,
                         parent_id
                  from dim_region_full
                  where dt = '20200623') city_for_level1
                 on region_id = city_for_level1.id
                     left join
                 (select id,
                         name
                  from dim_region_full
                  where dt = '20200623') city_for_level2
                 on city_for_level1.parent_id = city_for_level2.id) trans_1d
      group by city_id,
               city_name) city_trans_1d
     on city_order_1d.dt = city_trans_1d.dt
         and city_order_1d.recent_days = city_trans_1d.recent_days
         and city_order_1d.city_id = city_trans_1d.city_id
         and city_order_1d.city_name = city_trans_1d.city_name
union
select nvl(city_order_nd.dt, city_trans_nd.dt)                   dt,
       nvl(city_order_nd.recent_days, city_trans_nd.recent_days) recent_days,
       nvl(city_order_nd.city_id, city_trans_nd.city_id)         city_id,
       nvl(city_order_nd.city_name, city_trans_nd.city_name)     city_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '20250713'        dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_nd
      where dt = '20250713'
      group by city_id,
               city_name,
               recent_days) city_order_nd
         full outer join
     (select '20250713'                                           dt,
             city_id,
             city_name,
             recent_days,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from dws_trans_shift_trans_finish_nd
      where dt = '20250713'
      group by city_id,
               city_name,
               recent_days) city_trans_nd
     on city_order_nd.dt = city_trans_nd.dt
         and city_order_nd.recent_days = city_trans_nd.recent_days
         and city_order_nd.city_id = city_trans_nd.city_id
         and city_order_nd.city_name = city_trans_nd.city_name;

select *
from ads_city_stats;


drop table if exists ads_org_stats;
create external table ads_org_stats
(
    `dt`                        string COMMENT '统计日期',
    `recent_days`               tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `org_id`                    bigint COMMENT '机构ID',
    `org_name`                  string COMMENT '机构名称',
    `order_count`               bigint COMMENT '下单数',
    `order_amount`              decimal COMMENT '下单金额',
    `trans_finish_count`        bigint COMMENT '完成运输次数',
    `trans_finish_distance`     decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`      bigint COMMENT '完成运输时长，单位：秒',
    `avg_trans_finish_distance` decimal(16, 2) COMMENT '平均每次运输里程',
    `avg_trans_finish_dur_sec`  bigint COMMENT '平均每次运输时长，单位：秒'
) comment '机构分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_org_stats';


insert overwrite table ads_org_stats
select dt,
       recent_days,
       org_id,
       org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_org_stats
union
select nvl(org_order_1d.dt, org_trans_1d.dt)                   dt,
       nvl(org_order_1d.recent_days, org_trans_1d.recent_days) recent_days,
       nvl(org_order_1d.org_id, org_trans_1d.org_id)           org_id,
       nvl(org_order_1d.org_name, org_trans_1d.org_name)       org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '20250713'        dt,
             1                 recent_days,
             org_id,
             org_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_1d
      where dt = '2025-07-11'
      group by org_id,
               org_name) org_order_1d
         full outer join
     (select '20250713'                                           dt,
             org_id,
             org_name,
             1                                                    recent_days,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from dws_trans_org_truck_model_type_trans_finish_1d
      where dt = '20250713'
      group by org_id,
               org_name) org_trans_1d
     on org_order_1d.dt = org_trans_1d.dt
         and org_order_1d.recent_days = org_trans_1d.recent_days
         and org_order_1d.org_id = org_trans_1d.org_id
         and org_order_1d.org_name = org_trans_1d.org_name
union
select org_order_nd.dt,
       org_order_nd.recent_days,
       org_order_nd.org_id,
       org_order_nd.org_name,
       order_count,
       order_amount,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from (select '20250713'        dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count)  order_count,
             sum(order_amount) order_amount
      from dws_trade_org_cargo_type_order_nd
      where dt = '20250713'
      group by org_id,
               org_name,
               recent_days) org_order_nd
         join
     (select '20250713'                                           dt,
             recent_days,
             org_id,
             org_name,
             sum(trans_finish_count)                              trans_finish_count,
             sum(trans_finish_distance)                           trans_finish_distance,
             sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
             sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
             sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
      from dws_trans_shift_trans_finish_nd
      where dt = '20250713'
      group by org_id,
               org_name,
               recent_days) org_trans_nd
     on org_order_nd.dt = org_trans_nd.dt
         and org_order_nd.recent_days = org_trans_nd.recent_days
         and org_order_nd.org_id = org_trans_nd.org_id
         and org_order_nd.org_name = org_trans_nd.org_name;


select *
from ads_org_stats;

drop table if exists ads_shift_stats;
create external table ads_shift_stats
(
    `dt`                       string COMMENT '统计日期',
    `recent_days`              tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    `shift_id`                 bigint COMMENT '班次ID',
    `trans_finish_count`       bigint COMMENT '完成运输次数',
    `trans_finish_distance`    decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`     bigint COMMENT '完成运输时长，单位：秒',
    `trans_finish_order_count` bigint COMMENT '运输完成运单数'
) comment '班次分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_shift_stats';


insert overwrite table ads_shift_stats
select dt,
       recent_days,
       shift_id,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from ads_shift_stats
union
select '20250713' dt,
       recent_days,
       shift_id,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from dws_trans_shift_trans_finish_nd
where dt = '20250713';

select *
from ads_shift_stats;

drop table if exists ads_line_stats;
create external table ads_line_stats
(
    `dt`                       string COMMENT '统计日期',
    `recent_days`              tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    `line_id`                  bigint COMMENT '线路ID',
    `line_name`                string COMMENT '线路名称',
    `trans_finish_count`       bigint COMMENT '完成运输次数',
    `trans_finish_distance`    decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`     bigint COMMENT '完成运输时长，单位：秒',
    `trans_finish_order_count` bigint COMMENT '运输完成运单数'
) comment '线路分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_line_stats';


insert overwrite table ads_line_stats
select dt,
       recent_days,
       line_id,
       line_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       trans_finish_order_count
from ads_line_stats
union
select '20250713'                    dt,
       recent_days,
       line_id,
       line_name,
       sum(trans_finish_count)       trans_finish_count,
       sum(trans_finish_distance)    trans_finish_distance,
       sum(trans_finish_dur_sec)     trans_finish_dur_sec,
       sum(trans_finish_order_count) trans_finish_order_count
from dws_trans_shift_trans_finish_nd
where dt = '20250713'
group by line_id,
         line_name,
         recent_days;

select *
from ads_line_stats;


drop table if exists ads_driver_stats;
create external table ads_driver_stats
(
    `dt`                        string COMMENT '统计日期',
    `recent_days`               tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
    `driver_emp_id`             bigint comment '第一司机员工ID',
    `driver_name`               string comment '第一司机姓名',
    `trans_finish_count`        bigint COMMENT '完成运输次数',
    `trans_finish_distance`     decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`      bigint COMMENT '完成运输时长，单位：秒',
    `avg_trans_finish_distance` decimal(16, 2) COMMENT '平均每次运输里程',
    `avg_trans_finish_dur_sec`  bigint COMMENT '平均每次运输时长，单位：秒'
) comment '司机分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_driver_stats';


insert overwrite table ads_driver_stats
select dt,
       recent_days,
       driver_emp_id,
       driver_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_driver_stats
union
select '20250713'                                           dt,
       recent_days,
       driver_id,
       driver_name,
       sum(trans_finish_count)                              trans_finish_count,
       sum(trans_finish_distance)                           trans_finish_distance,
       sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
from (select recent_days,
             driver1_emp_id driver_id,
             driver1_name   driver_name,
             trans_finish_count,
             trans_finish_distance,
             trans_finish_dur_sec
      from dws_trans_shift_trans_finish_nd
      where dt = '20250713'
        and driver2_emp_id is null
      union
      select recent_days,
             cast(driver_info[0] as bigint) driver_id,
             driver_info[1]                 driver_name,
             trans_finish_count,
             trans_finish_distance,
             trans_finish_dur_sec
      from (select recent_days,
                   array(array(driver1_emp_id, driver1_name),
                         array(driver2_emp_id, driver2_name)) driver_arr,
                   trans_finish_count,
                   trans_finish_distance / 2                  trans_finish_distance,
                   trans_finish_dur_sec / 2                   trans_finish_dur_sec
            from dws_trans_shift_trans_finish_nd
            where dt = '20250713'
              and driver2_emp_id is not null) t1
               lateral view explode(driver_arr) tmp as driver_info) t2
group by driver_id,
         driver_name,
         recent_days;

select *
from ads_driver_stats;


drop table if exists ads_truck_stats;
create external table ads_truck_stats
(
    `dt`                        string COMMENT '统计日期',
    `recent_days`               tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `truck_model_type`          string COMMENT '卡车类别编码',
    `truck_model_type_name`     string COMMENT '卡车类别名称',
    `trans_finish_count`        bigint COMMENT '完成运输次数',
    `trans_finish_distance`     decimal(16, 2) COMMENT '完成运输里程',
    `trans_finish_dur_sec`      bigint COMMENT '完成运输时长，单位：秒',
    `avg_trans_finish_distance` decimal(16, 2) COMMENT '平均每次运输里程',
    `avg_trans_finish_dur_sec`  bigint COMMENT '平均每次运输时长，单位：秒'
) comment '卡车分析'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_truck_stats';


insert overwrite table ads_truck_stats
select dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       trans_finish_count,
       trans_finish_distance,
       trans_finish_dur_sec,
       avg_trans_finish_distance,
       avg_trans_finish_dur_sec
from ads_truck_stats
union
select '20250713'                                           dt,
       recent_days,
       truck_model_type,
       truck_model_type_name,
       sum(trans_finish_count)                              trans_finish_count,
       sum(trans_finish_distance)                           trans_finish_distance,
       sum(trans_finish_dur_sec)                            trans_finish_dur_sec,
       sum(trans_finish_distance) / sum(trans_finish_count) avg_trans_finish_distance,
       sum(trans_finish_dur_sec) / sum(trans_finish_count)  avg_trans_finish_dur_sec
from dws_trans_shift_trans_finish_nd
where dt = '20250713'
group by truck_model_type,
         truck_model_type_name,
         recent_days;

select *
from ads_truck_stats;


drop table if exists ads_express_stats;
create external table ads_express_stats
(
    `dt`                string COMMENT '统计日期',
    `recent_days`       tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `deliver_suc_count` bigint COMMENT '派送成功次数（订单数）',
    `sort_count`        bigint COMMENT '分拣次数'
) comment '快递综合统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_stats';

insert overwrite table ads_express_stats
select dt,
       recent_days,
       deliver_suc_count,
       sort_count
from ads_express_stats
union
select nvl(deliver_1d.dt, sort_1d.dt)                   dt,
       nvl(deliver_1d.recent_days, sort_1d.recent_days) recent_days,
       deliver_suc_count,
       sort_count
from (select '20250713'       dt,
             1                recent_days,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '20250713') deliver_1d
         full outer join
     (select '20250713'      dt,
             1               recent_days,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '20250711') sort_1d
     on deliver_1d.dt = sort_1d.dt
         and deliver_1d.recent_days = sort_1d.recent_days
union
select nvl(deliver_nd.dt, sort_nd.dt)                   dt,
       nvl(deliver_nd.recent_days, sort_nd.recent_days) recent_days,
       deliver_suc_count,
       sort_count
from (select '20250713'       dt,
             recent_days,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '20250713'
      group by recent_days) deliver_nd
         full outer join
     (select '20250713'      dt,
             recent_days,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '20250713'
      group by recent_days) sort_nd
     on deliver_nd.dt = sort_nd.dt
         and deliver_nd.recent_days = sort_nd.recent_days;

select *
from ads_express_stats;


drop table if exists ads_express_province_stats;
create external table ads_express_province_stats
(
    `dt`                   string COMMENT '统计日期',
    `recent_days`          tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`          bigint COMMENT '省份ID',
    `province_name`        string COMMENT '省份名称',
    `receive_order_count`  bigint COMMENT '揽收次数',
    `receive_order_amount` decimal(16, 2) COMMENT '揽收金额',
    `deliver_suc_count`    bigint COMMENT '派送成功次数',
    `sort_count`           bigint COMMENT '分拣次数'
) comment '各省份快递统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_province_stats';


insert overwrite table ads_express_province_stats
select dt,
       recent_days,
       province_id,
       province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from ads_express_province_stats
union
select nvl(nvl(province_deliver_1d.dt, province_sort_1d.dt), province_receive_1d.dt) dt,
       nvl(nvl(province_deliver_1d.recent_days, province_sort_1d.recent_days),
           province_receive_1d.recent_days)                                          recent_days,
       nvl(nvl(province_deliver_1d.province_id, province_sort_1d.province_id),
           province_receive_1d.province_id)                                          province_id,
       nvl(nvl(province_deliver_1d.province_name, province_sort_1d.province_name),
           province_receive_1d.province_name)                                        province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '20250713'       dt,
             1                recent_days,
             province_id,
             province_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '20250713'
      group by province_id,
               province_name) province_deliver_1d
         full outer join
     (select '20250713'      dt,
             1               recent_days,
             province_id,
             province_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '20250711'
      group by province_id,
               province_name) province_sort_1d
     on province_deliver_1d.dt = province_sort_1d.dt
         and province_deliver_1d.recent_days = province_sort_1d.recent_days
         and province_deliver_1d.province_id = province_sort_1d.province_id
         and province_deliver_1d.province_name = province_sort_1d.province_name
         full outer join
     (select '20250713'        dt,
             1                 recent_days,
             province_id,
             province_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '2025-07-11'
      group by province_id,
               province_name) province_receive_1d
     on province_deliver_1d.dt = province_receive_1d.dt
         and province_deliver_1d.recent_days = province_receive_1d.recent_days
         and province_deliver_1d.province_id = province_receive_1d.province_id
         and province_deliver_1d.province_name = province_receive_1d.province_name
union
select nvl(nvl(province_deliver_nd.dt, province_sort_nd.dt), province_receive_nd.dt) dt,
       nvl(nvl(province_deliver_nd.recent_days, province_sort_nd.recent_days),
           province_receive_nd.recent_days)                                          recent_days,
       nvl(nvl(province_deliver_nd.province_id, province_sort_nd.province_id),
           province_receive_nd.province_id)                                          province_id,
       nvl(nvl(province_deliver_nd.province_name, province_sort_nd.province_name),
           province_receive_nd.province_name)                                        province_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '20250713'       dt,
             recent_days,
             province_id,
             province_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '20250713'
      group by recent_days,
               province_id,
               province_name) province_deliver_nd
         full outer join
     (select '20250713'      dt,
             recent_days,
             province_id,
             province_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '20250713'
      group by recent_days,
               province_id,
               province_name) province_sort_nd
     on province_deliver_nd.dt = province_sort_nd.dt
         and province_deliver_nd.recent_days = province_sort_nd.recent_days
         and province_deliver_nd.province_id = province_sort_nd.province_id
         and province_deliver_nd.province_name = province_sort_nd.province_name
         full outer join
     (select '20250713'        dt,
             recent_days,
             province_id,
             province_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '2023-01-10'
      group by recent_days,
               province_id,
               province_name) province_receive_nd
     on province_deliver_nd.dt = province_receive_nd.dt
         and province_deliver_nd.recent_days = province_receive_nd.recent_days
         and province_deliver_nd.province_id = province_receive_nd.province_id
         and province_deliver_nd.province_name = province_receive_nd.province_name;


select *
from ads_express_province_stats;

drop table if exists ads_express_city_stats;
create external table ads_express_city_stats
(
    `dt`                   string COMMENT '统计日期',
    `recent_days`          tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `city_id`              bigint COMMENT '城市ID',
    `city_name`            string COMMENT '城市名称',
    `receive_order_count`  bigint COMMENT '揽收次数',
    `receive_order_amount` decimal(16, 2) COMMENT '揽收金额',
    `deliver_suc_count`    bigint COMMENT '派送成功次数',
    `sort_count`           bigint COMMENT '分拣次数'
) comment '各城市快递统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_city_stats';


insert overwrite table ads_express_city_stats
select dt,
       recent_days,
       city_id,
       city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from ads_express_city_stats
union
select nvl(nvl(city_deliver_1d.dt, city_sort_1d.dt), city_receive_1d.dt) dt,
       nvl(nvl(city_deliver_1d.recent_days, city_sort_1d.recent_days),
           city_receive_1d.recent_days)                                  recent_days,
       nvl(nvl(city_deliver_1d.city_id, city_sort_1d.city_id),
           city_receive_1d.city_id)                                      city_id,
       nvl(nvl(city_deliver_1d.city_name, city_sort_1d.city_name),
           city_receive_1d.city_name)                                    city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '20250713'       dt,
             1                recent_days,
             city_id,
             city_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '20250713'
      group by city_id,
               city_name) city_deliver_1d
         full outer join
     (select '20250713'      dt,
             1               recent_days,
             city_id,
             city_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '20250711'
      group by city_id,
               city_name) city_sort_1d
     on city_deliver_1d.dt = city_sort_1d.dt
         and city_deliver_1d.recent_days = city_sort_1d.recent_days
         and city_deliver_1d.city_id = city_sort_1d.city_id
         and city_deliver_1d.city_name = city_sort_1d.city_name
         full outer join
     (select '20250713'        dt,
             1                 recent_days,
             city_id,
             city_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '2025-07-11'
      group by city_id,
               city_name) city_receive_1d
     on city_deliver_1d.dt = city_receive_1d.dt
         and city_deliver_1d.recent_days = city_receive_1d.recent_days
         and city_deliver_1d.city_id = city_receive_1d.city_id
         and city_deliver_1d.city_name = city_receive_1d.city_name
union
select nvl(nvl(city_deliver_nd.dt, city_sort_nd.dt), city_receive_nd.dt) dt,
       nvl(nvl(city_deliver_nd.recent_days, city_sort_nd.recent_days),
           city_receive_nd.recent_days)                                  recent_days,
       nvl(nvl(city_deliver_nd.city_id, city_sort_nd.city_id),
           city_receive_nd.city_id)                                      city_id,
       nvl(nvl(city_deliver_nd.city_name, city_sort_nd.city_name),
           city_receive_nd.city_name)                                    city_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '20250713'       dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '20250713'
      group by recent_days,
               city_id,
               city_name) city_deliver_nd
         full outer join
     (select '20250713'      dt,
             recent_days,
             city_id,
             city_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '20250713'
      group by recent_days,
               city_id,
               city_name) city_sort_nd
     on city_deliver_nd.dt = city_sort_nd.dt
         and city_deliver_nd.recent_days = city_sort_nd.recent_days
         and city_deliver_nd.city_id = city_sort_nd.city_id
         and city_deliver_nd.city_name = city_sort_nd.city_name
         full outer join
     (select '20250713'        dt,
             recent_days,
             city_id,
             city_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '2023-01-10'
      group by recent_days,
               city_id,
               city_name) city_receive_nd
     on city_deliver_nd.dt = city_receive_nd.dt
         and city_deliver_nd.recent_days = city_receive_nd.recent_days
         and city_deliver_nd.city_id = city_receive_nd.city_id
         and city_deliver_nd.city_name = city_receive_nd.city_name;

select *
from ads_express_city_stats;

drop table if exists ads_express_org_stats;
create external table ads_express_org_stats
(
    `dt`                   string COMMENT '统计日期',
    `recent_days`          tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
    `org_id`               bigint COMMENT '机构ID',
    `org_name`             string COMMENT '机构名称',
    `receive_order_count`  bigint COMMENT '揽收次数',
    `receive_order_amount` decimal(16, 2) COMMENT '揽收金额',
    `deliver_suc_count`    bigint COMMENT '派送成功次数',
    `sort_count`           bigint COMMENT '分拣次数'
) comment '各机构快递统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/tms/ads/ads_express_org_stats';


insert overwrite table ads_express_org_stats
select dt,
       recent_days,
       org_id,
       org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from ads_express_org_stats
union
select nvl(nvl(org_deliver_1d.dt, org_sort_1d.dt), org_receive_1d.dt) dt,
       nvl(nvl(org_deliver_1d.recent_days, org_sort_1d.recent_days),
           org_receive_1d.recent_days)                                recent_days,
       nvl(nvl(org_deliver_1d.org_id, org_sort_1d.org_id),
           org_receive_1d.org_id)                                     org_id,
       nvl(nvl(org_deliver_1d.org_name, org_sort_1d.org_name),
           org_receive_1d.org_name)                                   org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '20250713'       dt,
             1                recent_days,
             org_id,
             org_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_1d
      where dt = '20250713'
      group by org_id,
               org_name) org_deliver_1d
         full outer join
     (select '20250713'      dt,
             1               recent_days,
             org_id,
             org_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_1d
      where dt = '20250711'
      group by org_id,
               org_name) org_sort_1d
     on org_deliver_1d.dt = org_sort_1d.dt
         and org_deliver_1d.recent_days = org_sort_1d.recent_days
         and org_deliver_1d.org_id = org_sort_1d.org_id
         and org_deliver_1d.org_name = org_sort_1d.org_name
         full outer join
     (select '20250713'        dt,
             1                 recent_days,
             org_id,
             org_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_1d
      where dt = '2025-07-11'
      group by org_id,
               org_name) org_receive_1d
     on org_deliver_1d.dt = org_receive_1d.dt
         and org_deliver_1d.recent_days = org_receive_1d.recent_days
         and org_deliver_1d.org_id = org_receive_1d.org_id
         and org_deliver_1d.org_name = org_receive_1d.org_name
union
select nvl(nvl(org_deliver_nd.dt, org_sort_nd.dt), org_receive_nd.dt) dt,
       nvl(nvl(org_deliver_nd.recent_days, org_sort_nd.recent_days),
           org_receive_nd.recent_days)                                recent_days,
       nvl(nvl(org_deliver_nd.org_id, org_sort_nd.org_id),
           org_receive_nd.org_id)                                     org_id,
       nvl(nvl(org_deliver_nd.org_name, org_sort_nd.org_name),
           org_receive_nd.org_name)                                   org_name,
       receive_order_count,
       receive_order_amount,
       deliver_suc_count,
       sort_count
from (select '20250713'       dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count) deliver_suc_count
      from dws_trans_org_deliver_suc_nd
      where dt = '20250713'
      group by recent_days,
               org_id,
               org_name) org_deliver_nd
         full outer join
     (select '20250713'      dt,
             recent_days,
             org_id,
             org_name,
             sum(sort_count) sort_count
      from dws_trans_org_sort_nd
      where dt = '20250713'
      group by recent_days,
               org_id,
               org_name) org_sort_nd
     on org_deliver_nd.dt = org_sort_nd.dt
         and org_deliver_nd.recent_days = org_sort_nd.recent_days
         and org_deliver_nd.org_id = org_sort_nd.org_id
         and org_deliver_nd.org_name = org_sort_nd.org_name
         full outer join
     (select '20250713'        dt,
             recent_days,
             org_id,
             org_name,
             sum(order_count)  receive_order_count,
             sum(order_amount) receive_order_amount
      from dws_trans_org_receive_nd
      where dt = '2023-01-10'
      group by recent_days,
               org_id,
               org_name) org_receive_nd
     on org_deliver_nd.dt = org_receive_nd.dt
         and org_deliver_nd.recent_days = org_receive_nd.recent_days
         and org_deliver_nd.org_id = org_receive_nd.org_id
         and org_deliver_nd.org_name = org_receive_nd.org_name;

select *
from ads_express_org_stats;

-- 第一张表
-- 10. ads_driver_stats
-- 统计主题：分司机的运输完成情况
-- 统计内容：按统计日期、最近天数及司机（driver_emp_id/driver_name），统计：
-- 司机完成运输次数、里程、时长
-- 平均每次运输里程和时长（含副司机时，里程和时长均分）
-- 数据来源：从班次运输完成表（dws_trans_shift_trans_finish_nd）处理司机信息（含主副司机拆分）后聚合。
-- 底层：ads_driver_stats →
-- dws_trans_shift_trans_finish_nd →
-- dwd_trans_trans_finish_inc →
-- ods_transport_task/dim_shift_full →
-- ods_line_base_shift/ods_line_base_info/ods_base_dic/ods_truck_driver


-- 第二张表
-- 14. ads_express_city_stats
-- 统计主题：分城市的快递业务指标
-- 统计内容：类似省份统计，但按城市（city_id/city_name）维度，统计揽收、派送、分拣数据。
-- 数据来源：从快递相关表按城市聚合。
-- 底层：ads_express_city_stats →
-- dws_trans_org_deliver_suc_1d/dws_trans_org_sort_1d/dws_trans_org_receive_1d/dws_trans_org_deliver_suc_nd/dws_trans_org_sort_nd/dws_trans_org_receive_nd →
-- dwd_trans_deliver_suc_detail_inc/dwd_bound_sort_inc/dwd_trans_receive_detail_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic/ods_order_org_bound/dim_organ_full/dim_region_full →
-- ods_base_organ/ods_base_region_info


-- 第三张表
-- 15. ads_express_org_stats
-- 统计主题：分机构的快递业务指标
-- 统计内容：按统计日期、最近天数及机构（org_id/org_name），统计机构的揽收、派送、分拣数据。
-- 数据来源：从快递相关表按机构聚合。
-- 底层：ads_express_org_stats →
-- dws_trans_org_deliver_suc_1d/dws_trans_org_sort_1d/dws_trans_org_receive_1d/dws_trans_org_deliver_suc_nd/dws_trans_org_sort_nd/dws_trans_org_receive_nd →
-- dwd_trans_deliver_suc_detail_inc/dwd_bound_sort_inc/dwd_trans_receive_detail_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic/ods_order_org_bound/dim_organ_full/dim_region_full →
-- ods_base_organ/ods_base_region_info


-- 第四张表
-- 13. ads_express_province_stats
-- 统计主题：分省份的快递业务指标
-- 统计内容：按统计日期、最近天数及省份（province_id/province_name），统计：
-- 揽收次数、金额
-- 派送成功次数、分拣次数
-- 数据来源：从快递相关表（dws_trans_org_deliver_suc_1d/nd、dws_trans_org_sort_1d/nd、
-- dws_trans_org_receive_1d/nd）按省份聚合。
-- 底层：ads_express_province_stats →
-- dws_trans_org_deliver_suc_1d/dws_trans_org_sort_1d/dws_trans_org_receive_1d/dws_trans_org_deliver_suc_nd/dws_trans_org_sort_nd/dws_trans_org_receive_nd →
-- dwd_trans_deliver_suc_detail_inc/dwd_bound_sort_inc/dwd_trans_receive_detail_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic/ods_order_org_bound/dim_organ_full/dim_region_full →
-- ods_base_organ/ods_base_region_info


-- 第五张表
-- 9. ads_line_stats
-- 统计主题：分线路的运输完成情况
-- 统计内容：按统计日期、最近天数及线路（line_id/line_name），统计各线路的运输完成指标（次数、里程、时长、运单数）。
-- 数据来源：从班次运输完成表（dws_trans_shift_trans_finish_nd）按线路分组聚合。
-- 底层：ads_line_stats →
-- dws_trans_shift_trans_finish_nd →
-- dwd_trans_trans_finish_inc →
-- ods_transport_task/dim_shift_full/dim_truck_full →
-- ods_line_base_shift/ods_line_base_info/ods_base_dic/ods_truck_info/ods_truck_team/ods_truck_model


-- 第六张表
-- 5. ads_order_cargo_type_stats
-- 统计主题：分货物类型的运单指标
-- 统计内容：按统计日期、最近天数及货物类型（cargo_type），统计不同类型货物的下单情况：
-- 各类型货物的下单数（order_count）
-- 各类型货物的下单金额（order_amount）
-- 数据来源：从分货物类型的下单表（dws_trade_org_cargo_type_order_1d/nd）按货物类型分组聚合。
-- 底层：ads_order_cargo_type_stats →
-- dws_trade_org_cargo_type_order_1d/dws_trade_org_cargo_type_order_nd →
-- dwd_trade_order_detail_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic/dim_organ_full/dim_region_full →
-- ods_base_organ/ods_base_region_info



-- 第七张表
-- 8. ads_shift_stats
-- 统计主题：分班次的运输完成情况
-- 统计内容：按统计日期、最近天数及班次（shift_id），统计各班次的运输完成指标：
-- 完成运输次数、里程、时长
-- 运输完成运单数
-- 数据来源：直接从班次运输完成表（dws_trans_shift_trans_finish_nd）提取。
-- 底层：ads_shift_stats →
-- dws_trans_shift_trans_finish_nd →
-- dwd_trans_trans_finish_inc →
-- ods_transport_task/dim_shift_full/dim_truck_full →
-- ods_line_base_shift/ods_line_base_info/ods_base_dic/ods_truck_info/ods_truck_team/ods_truck_model/ods_base_organ








--
-- 1. ads_trans_order_stats
-- 统计主题：运单相关核心指标
-- 统计内容：按统计日期（dt）和最近天数（recent_days，支持 1/7/30 天），统计接单与发单的总量及金额，包括：
-- 接单总数（receive_order_count）、接单金额（receive_order_amount）
-- 发单总数（dispatch_order_count）、发单金额（dispatch_order_amount）
-- 数据来源：从每日 / 多日汇总层表（dws_trans_org_receive_1d/nd、dws_trans_dispatch_1d/nd）聚合计算。
-- 底层：ads_trans_order_stats →
-- dws_trans_org_receive_1d/dws_trans_dispatch_1d/dws_trans_org_receive_nd/dws_trans_dispatch_nd →
-- dwd_trans_receive_detail_inc/dwd_trans_dispatch_detail_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic

-- 2. ads_trans_stats
-- 统计主题：运输完成情况综合指标
-- 统计内容：按统计日期和最近天数，统计运输完成的核心指标，包括：
-- 完成运输次数（trans_finish_count）
-- 完成运输里程（trans_finish_distance）
-- 完成运输时长（trans_finish_dur_sec，单位：秒）
-- 数据来源：从卡车类型运输完成表（dws_trans_org_truck_model_type_trans_finish_1d）和班次运输完成表（dws_trans_shift_trans_finish_nd）聚合。
-- 底层：ads_trans_stats →
-- dws_trans_org_truck_model_type_trans_finish_1d/dws_trans_shift_trans_finish_nd →
-- dwd_trans_trans_finish_inc →
-- ods_transport_task/dim_shift_full/dim_truck_full →
-- ods_line_base_shift/ods_line_base_info/ods_base_dic/ods_truck_info/ods_truck_team/ods_truck_model/ods_base_organ


-- 3. ads_trans_order_stats_td
-- 统计主题：历史至今运输中运单状态
-- 统计内容：按统计日期，统计当前处于运输中的运单总量及金额：
-- 运输中运单总数（bounding_order_count）
-- 运输中运单金额（bounding_order_amount）
-- 数据来源：通过发单汇总表（dws_trans_dispatch_td）与完成运输表（dws_trans_bound_finish_td，用负数抵消已完成运单）计算差值，得到当前运输中状态。
-- 底层：ads_trans_order_stats_td →
-- dws_trans_dispatch_td/dws_trans_bound_finish_td →
-- dws_trans_dispatch_1d/dwd_trans_bound_finish_detail_inc →
-- dwd_trans_dispatch_detail_inc/dwd_trans_bound_finish_detail_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic


-- 4. ads_order_stats
-- 统计主题：运单下单综合指标
-- 统计内容：按统计日期和最近天数，统计下单的总量及金额：
-- 下单数（order_count）
-- 下单金额（order_amount）
-- 数据来源：从按货物类型的下单表（dws_trade_org_cargo_type_order_1d/nd）聚合。
-- 底层：ads_order_stats →
-- dws_trade_org_cargo_type_order_1d/dws_trade_org_cargo_type_order_nd →
-- dwd_trade_order_detail_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic/dim_organ_full/dim_region_full →
-- ods_base_organ/ods_base_region_info



--
-- 6. ads_city_stats
-- 统计主题：分城市的运输与下单综合分析
-- 统计内容：按统计日期、最近天数及城市（city_id/city_name），综合统计：
-- 城市下单数、金额
-- 城市完成运输次数、里程、时长
-- 平均每次运输里程和时长
-- 数据来源：结合城市下单表与城市运输完成表（如dws_trade_org_cargo_type_order_1d/nd、dws_trans_shift_trans_finish_nd）关联聚合。
-- 底层：ads_city_stats →
-- dws_trade_org_cargo_type_order_1d/dws_trans_org_truck_model_type_trans_finish_1d/dws_trade_org_cargo_type_order_nd/dws_trans_shift_trans_finish_nd →
-- dwd_trade_order_detail_inc/dwd_trans_trans_finish_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic/dim_organ_full/dim_region_full/ods_transport_task/dim_shift_full/dim_truck_full →
-- ods_base_organ/ods_base_region_info/ods_line_base_shift/ods_line_base_info/ods_truck_info/ods_truck_team/ods_truck_model


-- 7. ads_org_stats
-- 统计主题：分机构的运输与下单综合分析
-- 统计内容：按统计日期、最近天数及机构（org_id/org_name），统计：
-- 机构下单数、金额
-- 机构完成运输次数、里程、时长
-- 平均每次运输里程和时长
-- 数据来源：结合机构下单表与机构运输完成表（如dws_trade_org_cargo_type_order_1d/nd、dws_trans_shift_trans_finish_nd）关联聚合。
-- 底层：ads_org_stats →
-- dws_trade_org_cargo_type_order_1d/dws_trans_org_truck_model_type_trans_finish_1d/dws_trade_org_cargo_type_order_nd/dws_trans_shift_trans_finish_nd →
-- dwd_trade_order_detail_inc/dwd_trans_trans_finish_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic/dim_organ_full/dim_region_full/ods_transport_task/dim_shift_full/dim_truck_full →
-- ods_base_organ/ods_base_region_info/ods_line_base_shift/ods_line_base_info/ods_truck_info/ods_truck_team/ods_truck_model




-- 11. ads_truck_stats
-- 统计主题：分卡车类型的运输完成情况
-- 统计内容：按统计日期、最近天数及卡车类别（truck_model_type），统计：
-- 各类型卡车的完成运输次数、里程、时长
-- 平均每次运输里程和时长
-- 数据来源：从班次运输完成表（dws_trans_shift_trans_finish_nd）按卡车类型分组聚合。
-- 底层：ads_truck_stats →
-- dws_trans_shift_trans_finish_nd →
-- dwd_trans_trans_finish_inc/dim_truck_full →
-- ods_transport_task/ods_truck_info/ods_truck_team/ods_truck_model/ods_base_organ/ods_base_dic →
-- ods_line_base_shift/ods_line_base_info


-- 12. ads_express_stats
-- 统计主题：快递业务综合指标
-- 统计内容：按统计日期和最近天数，统计快递核心环节数据：
-- 派送成功次数（deliver_suc_count）
-- 分拣次数（sort_count）
-- 数据来源：从派送成功表（dws_trans_org_deliver_suc_1d/nd）和分拣表（dws_trans_org_sort_1d/nd）聚合。
-- 底层：ads_express_stats →
-- dws_trans_org_deliver_suc_1d/dws_trans_org_sort_1d/dws_trans_org_deliver_suc_nd/dws_trans_org_sort_nd →
-- dwd_trans_deliver_suc_detail_inc/dwd_bound_sort_inc →
-- ods_order_cargo/ods_order_info/ods_base_dic/ods_order_org_bound/dim_organ_full/dim_region_full →
-- ods_base_organ/ods_base_region_info









