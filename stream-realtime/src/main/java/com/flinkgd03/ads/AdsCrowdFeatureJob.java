// AdsCrowdFeatureJob.java
package com.flinkgd03.ads;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * ADS层人群特征数据处理作业
 */
public class AdsCrowdFeatureJob {

    // MySQL配置信息
    private static final String MYSQL_URL = "jdbc:mysql://192.168.142.130:3306/FlinkGd03Ads?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    // DWS层Kafka主题
    private static final String DWS_CROWD_FEATURE_TOPIC = "FlinkGd03_dws_crowd_feature";

    public static void main(String[] args) throws Exception {
        System.out.println("🚀 启动 ADS 人群特征处理作业...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        System.out.println("🔧 Flink环境配置完成");

        // 从DWS层读取人群特征数据
        String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
        DataStreamSource<String> dwsDataStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        bootstrapServers,
                        DWS_CROWD_FEATURE_TOPIC,
                        "ads_crowd_feature_group_" + System.currentTimeMillis(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner((element, recordTimestamp) -> {
                            try {
                                JSONObject json = JSON.parseObject(element);
                                long timestamp = json.getLong("stat_time");
                                System.out.println("⏱️ 提取时间戳: " + timestamp + " for element: " + element);
                                return timestamp;
                            } catch (Exception e) {
                                System.err.println("❌ 时间戳提取失败: " + element);
                                return System.currentTimeMillis();
                            }
                        }),
                "read_dws_crowd_feature"
        );


        // 打印原始数据
        dwsDataStream.print("📥 原始DWS人群特征数据");

        // 写入MySQL
        dwsDataStream.addSink(new CrowdFeatureMysqlSink())
                .name("mysql_crowd_feature_sink")
                .uid("mysql_crowd_feature_sink_uid");

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("ADS-Crowd-Feature-Job");
    }

    /**
     * MySQL Sink for Crowd Feature Data
     */
    private static class CrowdFeatureMysqlSink extends RichSinkFunction<String> {
        private Connection connection;
        private PreparedStatement upsertPreparedStatement;
        private DateTimeFormatter dateTimeFormatter;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                System.out.println("✅ MySQL连接成功: " + MYSQL_URL);

                // 创建表SQL
                String createTableSql = "CREATE TABLE IF NOT EXISTS `ads_crowd_feature` (" +
                        "`id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '人群特征唯一ID', " +
                        "`stat_time` DATETIME NOT NULL COMMENT '统计时间（与DWS对齐，非实时）', " +
                        "`terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型（overall/pc/wireless）', " +
                        "`shop_id` BIGINT NOT NULL COMMENT '所属店铺ID', " +
                        "`crowd_type` VARCHAR(20) NOT NULL COMMENT '人群类型（shop_entry/product_visit/conversion）', " +
                        "`gender` VARCHAR(10) NOT NULL DEFAULT 'unknown' COMMENT '性别（ODS原始）', " +
                        "`age_range` VARCHAR(20) NOT NULL DEFAULT 'unknown' COMMENT '年龄区间（ODS原始）', " +
                        "`city` VARCHAR(50) DEFAULT NULL COMMENT '所在城市（ODS原始）', " +
                        "`taoqi_value` INT DEFAULT 0 COMMENT '淘气值原始数值（ODS原始）', " +
                        "`crowd_count` BIGINT NOT NULL DEFAULT 0 COMMENT '细分人群数量', " +
                        "`total_crowd_count` BIGINT NOT NULL DEFAULT 0 COMMENT '所属人群总数量', " +
                        "`crowd_ratio` DECIMAL(8,4) NOT NULL DEFAULT 0.0000 COMMENT '人群占比', " +
                        "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ADS层创建时间', " +
                        "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间', " +
                        "PRIMARY KEY (`id`), " +
                        "UNIQUE KEY `uk_stat_crowd_attr` (`stat_time`, `terminal_type`, `shop_id`, `crowd_type`, `gender`, `age_range`, `city`, `taoqi_value`), " +
                        "INDEX `idx_crowd_type` (`crowd_type`, `stat_time`) " +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS层-人群特征表（直接支撑看板）'";

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(createTableSql);
                    System.out.println("✅ ads_crowd_feature 表创建成功或已存在");
                }

                // 插入或更新SQL
                String upsertSql = "INSERT INTO ads_crowd_feature (" +
                        "stat_time, terminal_type, shop_id, crowd_type, gender, age_range, " +
                        "city, taoqi_value, crowd_count, total_crowd_count, crowd_ratio) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "crowd_count = VALUES(crowd_count), " +
                        "total_crowd_count = VALUES(total_crowd_count), " +
                        "crowd_ratio = VALUES(crowd_ratio)";

                upsertPreparedStatement = connection.prepareStatement(upsertSql);
                System.out.println("✅ MySQL PreparedStatement创建成功");

            } catch (Exception e) {
                System.err.println("❌ MySQL连接失败: " + e.getMessage());
                throw e;
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            try {
                System.out.println("💾 尝试处理并写入MySQL人群特征数据: " + value);
                JSONObject jsonObject = JSON.parseObject(value);

                // 解析时间戳为字符串格式
                long statTimeMillis = jsonObject.getLongValue("stat_time");
                LocalDateTime statTime = LocalDateTime.ofEpochSecond(
                        statTimeMillis / 1000,
                        0,
                        java.time.ZoneOffset.UTC
                );
                String statTimeString = statTime.format(dateTimeFormatter);

                // 提取各字段
                String terminalType = jsonObject.getString("terminal_type");
                long shopId = jsonObject.getLongValue("shop_id");
                String crowdType = jsonObject.getString("crowd_type");
                String gender = jsonObject.getString("gender");
                String ageRange = jsonObject.getString("age_range");
                String city = jsonObject.getString("city");
                int taoqiValue = jsonObject.getIntValue("taoqi_value");
                long crowdCount = jsonObject.getLongValue("crowd_count");
                long totalCrowdCount = jsonObject.getLongValue("total_crowd_count");
                BigDecimal crowdRatio = jsonObject.getBigDecimal("crowd_ratio") != null ?
                        jsonObject.getBigDecimal("crowd_ratio") : BigDecimal.ZERO;

                // 打印处理后的数据
                System.out.println("📊 处理后数据: " +
                        "stat_time=" + statTimeString + ", " +
                        "terminal_type=" + terminalType + ", " +
                        "shop_id=" + shopId + ", " +
                        "crowd_type=" + crowdType + ", " +
                        "gender=" + gender + ", " +
                        "age_range=" + ageRange + ", " +
                        "city=" + city + ", " +
                        "taoqi_value=" + taoqiValue + ", " +
                        "crowd_count=" + crowdCount + ", " +
                        "total_crowd_count=" + totalCrowdCount + ", " +
                        "crowd_ratio=" + crowdRatio);

                // 设置参数
                upsertPreparedStatement.setString(1, statTimeString);
                upsertPreparedStatement.setString(2, terminalType);
                upsertPreparedStatement.setLong(3, shopId);
                upsertPreparedStatement.setString(4, crowdType);
                upsertPreparedStatement.setString(5, gender);
                upsertPreparedStatement.setString(6, ageRange);
                upsertPreparedStatement.setString(7, city);
                upsertPreparedStatement.setInt(8, taoqiValue);
                upsertPreparedStatement.setLong(9, crowdCount);
                upsertPreparedStatement.setLong(10, totalCrowdCount);
                upsertPreparedStatement.setBigDecimal(11, crowdRatio);

                // 执行插入或更新
                int result = upsertPreparedStatement.executeUpdate();
                System.out.println("✅ MySQL写入成功，影响行数: " + result);

            } catch (Exception e) {
                System.err.println("❌ MySQL写入失败: " + e.getMessage());
                System.err.println("📝 失败数据: " + value);
                e.printStackTrace(); // 添加完整的异常堆栈信息
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            if (upsertPreparedStatement != null) {
                upsertPreparedStatement.close();
                System.out.println("✅ PreparedStatement关闭成功");
            }
            if (connection != null) {
                connection.close();
                System.out.println("✅ MySQL连接关闭成功");
            }
        }
    }
}
