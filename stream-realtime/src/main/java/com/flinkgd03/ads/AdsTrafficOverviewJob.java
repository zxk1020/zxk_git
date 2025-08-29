// AdsTrafficOverviewJob.java
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
 * ADS层流量总览数据处理作业
 * 从DWS层读取流量总览数据，计算相关指标并写入MySQL
 */
public class AdsTrafficOverviewJob {

    // MySQL配置信息
    private static final String MYSQL_URL = "jdbc:mysql://192.168.142.130:3306/FlinkGd03Ads?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    // DWS层Kafka主题
    private static final String DWS_TRAFFIC_OVERVIEW_TOPIC = "FlinkGd03_dws_traffic_overview";

    public static void main(String[] args) throws Exception {
        System.out.println("🚀 启动 ADS 流量总览处理作业...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        System.out.println("🔧 Flink环境配置完成");

        // 从DWS层读取流量总览数据
        String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
        DataStreamSource<String> dwsDataStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        bootstrapServers,
                        DWS_TRAFFIC_OVERVIEW_TOPIC,
                        "ads_traffic_overview_group_" + System.currentTimeMillis(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_dws_traffic_overview"
        );

        // 打印原始数据
        dwsDataStream.print("📥 原始DWS数据");

        // 处理数据并写入MySQL
        dwsDataStream.addSink(new TrafficOverviewMysqlSink())
                .name("mysql_traffic_overview_sink")
                .uid("mysql_traffic_overview_sink_uid");

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("ADS-Traffic-Overview-Job");
    }

    /**
     * MySQL Sink for Traffic Overview Data
     */
    private static class TrafficOverviewMysqlSink extends RichSinkFunction<String> {
        private Connection connection;
        private PreparedStatement insertPreparedStatement;
        private DateTimeFormatter dateTimeFormatter; // 声明但不在这里初始化

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            // 在open方法中初始化DateTimeFormatter，避免序列化问题
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                System.out.println("✅ MySQL连接成功: " + MYSQL_URL);

                // 创建表SQL
                String createTableSql = "CREATE TABLE IF NOT EXISTS `ads_traffic_overview` (" +
                        "`stat_time` DATETIME NOT NULL COMMENT '统计时间（与DWS对齐）', " +
                        "`terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型（overall/pc/wireless）', " +
                        "`shop_id` BIGINT NOT NULL COMMENT '所属店铺ID', " +
                        "`shop_visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '访问店铺访客数', " +
                        "`shop_page_view` BIGINT NOT NULL DEFAULT 0 COMMENT '访问店铺PV', " +
                        "`new_visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '新访客数', " +
                        "`old_visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '老访客数', " +
                        "`product_visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '访问商品访客数', " +
                        "`product_page_view` BIGINT NOT NULL DEFAULT 0 COMMENT '访问商品PV', " +
                        "`add_collection_count` BIGINT NOT NULL DEFAULT 0 COMMENT '加购收藏人数', " +
                        "`pay_buyer_count` BIGINT NOT NULL DEFAULT 0 COMMENT '支付买家数', " +
                        "`pay_amount` DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '支付金额', " +
                        "`pay_conversion_rate` DECIMAL(8,4) NOT NULL DEFAULT 0.0000 COMMENT '支付转化率（pay_buyer_count/shop_visitor_count）', " +
                        "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ADS层创建时间', " +
                        "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间', " +
                        "PRIMARY KEY (`stat_time`, `terminal_type`, `shop_id`), " +
                        "INDEX `idx_stat_time_terminal` (`stat_time`, `terminal_type`) " +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS层-流量总览表（直接支撑看板）'";

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(createTableSql);
                    System.out.println("✅ ads_traffic_overview 表创建成功或已存在");
                }

                // 插入SQL（使用ON DUPLICATE KEY UPDATE实现插入或更新）
                String upsertSql = "INSERT INTO ads_traffic_overview (" +
                        "stat_time, terminal_type, shop_id, shop_visitor_count, shop_page_view, " +
                        "new_visitor_count, old_visitor_count, product_visitor_count, product_page_view, " +
                        "add_collection_count, pay_buyer_count, pay_amount, pay_conversion_rate" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "shop_visitor_count = VALUES(shop_visitor_count), " +
                        "shop_page_view = VALUES(shop_page_view), " +
                        "new_visitor_count = VALUES(new_visitor_count), " +
                        "old_visitor_count = VALUES(old_visitor_count), " +
                        "product_visitor_count = VALUES(product_visitor_count), " +
                        "product_page_view = VALUES(product_page_view), " +
                        "add_collection_count = VALUES(add_collection_count), " +
                        "pay_buyer_count = VALUES(pay_buyer_count), " +
                        "pay_amount = VALUES(pay_amount), " +
                        "pay_conversion_rate = VALUES(pay_conversion_rate)";

                insertPreparedStatement = connection.prepareStatement(upsertSql);
                System.out.println("✅ MySQL PreparedStatement创建成功");

            } catch (Exception e) {
                System.err.println("❌ MySQL连接失败: " + e.getMessage());
                throw e;
            }
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            try {
                System.out.println("💾 尝试处理并写入MySQL流量总览数据: " + value);
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
                long shopVisitorCount = jsonObject.getLongValue("shop_visitor_count");
                long shopPageView = jsonObject.getLongValue("shop_page_view");
                long newVisitorCount = jsonObject.getLongValue("new_visitor_count");
                long oldVisitorCount = jsonObject.getLongValue("old_visitor_count");
                long productVisitorCount = jsonObject.getLongValue("product_visitor_count");
                long productPageView = jsonObject.getLongValue("product_page_view");
                long addCollectionCount = jsonObject.getLongValue("add_collection_count");
                long payBuyerCount = jsonObject.getLongValue("pay_buyer_count");
                BigDecimal payAmount = jsonObject.getBigDecimal("pay_amount") != null ?
                        jsonObject.getBigDecimal("pay_amount") : BigDecimal.ZERO;

                // 计算支付转化率
                BigDecimal payConversionRate = BigDecimal.ZERO;
                if (shopVisitorCount > 0) {
                    payConversionRate = new BigDecimal(payBuyerCount)
                            .divide(new BigDecimal(shopVisitorCount), 4, BigDecimal.ROUND_HALF_UP);
                }

                // 打印处理后的数据
                System.out.println("📊 处理后数据: " +
                        "stat_time=" + statTimeString + ", " +
                        "terminal_type=" + terminalType + ", " +
                        "shop_id=" + shopId + ", " +
                        "shop_visitor_count=" + shopVisitorCount + ", " +
                        "shop_page_view=" + shopPageView + ", " +
                        "new_visitor_count=" + newVisitorCount + ", " +
                        "old_visitor_count=" + oldVisitorCount + ", " +
                        "product_visitor_count=" + productVisitorCount + ", " +
                        "product_page_view=" + productPageView + ", " +
                        "add_collection_count=" + addCollectionCount + ", " +
                        "pay_buyer_count=" + payBuyerCount + ", " +
                        "pay_amount=" + payAmount + ", " +
                        "pay_conversion_rate=" + payConversionRate);

                // 设置参数
                insertPreparedStatement.setString(1, statTimeString);
                insertPreparedStatement.setString(2, terminalType);
                insertPreparedStatement.setLong(3, shopId);
                insertPreparedStatement.setLong(4, shopVisitorCount);
                insertPreparedStatement.setLong(5, shopPageView);
                insertPreparedStatement.setLong(6, newVisitorCount);
                insertPreparedStatement.setLong(7, oldVisitorCount);
                insertPreparedStatement.setLong(8, productVisitorCount);
                insertPreparedStatement.setLong(9, productPageView);
                insertPreparedStatement.setLong(10, addCollectionCount);
                insertPreparedStatement.setLong(11, payBuyerCount);
                insertPreparedStatement.setBigDecimal(12, payAmount);
                insertPreparedStatement.setBigDecimal(13, payConversionRate);

                // 执行插入或更新
                int result = insertPreparedStatement.executeUpdate();
                System.out.println("✅ MySQL写入成功，影响行数: " + result);

            } catch (Exception e) {
                System.err.println("❌ MySQL写入失败: " + e.getMessage());
                System.err.println("📝 失败数据: " + value);
                throw e;
            }
        }

        @Override
        public void close() throws Exception {
            if (insertPreparedStatement != null) {
                insertPreparedStatement.close();
                System.out.println("✅ PreparedStatement关闭成功");
            }
            if (connection != null) {
                connection.close();
                System.out.println("✅ MySQL连接关闭成功");
            }
        }
    }
}
