// AdsTrafficSourceRankingJob.java
package com.flinkgd03.ads;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * ADS层流量来源排行TOP10数据处理作业
 */
public class AdsTrafficSourceRankingJob {

    // MySQL配置信息
    private static final String MYSQL_URL = "jdbc:mysql://192.168.142.130:3306/FlinkGd03Ads?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    // DWS层Kafka主题
    private static final String DWS_TRAFFIC_SOURCE_RANKING_TOPIC = "FlinkGd03_dws_traffic_source_ranking";

    public static void main(String[] args) throws Exception {
        System.out.println("🚀 启动 ADS 流量来源排行TOP10处理作业...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        System.out.println("🔧 Flink环境配置完成");

        // 从DWS层读取流量来源排行数据
        String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
        DataStreamSource<String> dwsDataStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        bootstrapServers,
                        DWS_TRAFFIC_SOURCE_RANKING_TOPIC,
                        "ads_traffic_source_ranking_group_" + System.currentTimeMillis(),
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
                "read_dws_traffic_source_ranking"
        );


        // 打印原始数据
        dwsDataStream.print("📥 原始DWS流量来源数据");

        // 解析JSON数据
        DataStream<JSONObject> jsonDataStream = dwsDataStream
                .map(value -> {
                    try {
                        return JSON.parseObject(value);
                    } catch (Exception e) {
                        System.err.println("❌ JSON解析失败: " + value);
                        return null;
                    }
                })
                .filter(Objects::nonNull);

        // 按统计维度分组（stat_time按小时, terminal_type, shop_id）
        KeyedStream<JSONObject, Tuple3<Long, String, Long>> keyedStream = jsonDataStream
                .keyBy(new TrafficSourceKeySelector());

        // 按小时窗口聚合，计算每个流量来源的总访客数
        DataStream<String> aggregatedStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new TrafficSourceAggregationProcessWindowFunction());

        // 打印处理结果
        aggregatedStream.print("📊 流量来源聚合结果");

        // 写入MySQL
        aggregatedStream.addSink(new TrafficSourceRankingMysqlSink())
                .name("mysql_traffic_source_ranking_sink")
                .uid("mysql_traffic_source_ranking_sink_uid");

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("ADS-Traffic-Source-Ranking-Job");
    }

    /**
     * 流量来源Key选择器
     * 按小时时间戳、终端类型、店铺ID分组
     */
    private static class TrafficSourceKeySelector implements KeySelector<JSONObject, Tuple3<Long, String, Long>> {
        @Override
        public Tuple3<Long, String, Long> getKey(JSONObject json) throws Exception {
            long hourTime = json.getLong("stat_time") / (60 * 60 * 1000) * (60 * 60 * 1000); // 按小时分组
            return Tuple3.of(
                    hourTime,
                    json.getString("terminal_type"),
                    json.getLong("shop_id")
            );
        }
    }

    /**
     * 流量来源聚合窗口处理函数
     */
    private static class TrafficSourceAggregationProcessWindowFunction
            extends ProcessWindowFunction<JSONObject, String, Tuple3<Long, String, Long>, TimeWindow> {

        @Override
        public void process(
                Tuple3<Long, String, Long> key,
                Context context,
                Iterable<JSONObject> elements,
                Collector<String> out) throws Exception {

            // 按流量来源分组聚合
            Map<String, JSONObject> sourceAggregationMap = new HashMap<>();

            for (JSONObject element : elements) {
                String sourceKey = element.getString("traffic_source_first") + "|" +
                        element.getString("traffic_source_second");

                if (sourceAggregationMap.containsKey(sourceKey)) {
                    // 累加指标
                    JSONObject aggregated = sourceAggregationMap.get(sourceKey);
                    aggregated.put("visitor_count",
                            aggregated.getLongValue("visitor_count") + element.getLongValue("visitor_count"));
                    aggregated.put("product_visitor_count",
                            aggregated.getLongValue("product_visitor_count") + element.getLongValue("product_visitor_count"));
                    aggregated.put("product_pay_amount",
                            new BigDecimal(aggregated.getString("product_pay_amount")).add(new BigDecimal(element.getString("product_pay_amount"))));
                } else {
                    // 第一次出现，复制并放入
                    JSONObject copy = new JSONObject();
                    copy.put("stat_time", element.getLong("stat_time"));
                    copy.put("terminal_type", element.getString("terminal_type"));
                    copy.put("shop_id", element.getLong("shop_id"));
                    copy.put("traffic_source_first", element.getString("traffic_source_first"));
                    copy.put("traffic_source_second", element.getString("traffic_source_second"));
                    copy.put("visitor_count", element.getLongValue("visitor_count"));
                    copy.put("product_visitor_count", element.getLongValue("product_visitor_count"));
                    copy.put("product_pay_amount", new BigDecimal(element.getString("product_pay_amount")));
                    copy.put("sub_source_list", element.getJSONArray("sub_source_list"));
                    copy.put("related_product_id", element.getLong("related_product_id"));
                    sourceAggregationMap.put(sourceKey, copy);
                }
            }

            // 转换为列表并排序
            List<JSONObject> dataList = new ArrayList<>(sourceAggregationMap.values());

            // 按访客数降序排序
            dataList.sort((o1, o2) -> {
                long visitorCount1 = o1.getLongValue("visitor_count");
                long visitorCount2 = o2.getLongValue("visitor_count");
                return Long.compare(visitorCount2, visitorCount1);
            });

            // 取前10名并添加排名
            int rank = 1;
            for (JSONObject data : dataList) {
                if (rank > 10) {
                    break;
                }

                // 添加排名
                data.put("rank_num", rank);
                out.collect(data.toJSONString());
                rank++;
            }
        }
    }

    /**
     * MySQL Sink for Traffic Source Ranking Data
     */
    private static class TrafficSourceRankingMysqlSink extends RichSinkFunction<String> {
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
                String createTableSql = "CREATE TABLE IF NOT EXISTS `ads_traffic_source_ranking` (" +
                        "`id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '排行唯一ID', " +
                        "`stat_time` DATETIME NOT NULL COMMENT '统计时间（与DWS对齐）', " +
                        "`terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型（overall/pc/wireless）', " +
                        "`shop_id` BIGINT NOT NULL COMMENT '所属店铺ID', " +
                        "`traffic_source_first` VARCHAR(50) NOT NULL COMMENT '一级流量来源（ODS原始）', " +
                        "`traffic_source_second` VARCHAR(50) NOT NULL COMMENT '二级流量来源（ODS原始，TOP10核心展示）', " +
                        "`visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '访客数（排序依据，降序）', " +
                        "`rank_num` INT NOT NULL COMMENT 'TOP排名（1-10）', " +
                        "`sub_source_list` JSON DEFAULT NULL COMMENT '细分来源列表（JSON）', " +
                        "`related_product_id` BIGINT DEFAULT NULL COMMENT '主要导流商品ID', " +
                        "`product_visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '导流商品访客数', " +
                        "`product_pay_amount` DECIMAL(18,2) NOT NULL DEFAULT 0.00 COMMENT '导流商品支付金额', " +
                        "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ADS层创建时间', " +
                        "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间', " +
                        "PRIMARY KEY (`id`), " +
                        "UNIQUE KEY `uk_stat_terminal_source_rank` (`stat_time`, `terminal_type`, `shop_id`, `traffic_source_first`, `rank_num`), " +
                        "INDEX `idx_stat_source_rank` (`stat_time`, `traffic_source_first`, `rank_num`) " +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS层-流量来源排行TOP10表（直接支撑看板）'";

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(createTableSql);
                    System.out.println("✅ ads_traffic_source_ranking 表创建成功或已存在");
                }

                // 插入或更新SQL
                String upsertSql = "INSERT INTO ads_traffic_source_ranking (" +
                        "stat_time, terminal_type, shop_id, traffic_source_first, traffic_source_second, " +
                        "visitor_count, rank_num, sub_source_list, related_product_id, product_visitor_count, " +
                        "product_pay_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "visitor_count = VALUES(visitor_count), " +
                        "sub_source_list = VALUES(sub_source_list), " +
                        "related_product_id = VALUES(related_product_id), " +
                        "product_visitor_count = VALUES(product_visitor_count), " +
                        "product_pay_amount = VALUES(product_pay_amount)";

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
                System.out.println("💾 尝试处理并写入MySQL流量来源排行数据: " + value);
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
                String trafficSourceFirst = jsonObject.getString("traffic_source_first");
                String trafficSourceSecond = jsonObject.getString("traffic_source_second");
                long visitorCount = jsonObject.getLongValue("visitor_count");
                int rankNum = jsonObject.getIntValue("rank_num");
                JSONArray subSourceList = jsonObject.getJSONArray("sub_source_list");
                Long relatedProductId = jsonObject.getLong("related_product_id");
                long productVisitorCount = jsonObject.getLongValue("product_visitor_count");
                BigDecimal productPayAmount = jsonObject.getBigDecimal("product_pay_amount") != null ?
                        jsonObject.getBigDecimal("product_pay_amount") : BigDecimal.ZERO;

                // 打印处理后的数据
                System.out.println("📊 处理后数据: " +
                        "stat_time=" + statTimeString + ", " +
                        "terminal_type=" + terminalType + ", " +
                        "shop_id=" + shopId + ", " +
                        "traffic_source_first=" + trafficSourceFirst + ", " +
                        "traffic_source_second=" + trafficSourceSecond + ", " +
                        "visitor_count=" + visitorCount + ", " +
                        "rank_num=" + rankNum);

                // 设置参数
                upsertPreparedStatement.setString(1, statTimeString);
                upsertPreparedStatement.setString(2, terminalType);
                upsertPreparedStatement.setLong(3, shopId);
                upsertPreparedStatement.setString(4, trafficSourceFirst);
                upsertPreparedStatement.setString(5, trafficSourceSecond);
                upsertPreparedStatement.setLong(6, visitorCount);
                upsertPreparedStatement.setInt(7, rankNum);
                upsertPreparedStatement.setString(8, subSourceList != null ? JSON.toJSONString(subSourceList) : null);
                if (relatedProductId != null) {
                    upsertPreparedStatement.setLong(9, relatedProductId);
                } else {
                    upsertPreparedStatement.setNull(9, Types.BIGINT);
                }
                upsertPreparedStatement.setLong(10, productVisitorCount);
                upsertPreparedStatement.setBigDecimal(11, productPayAmount);

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
