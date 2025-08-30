// AdsPageTrafficRankingJob.java
package com.flinkgd03.ads;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * ADS层页面流量排行TOP10数据处理作业
 */
public class AdsPageTrafficRankingJob {

    // MySQL配置信息
    private static final String MYSQL_URL = "jdbc:mysql://192.168.142.130:3306/FlinkGd03Ads?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    // DWS层Kafka主题
    private static final String DWS_PAGE_TRAFFIC_TOPIC = "FlinkGd03_dws_page_traffic";

    public static void main(String[] args) throws Exception {
        System.out.println("🚀 启动 ADS 页面流量排行TOP10处理作业...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        System.out.println("🔧 Flink环境配置完成");

        // 从DWS层读取页面流量数据
        String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
        DataStreamSource<String> dwsDataStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        bootstrapServers,
                        DWS_PAGE_TRAFFIC_TOPIC,
                        "ads_page_traffic_ranking_group_" + System.currentTimeMillis(),
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
                "read_dws_page_traffic"
        );


        // 打印原始数据
        dwsDataStream.print("📥 原始DWS页面流量数据");

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

        // 按统计维度分组并计算TOP10
        DataStream<String> resultStream = jsonDataStream
                .keyBy(new KeySelector<JSONObject, Tuple4<Long, String, Long, String>>() {
                    @Override
                    public Tuple4<Long, String, Long, String> getKey(JSONObject json) throws Exception {
                        return Tuple4.of(
                                json.getLong("stat_time"),
                                json.getString("terminal_type"),
                                json.getLong("shop_id"),
                                "dummy" // 占位符，因为我们需要对整个窗口进行排序
                        );
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new PageTrafficRankingProcessWindowFunction());



        // 打印处理结果
        resultStream.print("📊 页面流量排行结果");

        // 写入MySQL
        resultStream.addSink(new PageTrafficRankingMysqlSink())
                .name("mysql_page_traffic_ranking_sink")
                .uid("mysql_page_traffic_ranking_sink_uid");

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("ADS-Page-Traffic-Ranking-Job");
    }

    /**
     * 页面流量排行窗口处理函数
     */
    private static class PageTrafficRankingProcessWindowFunction
            extends ProcessWindowFunction<JSONObject, String, Tuple4<Long, String, Long, String>, TimeWindow> {

        @Override
        public void process(
                Tuple4<Long, String, Long, String> key,
                Context context,
                Iterable<JSONObject> elements,
                Collector<String> out) throws Exception {

            System.out.println("🧮 开始处理页面流量排行窗口数据 - Key: " + key + ", 窗口时间范围: " +
                    new java.util.Date(context.window().getStart()) + " - " +
                    new java.util.Date(context.window().getEnd()));

            List<JSONObject> dataList = new ArrayList<>();
            int elementCount = 0;
            for (JSONObject element : elements) {
                elementCount++;
                System.out.println("📊 处理第" + elementCount + "个元素: " + element.toJSONString());
                dataList.add(element);
            }

            System.out.println("📋 窗口内总共 " + elementCount + " 个元素");

            // 按访客数降序排序
            dataList.sort((o1, o2) -> {
                long visitorCount1 = o1.getLongValue("visitor_count");
                long visitorCount2 = o2.getLongValue("visitor_count");
                return Long.compare(visitorCount2, visitorCount1);
            });

            System.out.println("⬇️ 排序后数据:");
            for (int i = 0; i < dataList.size(); i++) {
                System.out.println("  " + (i+1) + ". " + dataList.get(i).toJSONString());
            }

            // 取前10名
            int rank = 1;
            System.out.println("🏅 开始排名处理:");
            for (JSONObject data : dataList) {
                if (rank > 10) {
                    System.out.println("🛑 已达到TOP10限制，停止处理");
                    break;
                }

                // 添加排名
                data.put("rank_num", rank);
                System.out.println("🏅 排名 #" + rank + " 数据: " + data.toJSONString());
                out.collect(data.toJSONString());
                rank++;
            }

            System.out.println("🏁 页面流量排行窗口处理完成 - Key: " + key + ", 总共输出 " + (rank-1) + " 条数据");
        }

    }

    /**
     * MySQL Sink for Page Traffic Ranking Data
     */
    private static class PageTrafficRankingMysqlSink extends RichSinkFunction<String> {
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
                String createTableSql = "CREATE TABLE IF NOT EXISTS `ads_page_traffic_ranking` (" +
                        "`id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '排行唯一ID', " +
                        "`stat_time` DATETIME NOT NULL COMMENT '统计时间（与DWS对齐）', " +
                        "`terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型（overall/pc/wireless）', " +
                        "`shop_id` BIGINT NOT NULL COMMENT '所属店铺ID', " +
                        "`page_id` BIGINT NOT NULL COMMENT '页面ID（ODS原始，TOP10核心字段）', " +
                        "`visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '页面访客数（排序依据）', " +
                        "`rank_num` INT NOT NULL COMMENT 'TOP排名（1-10）', " +
                        "`page_view` BIGINT NOT NULL DEFAULT 0 COMMENT '页面PV', " +
                        "`click_count` BIGINT NOT NULL DEFAULT 0 COMMENT '页面总点击数', " +
                        "`avg_stay_time` INT DEFAULT 0 COMMENT '平均停留时间（秒）', " +
                        "`module_click_json` JSON DEFAULT NULL COMMENT '板块点击分布明细（JSON）', " +
                        "`guide_product_json` JSON DEFAULT NULL COMMENT '引导商品明细（JSON）', " +
                        "`trend_data_json` JSON DEFAULT NULL COMMENT '核心指标趋势（JSON）', " +
                        "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ADS层创建时间', " +
                        "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间', " +
                        "PRIMARY KEY (`id`), " +
                        "UNIQUE KEY `uk_stat_terminal_rank` (`stat_time`, `terminal_type`, `shop_id`, `rank_num`), " +
                        "INDEX `idx_stat_page_rank` (`stat_time`, `page_id`, `rank_num`) " +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS层-页面流量排行TOP10表（直接支撑看板）'";

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(createTableSql);
                    System.out.println("✅ ads_page_traffic_ranking 表创建成功或已存在");
                }

                // 插入或更新SQL
                String upsertSql = "INSERT INTO ads_page_traffic_ranking (" +
                        "stat_time, terminal_type, shop_id, page_id, visitor_count, rank_num, " +
                        "page_view, click_count, avg_stay_time, module_click_json, " +
                        "guide_product_json, trend_data_json) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "visitor_count = VALUES(visitor_count), " +
                        "page_view = VALUES(page_view), " +
                        "click_count = VALUES(click_count), " +
                        "avg_stay_time = VALUES(avg_stay_time), " +
                        "module_click_json = VALUES(module_click_json), " +
                        "guide_product_json = VALUES(guide_product_json), " +
                        "trend_data_json = VALUES(trend_data_json)";

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
                System.out.println("💾 尝试处理并写入MySQL页面流量排行数据: " + value);
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
                long pageId = jsonObject.getLongValue("page_id");
                long visitorCount = jsonObject.getLongValue("visitor_count");
                int rankNum = jsonObject.getIntValue("rank_num");
                long pageView = jsonObject.getLongValue("page_view");
                long clickCount = jsonObject.getLongValue("click_count");
                int avgStayTime = jsonObject.getIntValue("avg_stay_time");
                String moduleClickJson = jsonObject.getString("module_click_json");
                String guideProductJson = jsonObject.getString("guide_product_json");
                String trendDataJson = jsonObject.getString("trend_data_json");

                // 打印处理后的数据
                System.out.println("📊 处理后数据: " +
                        "stat_time=" + statTimeString + ", " +
                        "terminal_type=" + terminalType + ", " +
                        "shop_id=" + shopId + ", " +
                        "page_id=" + pageId + ", " +
                        "visitor_count=" + visitorCount + ", " +
                        "rank_num=" + rankNum + ", " +
                        "page_view=" + pageView + ", " +
                        "click_count=" + clickCount + ", " +
                        "avg_stay_time=" + avgStayTime + ", " +
                        "module_click_json=" + moduleClickJson + ", " +
                        "guide_product_json=" + guideProductJson + ", " +
                        "trend_data_json=" + trendDataJson);

                // 设置参数
                upsertPreparedStatement.setString(1, statTimeString);
                upsertPreparedStatement.setString(2, terminalType);
                upsertPreparedStatement.setLong(3, shopId);
                upsertPreparedStatement.setLong(4, pageId);
                upsertPreparedStatement.setLong(5, visitorCount);
                upsertPreparedStatement.setInt(6, rankNum);
                upsertPreparedStatement.setLong(7, pageView);
                upsertPreparedStatement.setLong(8, clickCount);
                upsertPreparedStatement.setInt(9, avgStayTime);
                upsertPreparedStatement.setString(10, moduleClickJson);
                upsertPreparedStatement.setString(11, guideProductJson);
                upsertPreparedStatement.setString(12, trendDataJson);

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
