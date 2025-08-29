// AdsKeywordRankingJob.java
package com.flinkgd03.ads;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import com.stream.common.utils.ConfigUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple5;
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

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * ADS层关键词排行TOP10数据处理作业
 */
public class AdsKeywordRankingJob {

    // MySQL配置信息
    private static final String MYSQL_URL = "jdbc:mysql://192.168.142.130:3306/FlinkGd03Ads?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    // DWS层Kafka主题
    private static final String DWS_KEYWORD_RANKING_TOPIC = "FlinkGd03_dws_keyword_ranking";

    public static void main(String[] args) throws Exception {
        System.out.println("🚀 启动 ADS 关键词排行TOP10处理作业...");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        System.out.println("🔧 Flink环境配置完成");

        // 从DWS层读取关键词排行数据
        String bootstrapServers = ConfigUtils.getString("kafka.bootstrap.servers");
        DataStreamSource<String> dwsDataStream = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        bootstrapServers,
                        DWS_KEYWORD_RANKING_TOPIC,
                        "ads_keyword_ranking_group_" + System.currentTimeMillis(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_dws_keyword_ranking"
        );

        // 打印原始数据
        dwsDataStream.print("📥 原始DWS关键词数据");

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
                .keyBy(json -> Tuple5.of(
                        json.getString("stat_time").substring(0, 10), // 日期部分
                        json.getString("stat_dimension"),
                        json.getString("terminal_type"),
                        json.getLong("shop_id"),
                        "dummy" // 占位符，因为我们需要对整个窗口进行排序
                ))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new KeywordRankingProcessWindowFunction());

        // 打印处理结果
        resultStream.print("📊 关键词排行结果");

        // 写入MySQL
        resultStream.addSink(new KeywordRankingMysqlSink())
                .name("mysql_keyword_ranking_sink")
                .uid("mysql_keyword_ranking_sink_uid");

        System.out.println("🏁 准备提交Flink作业...");
        env.execute("ADS-Keyword-Ranking-Job");
    }

    /**
     * 关键词排行窗口处理函数
     */
    private static class KeywordRankingProcessWindowFunction
            extends ProcessWindowFunction<JSONObject, String, Tuple5<String, String, String, Long, String>, TimeWindow> {

        @Override
        public void process(
                Tuple5<String, String, String, Long, String> key,
                Context context,
                Iterable<JSONObject> elements,
                Collector<String> out) throws Exception {

            List<JSONObject> dataList = new ArrayList<>();
            for (JSONObject element : elements) {
                dataList.add(element);
            }

            // 按搜索访客数降序排序
            dataList.sort((o1, o2) -> {
                long searchVisitorCount1 = o1.getLongValue("search_visitor_count");
                long searchVisitorCount2 = o2.getLongValue("search_visitor_count");
                return Long.compare(searchVisitorCount2, searchVisitorCount1);
            });

            // 取前10名
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
     * MySQL Sink for Keyword Ranking Data
     */
    private static class KeywordRankingMysqlSink extends RichSinkFunction<String> {
        private Connection connection;
        private PreparedStatement upsertPreparedStatement;
        private DateTimeFormatter dateFormatter;
        private DateTimeFormatter dateTimeFormatter;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            super.open(parameters);
            dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                System.out.println("✅ MySQL连接成功: " + MYSQL_URL);

                // 创建表SQL
                String createTableSql = "CREATE TABLE IF NOT EXISTS `ads_keyword_ranking` (" +
                        "`id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '排行唯一ID', " +
                        "`stat_time` DATE NOT NULL COMMENT '统计时间（日/7天结束日期）', " +
                        "`stat_dimension` VARCHAR(10) NOT NULL COMMENT '统计维度（day/7day）', " +
                        "`terminal_type` VARCHAR(20) NOT NULL COMMENT '终端类型（overall/pc/wireless）', " +
                        "`shop_id` BIGINT NOT NULL COMMENT '所属店铺ID', " +
                        "`keyword` VARCHAR(100) NOT NULL COMMENT '搜索关键词（ODS原始，TOP10核心展示）', " +
                        "`search_visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '搜索访客数（排序依据）', " +
                        "`rank_num` INT NOT NULL COMMENT 'TOP排名（1-10）', " +
                        "`click_visitor_count` BIGINT NOT NULL DEFAULT 0 COMMENT '点击结果访客数', " +
                        "`create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ADS层创建时间', " +
                        "`update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据更新时间', " +
                        "PRIMARY KEY (`id`), " +
                        "UNIQUE KEY `uk_stat_dim_terminal_rank` (`stat_time`, `stat_dimension`, `terminal_type`, `shop_id`, `rank_num`), " +
                        "INDEX `idx_stat_dim_rank` (`stat_time`, `stat_dimension`, `rank_num`) " +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS层-关键词排行TOP10表（直接支撑看板）'";

                try (Statement stmt = connection.createStatement()) {
                    stmt.execute(createTableSql);
                    System.out.println("✅ ads_keyword_ranking 表创建成功或已存在");
                }

                // 插入或更新SQL
                String upsertSql = "INSERT INTO ads_keyword_ranking (" +
                        "stat_time, stat_dimension, terminal_type, shop_id, keyword, " +
                        "search_visitor_count, rank_num, click_visitor_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "search_visitor_count = VALUES(search_visitor_count), " +
                        "click_visitor_count = VALUES(click_visitor_count)";

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
                System.out.println("💾 尝试处理并写入MySQL关键词排行数据: " + value);
                JSONObject jsonObject = JSON.parseObject(value);

                // 提取各字段
                String statTime = jsonObject.getString("stat_time"); // 日期格式
                String statDimension = jsonObject.getString("stat_dimension");
                String terminalType = jsonObject.getString("terminal_type");
                long shopId = jsonObject.getLongValue("shop_id");
                String keyword = jsonObject.getString("keyword");
                long searchVisitorCount = jsonObject.getLongValue("search_visitor_count");
                int rankNum = jsonObject.getIntValue("rank_num");
                long clickVisitorCount = jsonObject.getLongValue("click_visitor_count");

                // 打印处理后的数据
                System.out.println("📊 处理后数据: " +
                        "stat_time=" + statTime + ", " +
                        "stat_dimension=" + statDimension + ", " +
                        "terminal_type=" + terminalType + ", " +
                        "shop_id=" + shopId + ", " +
                        "keyword=" + keyword + ", " +
                        "search_visitor_count=" + searchVisitorCount + ", " +
                        "rank_num=" + rankNum + ", " +
                        "click_visitor_count=" + clickVisitorCount);

                // 设置参数
                upsertPreparedStatement.setString(1, statTime);
                upsertPreparedStatement.setString(2, statDimension);
                upsertPreparedStatement.setString(3, terminalType);
                upsertPreparedStatement.setLong(4, shopId);
                upsertPreparedStatement.setString(5, keyword);
                upsertPreparedStatement.setLong(6, searchVisitorCount);
                upsertPreparedStatement.setInt(7, rankNum);
                upsertPreparedStatement.setLong(8, clickVisitorCount);

                // 执行插入或更新
                int result = upsertPreparedStatement.executeUpdate();
                System.out.println("✅ MySQL写入成功，影响行数: " + result);

            } catch (Exception e) {
                System.err.println("❌ MySQL写入失败: " + e.getMessage());
                System.err.println("📝 失败数据: " + value);
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
