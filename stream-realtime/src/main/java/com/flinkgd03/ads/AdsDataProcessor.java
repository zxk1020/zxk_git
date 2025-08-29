package com.flinkgd03.ads;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * ADS层数据处理：从DWS层Kafka主题读取数据，聚合计算后写入MySQL的FlinkGd03Ads数据库
 */
public class AdsDataProcessor {

    // MySQL配置信息
    private static final String MYSQL_URL = "jdbc:mysql://192.168.142.130:3306/FlinkGd03Ads?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "root";

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        String kafkaServer = ConfigUtils.getString("kafka.bootstrap.servers");

        // 读取各个DWS层主题数据
        DataStreamSource<String> trafficOverviewStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dws_traffic_overview", "ads_traffic_overview_group",
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "traffic-overview-source"
        );

        DataStreamSource<String> trafficSourceRankingStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dws_traffic_source_ranking", "ads_traffic_source_ranking_group",
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "traffic-source-ranking-source"
        );

        DataStreamSource<String> keywordRankingStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dws_keyword_ranking", "ads_keyword_ranking_group",
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "keyword-ranking-source"
        );

        DataStreamSource<String> productTrafficStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dws_product_traffic", "ads_product_traffic_group",
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "product-traffic-source"
        );

        DataStreamSource<String> pageTrafficStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dws_page_traffic", "ads_page_traffic_group",
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "page-traffic-source"
        );

        DataStreamSource<String> crowdFeatureStream = env.fromSource(
                KafkaUtils.buildKafkaSource(kafkaServer, "FlinkGd03_dws_crowd_feature", "ads_crowd_feature_group",
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "crowd-feature-source"
        );

        // 处理流量总览数据
        trafficOverviewStream
                .map(value -> JSONObject.parseObject(value))
                .keyBy(json -> json.getLong("stat_time") + "_" + json.getString("terminal_type") + "_" + json.getLong("shop_id"))
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(
                        new TrafficOverviewAggregateFunction(),
                        new TrafficOverviewProcessWindowFunction()
                )
                .addSink(new MysqlTrafficOverviewSink())
                .name("sink-traffic-overview-ads");

        // 处理流量来源排行数据 (TOP10)
        trafficSourceRankingStream
                .map(value -> JSONObject.parseObject(value))
                .keyBy(json -> json.getLong("stat_time") + "_" + json.getString("terminal_type") + "_" + json.getLong("shop_id"))
                .process(new TrafficSourceRankingProcessFunction())
                .addSink(new MysqlTrafficSourceRankingSink())
                .name("sink-traffic-source-ranking-ads");

        // 处理关键词排行数据 (TOP10)
        keywordRankingStream
                .map(value -> JSONObject.parseObject(value))
                .keyBy(json -> json.getString("stat_time") + "_" + json.getString("stat_dimension") + "_" +
                        json.getString("terminal_type") + "_" + json.getLong("shop_id"))
                .process(new KeywordRankingProcessFunction())
                .addSink(new MysqlKeywordRankingSink())
                .name("sink-keyword-ranking-ads");

        // 处理商品流量排行数据 (TOP10)
        productTrafficStream
                .map(value -> JSONObject.parseObject(value))
                .keyBy(json -> json.getLong("stat_time") + "_" + json.getString("terminal_type") + "_" + json.getLong("shop_id"))
                .process(new ProductTrafficRankingProcessFunction())
                .addSink(new MysqlProductTrafficRankingSink())
                .name("sink-product-traffic-ranking-ads");

        // 处理页面流量排行数据 (TOP10)
        pageTrafficStream
                .map(value -> JSONObject.parseObject(value))
                .keyBy(json -> json.getLong("stat_time") + "_" + json.getString("terminal_type") + "_" + json.getLong("shop_id"))
                .process(new PageTrafficRankingProcessFunction())
                .addSink(new MysqlPageTrafficRankingSink())
                .name("sink-page-traffic-ranking-ads");

        // 处理人群特征数据
        crowdFeatureStream
                .map(value -> JSONObject.parseObject(value))
                .keyBy(json -> json.getLong("stat_time") + "_" + json.getString("terminal_type") + "_" + json.getLong("shop_id"))
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(
                        new CrowdFeatureAggregateFunction(),
                        new CrowdFeatureProcessWindowFunction()
                )
                .addSink(new MysqlCrowdFeatureSink())
                .name("sink-crowd-feature-ads");

        env.execute("ADS Layer Data Processing");
    }

    // 流量总览聚合函数
    private static class TrafficOverviewAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public JSONObject createAccumulator() {
            return new JSONObject();
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            if (accumulator.isEmpty()) {
                accumulator.put("stat_time", value.getLong("stat_time"));
                accumulator.put("terminal_type", value.getString("terminal_type"));
                accumulator.put("shop_id", value.getLong("shop_id"));
                accumulator.put("shop_visitor_count", value.getLong("shop_visitor_count"));
                accumulator.put("shop_page_view", value.getLong("shop_page_view"));
                accumulator.put("new_visitor_count", value.getLong("new_visitor_count"));
                accumulator.put("old_visitor_count", value.getLong("old_visitor_count"));
                accumulator.put("product_visitor_count", value.getLong("product_visitor_count"));
                accumulator.put("product_page_view", value.getLong("product_page_view"));
                accumulator.put("add_collection_count", value.getLong("add_collection_count"));
                accumulator.put("pay_buyer_count", value.getLong("pay_buyer_count"));
                accumulator.put("pay_amount", new BigDecimal(value.getString("pay_amount")));
            } else {
                accumulator.put("shop_visitor_count", accumulator.getLong("shop_visitor_count") + value.getLong("shop_visitor_count"));
                accumulator.put("shop_page_view", accumulator.getLong("shop_page_view") + value.getLong("shop_page_view"));
                accumulator.put("new_visitor_count", accumulator.getLong("new_visitor_count") + value.getLong("new_visitor_count"));
                accumulator.put("old_visitor_count", accumulator.getLong("old_visitor_count") + value.getLong("old_visitor_count"));
                accumulator.put("product_visitor_count", accumulator.getLong("product_visitor_count") + value.getLong("product_visitor_count"));
                accumulator.put("product_page_view", accumulator.getLong("product_page_view") + value.getLong("product_page_view"));
                accumulator.put("add_collection_count", accumulator.getLong("add_collection_count") + value.getLong("add_collection_count"));
                accumulator.put("pay_buyer_count", accumulator.getLong("pay_buyer_count") + value.getLong("pay_buyer_count"));
                accumulator.put("pay_amount", accumulator.getBigDecimal("pay_amount").add(new BigDecimal(value.getString("pay_amount"))));
            }
            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            JSONObject merged = new JSONObject();
            merged.put("stat_time", a.getLong("stat_time"));
            merged.put("terminal_type", a.getString("terminal_type"));
            merged.put("shop_id", a.getLong("shop_id"));
            merged.put("shop_visitor_count", a.getLong("shop_visitor_count") + b.getLong("shop_visitor_count"));
            merged.put("shop_page_view", a.getLong("shop_page_view") + b.getLong("shop_page_view"));
            merged.put("new_visitor_count", a.getLong("new_visitor_count") + b.getLong("new_visitor_count"));
            merged.put("old_visitor_count", a.getLong("old_visitor_count") + b.getLong("old_visitor_count"));
            merged.put("product_visitor_count", a.getLong("product_visitor_count") + b.getLong("product_visitor_count"));
            merged.put("product_page_view", a.getLong("product_page_view") + b.getLong("product_page_view"));
            merged.put("add_collection_count", a.getLong("add_collection_count") + b.getLong("add_collection_count"));
            merged.put("pay_buyer_count", a.getLong("pay_buyer_count") + b.getLong("pay_buyer_count"));
            merged.put("pay_amount", a.getBigDecimal("pay_amount").add(b.getBigDecimal("pay_amount")));
            return merged;
        }
    }

    // 流量总览窗口处理函数
    private static class TrafficOverviewProcessWindowFunction extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
            JSONObject data = elements.iterator().next();

            // 计算支付转化率
            long shopVisitorCount = data.getLong("shop_visitor_count");
            long payBuyerCount = data.getLong("pay_buyer_count");
            BigDecimal payConversionRate = shopVisitorCount > 0 ?
                    new BigDecimal(payBuyerCount).divide(new BigDecimal(shopVisitorCount), 4, BigDecimal.ROUND_HALF_UP) :
                    BigDecimal.ZERO;
            data.put("pay_conversion_rate", payConversionRate);

            out.collect(data);
        }
    }

    // 流量来源排行处理函数
    public static class TrafficSourceRankingProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient org.apache.flink.api.common.state.MapState<String, List<JSONObject>> trafficSourceState;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.flink.api.common.state.MapStateDescriptor<String, List<JSONObject>> descriptor =
                    new org.apache.flink.api.common.state.MapStateDescriptor<>("trafficSourceState",
                            org.apache.flink.api.common.typeinfo.Types.STRING,
                            org.apache.flink.api.common.typeinfo.Types.LIST(org.apache.flink.api.common.typeinfo.Types.GENERIC(JSONObject.class)));
            trafficSourceState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
            String key = value.getString("traffic_source_first") + "_" + value.getString("traffic_source_second");
            List<JSONObject> list = trafficSourceState.get(key);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(value);
            trafficSourceState.put(key, list);

            // 触发计算
            ctx.timerService().registerEventTimeTimer(value.getLong("stat_time") + 3600000); // 1小时后触发
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            Iterable<String> keys = trafficSourceState.keys();
            List<JSONObject> allData = new ArrayList<>();

            for (String key : keys) {
                List<JSONObject> list = trafficSourceState.get(key);
                if (list != null) {
                    allData.addAll(list);
                }
            }

            // 按访客数排序
            allData.sort((o1, o2) -> Long.compare(o2.getLong("visitor_count"), o1.getLong("visitor_count")));

            // 取前10名并设置排名
            for (int i = 0; i < Math.min(10, allData.size()); i++) {
                JSONObject ranking = allData.get(i);
                ranking.put("rank_num", i + 1);
                out.collect(ranking);
            }

            trafficSourceState.clear();
        }
    }

    // 关键词排行处理函数
// 关键词排行处理函数
    public static class KeywordRankingProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient org.apache.flink.api.common.state.MapState<String, List<JSONObject>> keywordState;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.flink.api.common.state.MapStateDescriptor<String, List<JSONObject>> descriptor =
                    new org.apache.flink.api.common.state.MapStateDescriptor<>("keywordState",
                            org.apache.flink.api.common.typeinfo.Types.STRING,
                            org.apache.flink.api.common.typeinfo.Types.LIST(org.apache.flink.api.common.typeinfo.Types.GENERIC(JSONObject.class)));
            keywordState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
            String key = value.getString("keyword");
            List<JSONObject> list = keywordState.get(key);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(value);
            keywordState.put(key, list);

            // 触发计算 - 使用字符串处理时间，避免使用getDate()
            try {
                String statTimeStr = value.getString("stat_time");
                long statTime;

                // 判断是否为纯数字时间戳字符串
                if (statTimeStr != null && statTimeStr.matches("\\d+")) {
                    statTime = Long.parseLong(statTimeStr);
                } else if (statTimeStr != null && statTimeStr.length() >= 10) {
                    // 如果是日期格式如 "2023-01-01"，转换为当天0点的时间戳
                    statTime = System.currentTimeMillis(); // 简化处理，使用当前时间
                } else {
                    statTime = System.currentTimeMillis();
                }

                ctx.timerService().registerEventTimeTimer(statTime + 86400000); // 1天后触发
            } catch (Exception e) {
                // 如果解析失败，使用当前时间
                long statTime = System.currentTimeMillis();
                ctx.timerService().registerEventTimeTimer(statTime + 86400000); // 1天后触发
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            Iterable<String> keys = keywordState.keys();
            List<JSONObject> allData = new ArrayList<>();

            for (String key : keys) {
                List<JSONObject> list = keywordState.get(key);
                if (list != null) {
                    allData.addAll(list);
                }
            }

            // 按搜索访客数排序并取TOP10
            allData.sort((o1, o2) -> Long.compare(o2.getLong("search_visitor_count"), o1.getLong("search_visitor_count")));

            for (int i = 0; i < Math.min(10, allData.size()); i++) {
                JSONObject item = allData.get(i);
                item.put("rank_num", i + 1);
                out.collect(item);
            }
            keywordState.clear();
        }
    }

    // 商品流量排行处理函数
    public static class ProductTrafficRankingProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient org.apache.flink.api.common.state.MapState<Long, List<JSONObject>> productState;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.flink.api.common.state.MapStateDescriptor<Long, List<JSONObject>> descriptor =
                    new org.apache.flink.api.common.state.MapStateDescriptor<>("productState",
                            org.apache.flink.api.common.typeinfo.Types.LONG,
                            org.apache.flink.api.common.typeinfo.Types.LIST(org.apache.flink.api.common.typeinfo.Types.GENERIC(JSONObject.class)));
            productState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
            Long productId = value.getLong("product_id");
            List<JSONObject> list = productState.get(productId);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(value);
            productState.put(productId, list);

            // 触发计算
            ctx.timerService().registerEventTimeTimer(value.getLong("stat_time") + 3600000); // 1小时后触发
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            Iterable<Long> keys = productState.keys();
            List<JSONObject> allData = new ArrayList<>();

            for (Long key : keys) {
                List<JSONObject> list = productState.get(key);
                if (list != null) {
                    allData.addAll(list);
                }
            }

            // 按访客数排序
            allData.sort((o1, o2) -> Long.compare(o2.getLong("visitor_count"), o1.getLong("visitor_count")));

            // 取前10名并设置排名
            for (int i = 0; i < Math.min(10, allData.size()); i++) {
                JSONObject ranking = allData.get(i);
                ranking.put("rank_num", i + 1);

                // 计算商品支付转化率
                long visitorCount = ranking.getLong("visitor_count");
                long payBuyerCount = ranking.getLong("pay_buyer_count");
                BigDecimal payConversionRate = visitorCount > 0 ?
                        new BigDecimal(payBuyerCount).divide(new BigDecimal(visitorCount), 4, BigDecimal.ROUND_HALF_UP) :
                        BigDecimal.ZERO;
                ranking.put("pay_conversion_rate", payConversionRate);

                out.collect(ranking);
            }

            productState.clear();
        }
    }

    // 页面流量排行处理函数
    public static class PageTrafficRankingProcessFunction extends KeyedProcessFunction<String, JSONObject, JSONObject> {
        private transient org.apache.flink.api.common.state.MapState<Long, List<JSONObject>> pageState;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.flink.api.common.state.MapStateDescriptor<Long, List<JSONObject>> descriptor =
                    new org.apache.flink.api.common.state.MapStateDescriptor<>("pageState",
                            org.apache.flink.api.common.typeinfo.Types.LONG,
                            org.apache.flink.api.common.typeinfo.Types.LIST(org.apache.flink.api.common.typeinfo.Types.GENERIC(JSONObject.class)));
            pageState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
            Long pageId = value.getLong("page_id");
            List<JSONObject> list = pageState.get(pageId);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(value);
            pageState.put(pageId, list);

            // 触发计算
            ctx.timerService().registerEventTimeTimer(value.getLong("stat_time") + 3600000); // 1小时后触发
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
            Iterable<Long> keys = pageState.keys();
            List<JSONObject> allData = new ArrayList<>();

            for (Long key : keys) {
                List<JSONObject> list = pageState.get(key);
                if (list != null) {
                    allData.addAll(list);
                }
            }

            // 按访客数排序
            allData.sort((o1, o2) -> Long.compare(o2.getLong("visitor_count"), o1.getLong("visitor_count")));

            // 取前10名并设置排名
            for (int i = 0; i < Math.min(10, allData.size()); i++) {
                JSONObject ranking = allData.get(i);
                ranking.put("rank_num", i + 1);
                out.collect(ranking);
            }

            pageState.clear();
        }
    }

    // 人群特征聚合函数
    private static class CrowdFeatureAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, JSONObject, JSONObject> {
        @Override
        public JSONObject createAccumulator() {
            return new JSONObject();
        }

        @Override
        public JSONObject add(JSONObject value, JSONObject accumulator) {
            if (accumulator.isEmpty()) {
                accumulator.put("stat_time", value.getLong("stat_time"));
                accumulator.put("terminal_type", value.getString("terminal_type"));
                accumulator.put("shop_id", value.getLong("shop_id"));
                accumulator.put("crowd_type", value.getString("crowd_type"));
                accumulator.put("gender", value.getString("gender"));
                accumulator.put("age_range", value.getString("age_range"));
                accumulator.put("city", value.getString("city"));
                accumulator.put("taoqi_value", value.getInteger("taoqi_value"));
                accumulator.put("crowd_count", value.getLong("crowd_count"));
                accumulator.put("total_crowd_count", value.getLong("total_crowd_count"));
            } else {
                accumulator.put("crowd_count", accumulator.getLong("crowd_count") + value.getLong("crowd_count"));
                accumulator.put("total_crowd_count", accumulator.getLong("total_crowd_count") + value.getLong("total_crowd_count"));
            }
            return accumulator;
        }

        @Override
        public JSONObject getResult(JSONObject accumulator) {
            return accumulator;
        }

        @Override
        public JSONObject merge(JSONObject a, JSONObject b) {
            JSONObject merged = new JSONObject();
            merged.put("stat_time", a.getLong("stat_time"));
            merged.put("terminal_type", a.getString("terminal_type"));
            merged.put("shop_id", a.getLong("shop_id"));
            merged.put("crowd_type", a.getString("crowd_type"));
            merged.put("gender", a.getString("gender"));
            merged.put("age_range", a.getString("age_range"));
            merged.put("city", a.getString("city"));
            merged.put("taoqi_value", a.getInteger("taoqi_value"));
            merged.put("crowd_count", a.getLong("crowd_count") + b.getLong("crowd_count"));
            merged.put("total_crowd_count", a.getLong("total_crowd_count") + b.getLong("total_crowd_count"));
            return merged;
        }
    }

    // 人群特征窗口处理函数
    private static class CrowdFeatureProcessWindowFunction extends ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
            JSONObject data = elements.iterator().next();

            // 计算人群占比
            long crowdCount = data.getLong("crowd_count");
            long totalCrowdCount = data.getLong("total_crowd_count");
            BigDecimal crowdRatio = totalCrowdCount > 0 ?
                    new BigDecimal(crowdCount).divide(new BigDecimal(totalCrowdCount), 4, BigDecimal.ROUND_HALF_UP) :
                    BigDecimal.ZERO;
            data.put("crowd_ratio", crowdRatio);

            out.collect(data);
        }
    }

    // MySQL Sink for Traffic Overview Data
    private static class MysqlTrafficOverviewSink extends RichSinkFunction<JSONObject> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                String sql = "INSERT INTO ads_traffic_overview (stat_time, terminal_type, shop_id, shop_visitor_count, " +
                        "shop_page_view, new_visitor_count, old_visitor_count, product_visitor_count, product_page_view, " +
                        "add_collection_count, pay_buyer_count, pay_amount, pay_conversion_rate, create_time, update_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "shop_visitor_count=VALUES(shop_visitor_count), shop_page_view=VALUES(shop_page_view), " +
                        "new_visitor_count=VALUES(new_visitor_count), old_visitor_count=VALUES(old_visitor_count), " +
                        "product_visitor_count=VALUES(product_visitor_count), product_page_view=VALUES(product_page_view), " +
                        "add_collection_count=VALUES(add_collection_count), pay_buyer_count=VALUES(pay_buyer_count), " +
                        "pay_amount=VALUES(pay_amount), pay_conversion_rate=VALUES(pay_conversion_rate), update_time=NOW()";
                preparedStatement = connection.prepareStatement(sql);
            } catch (Exception e) {
                throw new RuntimeException("MySQL连接失败", e);
            }
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            try {
                preparedStatement.setTimestamp(1, new Timestamp(value.getLong("stat_time")));
                preparedStatement.setString(2, value.getString("terminal_type"));
                preparedStatement.setLong(3, value.getLong("shop_id"));
                preparedStatement.setLong(4, value.getLong("shop_visitor_count"));
                preparedStatement.setLong(5, value.getLong("shop_page_view"));
                preparedStatement.setLong(6, value.getLong("new_visitor_count"));
                preparedStatement.setLong(7, value.getLong("old_visitor_count"));
                preparedStatement.setLong(8, value.getLong("product_visitor_count"));
                preparedStatement.setLong(9, value.getLong("product_page_view"));
                preparedStatement.setLong(10, value.getLong("add_collection_count"));
                preparedStatement.setLong(11, value.getLong("pay_buyer_count"));
                preparedStatement.setBigDecimal(12, value.getBigDecimal("pay_amount"));
                preparedStatement.setBigDecimal(13, value.getBigDecimal("pay_conversion_rate"));
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("写入MySQL失败: " + value.toJSONString(), e);
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    // MySQL Sink for Traffic Source Ranking Data
    private static class MysqlTrafficSourceRankingSink extends RichSinkFunction<JSONObject> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                String sql = "INSERT INTO ads_traffic_source_ranking (stat_time, terminal_type, shop_id, traffic_source_first, " +
                        "traffic_source_second, visitor_count, rank_num, sub_source_list, related_product_id, " +
                        "product_visitor_count, product_pay_amount, create_time, update_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "visitor_count=VALUES(visitor_count), sub_source_list=VALUES(sub_source_list), " +
                        "related_product_id=VALUES(related_product_id), product_visitor_count=VALUES(product_visitor_count), " +
                        "product_pay_amount=VALUES(product_pay_amount), update_time=NOW()";
                preparedStatement = connection.prepareStatement(sql);
            } catch (Exception e) {
                throw new RuntimeException("MySQL连接失败", e);
            }
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            try {
                preparedStatement.setTimestamp(1, new Timestamp(value.getLong("stat_time")));
                preparedStatement.setString(2, value.getString("terminal_type"));
                preparedStatement.setLong(3, value.getLong("shop_id"));
                preparedStatement.setString(4, value.getString("traffic_source_first"));
                preparedStatement.setString(5, value.getString("traffic_source_second"));
                preparedStatement.setLong(6, value.getLong("visitor_count"));
                preparedStatement.setInt(7, value.getInteger("rank_num"));
                preparedStatement.setString(8, value.getString("sub_source_list"));
                if (value.getLong("related_product_id") != null) {
                    preparedStatement.setLong(9, value.getLong("related_product_id"));
                } else {
                    preparedStatement.setNull(9, java.sql.Types.BIGINT);
                }
                preparedStatement.setLong(10, value.getLong("product_visitor_count"));
                preparedStatement.setBigDecimal(11, value.getBigDecimal("product_pay_amount"));
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("写入MySQL失败: " + value.toJSONString(), e);
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    // MySQL Sink for Keyword Ranking Data
    private static class MysqlKeywordRankingSink extends RichSinkFunction<JSONObject> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                String sql = "INSERT INTO ads_keyword_ranking (stat_time, stat_dimension, terminal_type, shop_id, keyword, " +
                        "search_visitor_count, rank_num, click_visitor_count, create_time, update_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "search_visitor_count=VALUES(search_visitor_count), click_visitor_count=VALUES(click_visitor_count), " +
                        "update_time=NOW()";
                preparedStatement = connection.prepareStatement(sql);
            } catch (Exception e) {
                throw new RuntimeException("MySQL连接失败", e);
            }
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            try {
                preparedStatement.setDate(1, (Date) value.getDate("stat_time"));
                preparedStatement.setString(2, value.getString("stat_dimension"));
                preparedStatement.setString(3, value.getString("terminal_type"));
                preparedStatement.setLong(4, value.getLong("shop_id"));
                preparedStatement.setString(5, value.getString("keyword"));
                preparedStatement.setLong(6, value.getLong("search_visitor_count"));
                preparedStatement.setInt(7, value.getInteger("rank_num"));
                preparedStatement.setLong(8, value.getLong("click_visitor_count"));
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("写入MySQL失败: " + value.toJSONString(), e);
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    // MySQL Sink for Product Traffic Ranking Data
    private static class MysqlProductTrafficRankingSink extends RichSinkFunction<JSONObject> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                String sql = "INSERT INTO ads_product_traffic_ranking (stat_time, terminal_type, shop_id, product_id, visitor_count, " +
                        "rank_num, page_view, avg_stay_time, add_cart_count, collection_count, pay_buyer_count, " +
                        "pay_amount, pay_conversion_rate, traffic_source_json, create_time, update_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "visitor_count=VALUES(visitor_count), page_view=VALUES(page_view), avg_stay_time=VALUES(avg_stay_time), " +
                        "add_cart_count=VALUES(add_cart_count), collection_count=VALUES(collection_count), " +
                        "pay_buyer_count=VALUES(pay_buyer_count), pay_amount=VALUES(pay_amount), " +
                        "pay_conversion_rate=VALUES(pay_conversion_rate), traffic_source_json=VALUES(traffic_source_json), " +
                        "update_time=NOW()";
                preparedStatement = connection.prepareStatement(sql);
            } catch (Exception e) {
                throw new RuntimeException("MySQL连接失败", e);
            }
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            try {
                preparedStatement.setTimestamp(1, new Timestamp(value.getLong("stat_time")));
                preparedStatement.setString(2, value.getString("terminal_type"));
                preparedStatement.setLong(3, value.getLong("shop_id"));
                preparedStatement.setLong(4, value.getLong("product_id"));
                preparedStatement.setLong(5, value.getLong("visitor_count"));
                preparedStatement.setInt(6, value.getInteger("rank_num"));
                preparedStatement.setLong(7, value.getLong("page_view"));
                preparedStatement.setInt(8, value.getInteger("avg_stay_time"));
                preparedStatement.setLong(9, value.getLong("add_cart_count"));
                preparedStatement.setLong(10, value.getLong("collection_count"));
                preparedStatement.setLong(11, value.getLong("pay_buyer_count"));
                preparedStatement.setBigDecimal(12, value.getBigDecimal("pay_amount"));
                preparedStatement.setBigDecimal(13, value.getBigDecimal("pay_conversion_rate"));
                preparedStatement.setString(14, value.getString("traffic_source_json"));
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("写入MySQL失败: " + value.toJSONString(), e);
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    // MySQL Sink for Page Traffic Ranking Data
    private static class MysqlPageTrafficRankingSink extends RichSinkFunction<JSONObject> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                String sql = "INSERT INTO ads_page_traffic_ranking (stat_time, terminal_type, shop_id, page_id, visitor_count, " +
                        "rank_num, page_view, click_count, avg_stay_time, module_click_json, guide_product_json, " +
                        "trend_data_json, create_time, update_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "visitor_count=VALUES(visitor_count), page_view=VALUES(page_view), click_count=VALUES(click_count), " +
                        "avg_stay_time=VALUES(avg_stay_time), module_click_json=VALUES(module_click_json), " +
                        "guide_product_json=VALUES(guide_product_json), trend_data_json=VALUES(trend_data_json), " +
                        "update_time=NOW()";
                preparedStatement = connection.prepareStatement(sql);
            } catch (Exception e) {
                throw new RuntimeException("MySQL连接失败", e);
            }
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            try {
                preparedStatement.setTimestamp(1, new Timestamp(value.getLong("stat_time")));
                preparedStatement.setString(2, value.getString("terminal_type"));
                preparedStatement.setLong(3, value.getLong("shop_id"));
                preparedStatement.setLong(4, value.getLong("page_id"));
                preparedStatement.setLong(5, value.getLong("visitor_count"));
                preparedStatement.setInt(6, value.getInteger("rank_num"));
                preparedStatement.setLong(7, value.getLong("page_view"));
                preparedStatement.setLong(8, value.getLong("click_count"));
                preparedStatement.setInt(9, value.getInteger("avg_stay_time"));
                preparedStatement.setString(10, value.getString("module_click_json"));
                preparedStatement.setString(11, value.getString("guide_product_json"));
                preparedStatement.setString(12, value.getString("trend_data_json"));
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("写入MySQL失败: " + value.toJSONString(), e);
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    // MySQL Sink for Crowd Feature Data
    private static class MysqlCrowdFeatureSink extends RichSinkFunction<JSONObject> {
        private Connection connection;
        private PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
                String sql = "INSERT INTO ads_crowd_feature (stat_time, terminal_type, shop_id, crowd_type, gender, age_range, " +
                        "city, taoqi_value, crowd_count, total_crowd_count, crowd_ratio, create_time, update_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW()) " +
                        "ON DUPLICATE KEY UPDATE " +
                        "crowd_count=VALUES(crowd_count), total_crowd_count=VALUES(total_crowd_count), " +
                        "crowd_ratio=VALUES(crowd_ratio), update_time=NOW()";
                preparedStatement = connection.prepareStatement(sql);
            } catch (Exception e) {
                throw new RuntimeException("MySQL连接失败", e);
            }
        }

        @Override
        public void invoke(JSONObject value, Context context) throws Exception {
            try {
                preparedStatement.setTimestamp(1, new Timestamp(value.getLong("stat_time")));
                preparedStatement.setString(2, value.getString("terminal_type"));
                preparedStatement.setLong(3, value.getLong("shop_id"));
                preparedStatement.setString(4, value.getString("crowd_type"));
                preparedStatement.setString(5, value.getString("gender"));
                preparedStatement.setString(6, value.getString("age_range"));
                preparedStatement.setString(7, value.getString("city"));
                preparedStatement.setInt(8, value.getInteger("taoqi_value"));
                preparedStatement.setLong(9, value.getLong("crowd_count"));
                preparedStatement.setLong(10, value.getLong("total_crowd_count"));
                preparedStatement.setBigDecimal(11, value.getBigDecimal("crowd_ratio"));
                preparedStatement.executeUpdate();
            } catch (Exception e) {
                throw new RuntimeException("写入MySQL失败: " + value.toJSONString(), e);
            }
        }

        @Override
        public void close() throws Exception {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
