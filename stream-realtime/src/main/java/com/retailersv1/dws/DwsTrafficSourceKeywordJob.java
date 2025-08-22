// DwsTrafficSourceKeywordJob.java
package com.retailersv1.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author hp
 */
public class DwsTrafficSourceKeywordJob {

    public void process(StreamExecutionEnvironment env) {
        try {
            // 从DWD层读取页面浏览数据
            DataStreamSource<String> pageDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            com.stream.common.utils.ConfigUtils.getString("kafka.bootstrap.servers"),
                            com.stream.common.utils.ConfigUtils.getString("kafka.dwd.page.topic"),
                            "dws_keyword_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.forMonotonousTimestamps(),
                    "read_dwd_page"
            );

            // 解析并过滤包含搜索关键词的数据
            DataStream<JSONObject> keywordDs = pageDs
                    .map(value -> {
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(json -> json != null && json.getString("search_keyword") != null)
                    .name("filter_search_keyword");

            // 按窗口聚合统计关键词出现次数
            DataStream<String> keywordWindowDs = keywordDs
                    .keyBy(json -> json.getString("search_keyword"))
                    .window(TumblingEventTimeWindows.of(Time.minutes(5))) // 5分钟窗口
                    .aggregate(
                            new KeywordAggregateFunction(),
                            new KeywordProcessWindowFunction()
                    );

            // 添加打印语句
            keywordWindowDs.print("🔍 流量域-关键词统计").setParallelism(1);

            // 写入Doris - 修改后
            keywordWindowDs.sinkTo(DorisSinkUtils.buildDorisSink("dws_traffic_source_keyword_page_view_window"))
                    .name("sink_keyword_to_doris")
                    .uid("dws_keyword_doris_sink");

            System.out.println("✅ 流量域处理完成");

        } catch (Exception e) {
            System.err.println("❌ 流量域处理异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 聚合函数：统计关键词出现次数
    private static class KeywordAggregateFunction implements AggregateFunction<JSONObject, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(JSONObject value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    // 窗口处理函数：生成最终结果
    private static class KeywordProcessWindowFunction extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void process(String key, Context context, Iterable<Long> elements, Collector<String> out) {
            long count = elements.iterator().next();
            TimeWindow window = context.window();

            JSONObject result = new JSONObject();
            result.put("stt", new Timestamp(window.getStart()).toString());
            result.put("edt", new Timestamp(window.getEnd()).toString());
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));
            result.put("keyword", key);
            result.put("keyword_count", count);

            out.collect(result.toJSONString());
        }
    }
}
