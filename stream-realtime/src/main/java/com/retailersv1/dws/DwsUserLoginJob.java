// DwsUserLoginJob.java
package com.retailersv1.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * @author hp
 */
public class DwsUserLoginJob {

    public void process(StreamExecutionEnvironment env) {
        try {
            // 从DWD层读取页面浏览数据
            DataStreamSource<String> pageDs = env.fromSource(
                    KafkaUtils.buildKafkaSource(
                            com.stream.common.utils.ConfigUtils.getString("kafka.bootstrap.servers"),
                            com.stream.common.utils.ConfigUtils.getString("kafka.dwd.page.topic"),
                            "dws_user_login_group_" + System.currentTimeMillis(),
                            OffsetsInitializer.earliest()
                    ),
                    WatermarkStrategy.forMonotonousTimestamps(),
                    "read_dwd_page"
            );

            // 解析并过滤登录行为数据
            DataStream<JSONObject> loginDs = pageDs
                    .map(value -> {
                        try {
                            return JSON.parseObject(value);
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(json -> json != null && isLoginBehavior(json))
                    .name("filter_login_behavior");

            // 全局窗口聚合用户登录统计
            DataStream<String> userLoginWindowDs = loginDs
                    .windowAll(TumblingEventTimeWindows.of(Time.minutes(5))) // 5分钟窗口
                    .aggregate(
                            new UserLoginAggregateFunction(),
                            new UserLoginProcessWindowFunction()
                    );

            // 添加打印语句
            userLoginWindowDs.print("👤 用户域-登录统计").setParallelism(1);

            // 写入Doris - 修改后
            userLoginWindowDs.sinkTo(DorisSinkUtils.buildDorisSink("dws_user_user_login_window"))
                    .name("sink_user_login_to_doris")
                    .uid("dws_user_login_doris_sink");

            System.out.println("✅ 用户域处理完成");

        } catch (Exception e) {
            System.err.println("❌ 用户域处理异常: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 判断是否为登录行为
    private static boolean isLoginBehavior(JSONObject json) {
        try {
            String pageId = json.getJSONObject("page").getString("page_id");
            return "login".equals(pageId);
        } catch (Exception e) {
            return false;
        }
    }

    // 聚合函数：统计用户登录情况
    private static class UserLoginAggregateFunction implements org.apache.flink.api.common.functions.AggregateFunction<JSONObject, Tuple2<Set<String>, Set<String>>, Tuple2<Integer, Integer>> {
        @Override
        public Tuple2<Set<String>, Set<String>> createAccumulator() {
            return new Tuple2<>(new HashSet<>(), new HashSet<>()); // (所有用户, 回流用户)
        }

        @Override
        public Tuple2<Set<String>, Set<String>> add(JSONObject value, Tuple2<Set<String>, Set<String>> accumulator) {
            String userId = value.getJSONObject("common").getString("user_id");
            String isNew = value.getJSONObject("common").getString("is_new");

            // 统计所有用户
            accumulator.f0.add(userId);

            // 统计回流用户（非新用户）
            if ("0".equals(isNew)) {
                accumulator.f1.add(userId);
            }

            return accumulator;
        }

        @Override
        public Tuple2<Integer, Integer> getResult(Tuple2<Set<String>, Set<String>> accumulator) {
            return new Tuple2<>(accumulator.f1.size(), accumulator.f0.size()); // (回流用户数, 独立用户数)
        }

        @Override
        public Tuple2<Set<String>, Set<String>> merge(Tuple2<Set<String>, Set<String>> a, Tuple2<Set<String>, Set<String>> b) {
            Set<String> allUsers = new HashSet<>(a.f0);
            allUsers.addAll(b.f0);

            Set<String> backUsers = new HashSet<>(a.f1);
            backUsers.addAll(b.f1);

            return new Tuple2<>(allUsers, backUsers);
        }
    }

    // 窗口处理函数：生成最终结果
    private static class UserLoginProcessWindowFunction extends ProcessAllWindowFunction<Tuple2<Integer, Integer>, String, TimeWindow> {
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        public void process(Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<String> out) {
            Tuple2<Integer, Integer> stats = elements.iterator().next();
            TimeWindow window = context.window();

            JSONObject result = new JSONObject();
            result.put("stt", new Timestamp(window.getStart()).toString());
            result.put("edt", new Timestamp(window.getEnd()).toString());
            result.put("cur_date", dateFormat.format(new Date(window.getStart())));
            result.put("back_ct", stats.f0); // 回流用户数
            result.put("uu_ct", stats.f1);   // 独立用户数

            out.collect(result.toJSONString());
        }
    }
}
