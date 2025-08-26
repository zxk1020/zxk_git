package com.flinkgd03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

/**
 * @author hp
 */
public class UserLoginStatistics {
    // 日期格式化
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 生成模拟数据
        DataStream<Tuple4<String, String, String, Integer>> loginStream = env.addSource(new LoginSource())
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.INT));

        // 3. 按用户ID分组
        KeyedStream<Tuple4<String, String, String, Integer>, String> keyedStream =
                loginStream.keyBy(record -> record.f0);

        // 4. 使用状态编程判断新老用户，并同时输出用户详细信息
        DataStream<String> userDetailStream = keyedStream.map(
                new RichMapFunction<Tuple4<String, String, String, Integer>, String>() {

                    // 存储用户首次登录日期的状态
                    private ValueState<String> firstLoginDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态
                        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>(
                                "firstLoginDateState",
                                String.class
                        );
                        // 设置状态TTL为7天，避免状态无限增长
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(7))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        descriptor.enableTimeToLive(ttlConfig);
                        firstLoginDateState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public String map(Tuple4<String, String, String, Integer> record) throws Exception {
                        String userId = record.f0;
                        String deviceId = record.f1;
                        String loginDate = record.f2;

                        // 获取状态中存储的首次登录日期
                        String firstLoginDate = firstLoginDateState.value();

                        int isNew;
                        if (firstLoginDate == null) {
                            // 首次登录，标记为新用户
                            isNew = 1;
                            firstLoginDateState.update(loginDate);
                            // 输出用户首次登录信息
                            return "用户 " + userId + " (设备: " + deviceId + ") 于 " + loginDate + " 首次登录，标记为新用户";
                        } else if (firstLoginDate.equals(loginDate)) {
                            // 当天首次登录，标记为新用户
                            isNew = 1;
                            return "用户 " + userId + " (设备: " + deviceId + ") 于 " + loginDate + " 当天首次登录，标记为新用户";
                        } else {
                            // 非首次登录，标记为老用户
                            isNew = 0;
                            return "用户 " + userId + " (设备: " + deviceId + ") 于 " + loginDate + " 登录，标记为老用户 (首次登录: " + firstLoginDate + ")";
                        }
                    }
                }
        ).returns(Types.STRING);

        // 打印用户详细信息
        userDetailStream.print("用户登录详情");

        // 5. 统计每天的新老用户数量
        DataStream<Tuple4<String, String, String, Integer>> processedStream = keyedStream.map(
                new RichMapFunction<Tuple4<String, String, String, Integer>, Tuple4<String, String, String, Integer>>() {

                    // 存储用户首次登录日期的状态
                    private ValueState<String> firstLoginDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态
                        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>(
                                "firstLoginDateState",
                                String.class
                        );
                        // 设置状态TTL为7天，避免状态无限增长
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(7))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build();
                        descriptor.enableTimeToLive(ttlConfig);
                        firstLoginDateState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple4<String, String, String, Integer> map(
                            Tuple4<String, String, String, Integer> record) throws Exception {

                        String userId = record.f0;
                        String deviceId = record.f1;
                        String loginDate = record.f2;

                        // 获取状态中存储的首次登录日期
                        String firstLoginDate = firstLoginDateState.value();

                        int isNew;
                        if (firstLoginDate == null) {
                            // 首次登录，标记为新用户
                            isNew = 1;
                            firstLoginDateState.update(loginDate);
                        } else if (firstLoginDate.equals(loginDate)) {
                            // 当天首次登录，标记为新用户
                            isNew = 1;
                        } else {
                            // 非首次登录，标记为老用户
                            isNew = 0;
                        }

                        return new Tuple4<>(userId, deviceId, loginDate, isNew);
                    }
                }
        ).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.INT));

        // 统计每天的新老用户数量
        DataStream<Tuple2<String, Integer>> countedStream = processedStream
                .map(record -> Tuple2.of(record.f2 + "-" + record.f3, 1)) // (日期-是否新用户, 1)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private ValueState<Integer> countState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                                "dailyCountState",
                                Integer.class
                        );
                        countState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        Integer currentCount = countState.value();
                        if (currentCount == null) {
                            currentCount = 0;
                        }
                        currentCount += value.f1;
                        countState.update(currentCount);
                        return Tuple2.of(value.f0, currentCount);
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 使用处理时间窗口收集并排序统计结果
        countedStream
                .map(tuple -> {
                    String[] parts = tuple.f0.split("-");
                    String date = parts[0];
                    int isNew = Integer.parseInt(parts[1]);
                    String userType = isNew == 1 ? "新用户" : "老用户";
                    return Tuple2.of(date, date + " " + userType + "数量: " + tuple.f1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .print("每日统计");

        // 执行程序
        env.execute("用户登录新老用户统计");
    }

    // 生成模拟数据的Source
    public static class LoginSource implements SourceFunction<Tuple4<String, String, String, Integer>> {
        private boolean isRunning = true;
        private final Random random = new Random();
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        // 生成未来5天的日期
        private List<String> generateDates() {
            List<String> dates = new ArrayList<>();
            Calendar calendar = Calendar.getInstance();

            for (int i = 0; i < 5; i++) {
                dates.add(sdf.format(calendar.getTime()));
                calendar.add(Calendar.DAY_OF_YEAR, 1);
            }

            return dates;
        }

        @Override
        public void run(SourceContext<Tuple4<String, String, String, Integer>> ctx) throws Exception {
            List<String> dates = generateDates();
            int userCount = 30; // 共30个用户

            // 生成100条登录记录
            for (int i = 0; i < 100 && isRunning; i++) {
                // 随机生成用户ID (1-30)
                String userId = "user_" + (random.nextInt(userCount) + 1);

                // 随机生成设备ID
                String deviceId = "device_" + (random.nextInt(50) + 1);

                // 随机选择一个日期
                String loginDate = dates.get(random.nextInt(dates.size()));

                // 初始is_new设为0，后续会在处理中修正
                ctx.collect(new Tuple4<>(userId, deviceId, loginDate, 0));

                // 模拟数据流，每100ms发送一条
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // 工具方法：将日期字符串转换为毫秒
    public static long dateToTs(String dateStr) throws ParseException {
        return sdf.parse(dateStr).getTime();
    }

    // 工具方法：将毫秒转换为日期字符串
    public static String tsToDate(long ts) {
        return sdf.format(new Date(ts));
    }
}
