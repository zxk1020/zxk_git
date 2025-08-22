package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficStartProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject start = jsonObject.getJSONObject("start");  // 原代码错误，应为start而不是start

            JSONObject result = new JSONObject();
            result.put("mid", common.getString("mid"));
            result.put("user_id", common.getString("uid"));  // 修改字段名 uid
            result.put("channel", common.getString("ch"));  // 修改字段名 ch
            result.put("start_type", start.getString("entry"));  // 修改字段名 entry
            result.put("ts", jsonObject.getLong("ts"));
            result.put("is_new", common.getString("is_new"));
            // 添加额外字段
            result.put("loading_time", start.getLong("loading_time"));

            out.collect(result.toJSONString());
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

