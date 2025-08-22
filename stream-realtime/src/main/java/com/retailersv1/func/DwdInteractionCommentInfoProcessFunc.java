package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdInteractionCommentInfoProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject data = jsonObject.getJSONObject("after");

            JSONObject result = new JSONObject();
            result.put("id", data.getString("id"));
            result.put("user_id", data.getString("user_id"));
            result.put("sku_id", data.getString("sku_id"));
            result.put("appraise", data.getString("appraise"));
            result.put("appraise_name", data.getString("appraise")); // 原数据中没有appraise_name，使用appraise
            result.put("comment_txt", data.getString("comment_txt"));
            result.put("create_time", data.getLong("create_time")); // 修改为Long类型
            result.put("ts", jsonObject.getLong("ts_ms")); // 字段名修正

            out.collect(result.toJSONString());
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

