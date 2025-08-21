package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficDisplayProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject display = jsonObject.getJSONObject("display");

            JSONObject result = new JSONObject();
            result.put("mid", common.getString("mid"));
            result.put("user_id", common.getString("user_id"));
            result.put("item", display.getString("item"));
            result.put("item_type", display.getString("item_type"));
            result.put("pos_id", display.getString("pos_id"));
            result.put("pos_seq", display.getString("pos_seq"));
            result.put("ts", jsonObject.getLong("ts"));

            out.collect(result.toJSONString());
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}
