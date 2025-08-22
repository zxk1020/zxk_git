package com.retailersv1.func;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficActionProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject common = jsonObject.getJSONObject("common");

            // 处理 actions 数组
            if (jsonObject.getJSONArray("action") != null) {
                JSONArray actions = jsonObject.getJSONArray("action");
                for (int i = 0; i < actions.size(); i++) {
                    JSONObject action = actions.getJSONObject(i);

                    JSONObject result = new JSONObject();
                    result.put("mid", common.getString("mid"));
                    result.put("user_id", common.getString("uid"));  // 修改字段名 uid
                    result.put("action_id", action.getString("action_id"));
                    result.put("item", action.getString("item"));
                    result.put("item_type", action.getString("item_type"));
                    result.put("ts", action.getLong("ts"));  // 使用action中的ts而不是jsonObject的ts

                    out.collect(result.toJSONString());
                }
            }
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

