package com.retailersv1.func;

import com.alibaba.fastjson.JSONArray;
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

            // 处理 displays 数组
            if (jsonObject.getJSONArray("displays") != null) {
                JSONArray displays = jsonObject.getJSONArray("displays");
                for (int i = 0; i < displays.size(); i++) {
                    JSONObject display = displays.getJSONObject(i);

                    JSONObject result = new JSONObject();
                    result.put("mid", common.getString("mid"));
                    result.put("user_id", common.getString("uid"));  // 修改字段名 uid
                    result.put("item", display.getString("item"));
                    result.put("item_type", display.getString("item_type"));
                    result.put("pos_id", display.getInteger("pos_id"));  // 修改为Integer类型
                    result.put("pos_seq", display.getInteger("pos_seq"));  // 修改为Integer类型
                    result.put("ts", jsonObject.getLong("ts"));
                    result.put("page_id", jsonObject.getJSONObject("page").getString("page_id"));  // 添加page_id

                    out.collect(result.toJSONString());
                }
            }
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}

