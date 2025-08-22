package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficPageProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");

            JSONObject result = new JSONObject();
            result.put("mid", common.getString("mid"));
            result.put("user_id", common.getString("uid"));  // 修改字段名 uid
            result.put("page_id", page.getString("page_id"));
            result.put("during_time", page.getLong("during_time"));  // 修改为Long类型
            result.put("channel", common.getString("ch"));  // 修改字段名 ch
            result.put("ts", jsonObject.getLong("ts"));
            result.put("is_new", common.getString("is_new"));
            // 添加额外字段
            result.put("last_page_id", page.getString("last_page_id"));
            result.put("item", page.getString("item"));
            result.put("item_type", page.getString("item_type"));

            out.collect(result.toJSONString());
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}
