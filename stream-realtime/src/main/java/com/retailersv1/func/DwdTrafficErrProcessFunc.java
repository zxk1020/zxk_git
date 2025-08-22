package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdTrafficErrProcessFunc extends ProcessFunction<String, String> {
    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(value);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject err = jsonObject.getJSONObject("err");

            JSONObject result = new JSONObject();
            result.put("mid", common.getString("mid"));
            result.put("user_id", common.getString("uid"));  // 修改字段名 uid
            result.put("err_code", err.getInteger("error_code"));  // 修改字段名 error_code并改为Integer类型
            result.put("err_msg", err.getString("msg"));  // 修改字段名 msg
            result.put("ts", jsonObject.getLong("ts"));

            out.collect(result.toJSONString());
        } catch (Exception e) {
            ctx.output(new OutputTag<String>("dirty") {}, value);
        }
    }
}
