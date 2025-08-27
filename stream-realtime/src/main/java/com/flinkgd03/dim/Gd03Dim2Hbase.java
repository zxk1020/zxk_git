package com.flinkgd03.dim;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.HbaseUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 将DIM层数据同步到HBase
 */
public class Gd03Dim2Hbase {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 初始化HBase表结构
        HbaseUtils hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
        hbaseUtils.initGd03DimTables();

        String database = ConfigUtils.getString("mysql.databases.conf"); // 使用配置中的数据库名

        // 同步各个DIM表数据到HBase
        syncDimTable(env, database, "product", "dim_product", "product_id");
        syncDimTable(env, database, "page", "dim_page", "page_id");
        syncDimTable(env, database, "traffic_source", "dim_traffic_source", "source_id");
        syncDimTable(env, database, "city", "dim_city", "city_id");
        syncDimTable(env, database, "taoqi_level", "dim_taoqi_level", "level_id");

        env.execute("Gd03 DIM to HBase");
    }

    /**
     * 同步单个DIM表到HBase
     */
    private static void syncDimTable(StreamExecutionEnvironment env, String database,
                                     String sourceTableName, String dimTableName, String rowKeyField) throws Exception {
        MySqlSource<String> source = CdcSourceUtils.getMySQLCdcSource(
                database,
                database + "." + sourceTableName,  // 使用实际的源表名
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                "6000-7000",
                StartupOptions.earliest()
        );

        DataStreamSource<String> stream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                sourceTableName + "_source");

        stream.map(JSONObject::parseObject)
                .addSink(new Dim2HBaseSinkFunction(dimTableName, rowKeyField))
                .name("sink_" + dimTableName)
                .uid("sink_" + dimTableName);
    }

    /**
     * DIM数据写入HBase的Sink函数
     */
    public static class Dim2HBaseSinkFunction extends RichSinkFunction<JSONObject> {
        private final String dimTableName;
        private final String rowKeyField;
        private HbaseUtils hbaseUtils;
        private org.apache.hadoop.hbase.client.Connection connection;

        public Dim2HBaseSinkFunction(String dimTableName, String rowKeyField) {
            this.dimTableName = dimTableName;
            this.rowKeyField = rowKeyField;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            hbaseUtils = new HbaseUtils(ConfigUtils.getString("zookeeper.server.host.list"));
            connection = hbaseUtils.getConnection();
        }

        @Override
        public void invoke(JSONObject jsonObject, Context context) throws Exception {
            String op = jsonObject.getString("op");

            // 只处理创建、更新、读取操作，忽略删除操作
            if (!"d".equals(op)) {
                JSONObject data = jsonObject.getJSONObject("after");
                if (data == null) {
                    data = jsonObject.getJSONObject("before");
                }

                if (data != null) {
                    String rowKey = data.getString(rowKeyField);
                    if (rowKey != null) {
                        Table table = connection.getTable(TableName.valueOf("FlinkGd03:" + dimTableName));
                        Put put = new Put(Bytes.toBytes(rowKey));

                        // 将所有字段添加到info列族中
                        for (String key : data.keySet()) {
                            Object value = data.get(key);
                            if (value != null) {
                                put.addColumn(Bytes.toBytes("info"),
                                        Bytes.toBytes(key),
                                        Bytes.toBytes(value.toString()));
                            }
                        }

                        table.put(put);
                        table.close();
                        System.out.println("成功写入数据到HBase表: " + dimTableName + ", RowKey: " + rowKey);
                    }
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }
}
