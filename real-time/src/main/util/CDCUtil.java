//package Real-Time.src.main.java.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class CDCUtil {


    public static DataStreamSource<String> getMysqlSource(StreamExecutionEnvironment env,String databaseName) {

        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode", "double");
        properties.setProperty("time.precision.mode", "connect");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("node00")
                .port(3306)
                .debeziumProperties(properties)
                .databaseList(databaseName) // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .startupOptions(StartupOptions.earliest())
//                .startupOptions(StartupOptions.latest())  ToDo 增量
                .tableList(
                        databaseName + ".*"
                ) // 设置捕获的表
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();
        env.setParallelism(1);
        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

     return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
    }


}
