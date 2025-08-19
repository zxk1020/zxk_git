package sql_servers;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @BelongsProject: zxk_git
 * @BelongsPackage: sql_servers
 * @Author: zhuxiangkuan
 * @CreateTime: 2025-07-24  22:46
 * @Description: TODO
 * @Version: 1.0
 */
public class TestSqlServerCDC {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties debeziumProperties = new Properties();

        debeziumProperties.put("database.trustServerCertificate", "true");
//        debeziumProperties.put("snapshot.mode", "schema_only");

        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("database.history", "io.debezium.relational.history.MemoryDatabaseHistory");
        debeziumProperties.put("snapshot.mode", "initial");

        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("cdh01")
                .port(1433)
                .username("sa")
                .password("z342756.") //lwj200499.
                .database("test")
                .tableList("dbo.test_cdc")
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "SQL Server CDC Source");
        dataStreamSource.print().setParallelism(1);

        env.execute("SQL Server CDC Test");


    }
}
