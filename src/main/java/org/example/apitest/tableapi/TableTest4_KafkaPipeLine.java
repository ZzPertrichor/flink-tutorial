package org.example.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @author zm
 */
public class TableTest4_KafkaPipeLine {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporaryTable("source", TableDescriptor.forConnector("kafka")
                .option(PROPS_BOOTSTRAP_SERVERS.key(), "172.27.186.224:9092")
                .option(TOPIC.key(), "source")
                .option(PROPS_GROUP_ID.key(), "group-source")
                .option(SCAN_STARTUP_MODE.key(), ScanStartupMode.LATEST_OFFSET.toString())
                .format("csv")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("timestamp", DataTypes.BIGINT())
                        .column("temp", DataTypes.DOUBLE())
                        .build())
                .build());

        final Table source = tEnv.from("source");
        final Table output = source.select($("id"), $("temp"));

        tEnv.createTemporaryTable("sink", TableDescriptor.forConnector("kafka")
                .option(PROPS_BOOTSTRAP_SERVERS.key(), "172.27.186.224:9092")
                .option(TOPIC.key(), "sink")
                .option(PROPS_GROUP_ID.key(), "group-sink")
                .option(SCAN_STARTUP_MODE.key(), ScanStartupMode.LATEST_OFFSET.toString())
                .format("csv")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("temp", DataTypes.DOUBLE())
                        .build())
                .build());

        output.executeInsert("sink");
    }
}
