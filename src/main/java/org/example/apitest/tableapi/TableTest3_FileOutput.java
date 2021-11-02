package org.example.apitest.tableapi;

import org.apache.flink.formats.csv.CsvFormatFactory;
import org.apache.flink.table.api.*;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PATH;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.SINK_PARALLELISM;

/**
 * @author zm
 */
public class TableTest3_FileOutput {

    public static void main(String[] args) throws Exception {
        final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        final TableEnvironment tEnv = TableEnvironment.create(settings);

        final Schema schema = Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .column("temp", DataTypes.DOUBLE())
                .build();
        tEnv.createTemporaryTable("sensor", TableDescriptor.forConnector("filesystem")
                .schema(schema)
                .option(PATH, "file:///C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt")
                .format(CsvFormatFactory.IDENTIFIER)
                .build());
        tEnv.createTemporaryTable("output", TableDescriptor.forConnector("filesystem")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("temperature", DataTypes.DOUBLE())
                        .build())
                .option(PATH, "file:///C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\out")
                .option(SINK_PARALLELISM, 1)
                .format(CsvFormatFactory.IDENTIFIER)
                .build());

        final Table output = tEnv.from("sensor")
                .select($("id"), $("temp").as("temperature"));
        output.executeInsert("output");
    }
}
