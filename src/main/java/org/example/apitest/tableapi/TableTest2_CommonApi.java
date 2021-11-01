package org.example.apitest.tableapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author zm
 */
public class TableTest2_CommonApi {

    public static void main(String[] args) {
        final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        final TableEnvironment env = TableEnvironment.create(settings);

        String filePath = "C:\\Users\\pactera\\Documents\\WorkSpace\\flink-tutorial\\src\\main\\resources\\sensor.txt";
        env.createTemporaryTable("sensor",
                TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder().build())
                        .option("path", filePath)
                        .build());

    }
}
