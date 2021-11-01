package org.example.apitest.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.apitest.bean.SensorReading;
import org.example.apitest.source.SourceTest4_DSF;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zm
 */
public class SinkTest3_JDBC {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);

        env.addSource(new SourceTest4_DSF.SensorSource())
                .addSink(new MysqlRichSinkFunction());

        env.execute();
    }

    public static class MysqlRichSinkFunction extends RichSinkFunction<SensorReading> {
        private Connection connection;
        private PreparedStatement insertPs;
        private PreparedStatement updatePs;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
            insertPs = connection.prepareStatement("insert into sensor_temp (id, temp) values (?, ?)");
            updatePs = connection.prepareStatement("update sensor_temp set temp = ? where id = ?");
        }

        @Override
        public void invoke(SensorReading value, Context context) throws Exception {
            updatePs.setDouble(1, value.getTemperature());
            updatePs.setString(2, value.getId());
            updatePs.execute();
            if (updatePs.getUpdateCount() == 0) {
                insertPs.setString(1, value.getId());
                insertPs.setDouble(2, value.getTemperature());
                insertPs.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertPs.close();
            updatePs.close();
            connection.close();
        }
    }
}
