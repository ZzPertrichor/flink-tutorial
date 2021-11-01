package org.example.apitest.bean;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

/**
 * 传感器温度读数
 *
 * @author zm
 * @since 2021-10-20
 */
public class SensorReading implements Serializable {

    private String id;
    private Long timestamp;
    private Double temperature;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public static SensorReading fromText(String text) {
        final String[] fields = text.split(",");
        return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
    }

    public static Tuple2<String, Double> toTuple(SensorReading sr) {
        return new Tuple2<>(sr.getId(), sr.getTemperature());
    }

    /**
     * maximal and latest
     */
    public static SensorReading reduce(SensorReading sr1, SensorReading sr2) {
        return new SensorReading(sr1.getId(), sr2.getTimestamp(),
                Math.max(sr1.getTemperature(), sr2.getTemperature()));
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
