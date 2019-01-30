package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class WordProcessor {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "0.0.0.0:9092");
        properties.setProperty("group.id", "log-word-analysis");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("log-analysis", new SimpleStringSchema(), properties));

        stream.map((MapFunction<String, Object>) s -> s.toUpperCase()).print();

        env.execute();
    }
}
