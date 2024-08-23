package io.streamnative.nlu.pulsarflink;

import java.io.FileNotFoundException;
import java.util.Map;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.PulsarSource;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountPulsar {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountPulsar.class);

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        PulsarSource<String> pulsarSource = getPulsarSource(params);
        DataStreamSource<String> text = env.fromSource(pulsarSource, WatermarkStrategy.noWatermarks(), "Pulsar Source");

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .name("tokenizer")
                        .keyBy(value -> value.f0)
                        .sum(1);

        counts.print();

        env.execute("Pulsar Source");
    }

    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    System.out.println(token);
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    private static PulsarSource<String> getPulsarSource(ParameterTool params)
            throws JsonProcessingException, FileNotFoundException {

        Map<String, String> env = System.getenv();
        String serviceUrl = env.get("brokerServiceURL");
        String adminUrl = env.get("webServiceURL");
        String authPlugin = env.get("authPlugin");
        String authParams = env.get("authParams");
        String topicName = params.get("topic");
        String subscriptionName = params.get("subName");

        return PulsarSource.builder()
                .setServiceUrl(serviceUrl)
//                .setAdminUrl(adminUrl)
                .setStartCursor(StartCursor.earliest())
                .setTopics(topicName)
                //.setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
                .setDeserializationSchema(new SimpleStringSchema())
                .setSubscriptionName(subscriptionName)

                .setConfig(PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME, authPlugin)
                .setConfig(PulsarOptions.PULSAR_AUTH_PARAMS, authParams)
                .build();
    }
}