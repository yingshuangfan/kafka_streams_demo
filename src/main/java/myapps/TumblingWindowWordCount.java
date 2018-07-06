package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class TumblingWindowWordCount {

    public static void main(String[] args) throws Exception, IllegalArgumentException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final StreamsBuilder builder = new StreamsBuilder();

        // TODO: 将步骤拆开
        final KStream<String, String> plainText = builder.stream("streams-plaintext-input");
        final KStream<String, String> splitWords = plainText.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")));
        final KGroupedStream<String, String> groupedWords = splitWords.groupBy((key, value) -> value);

        // TODO: 对时间窗聚合
        long windowSizeMs = TimeUnit.MINUTES.toMillis(1); // 1 * 60 * 1000L(ms)
        final TimeWindowedKStream<String, String> windowedGroupedWords = groupedWords.windowedBy(TimeWindows.of(windowSizeMs).advanceBy(windowSizeMs));
        final KTable<Windowed<String>, Long> wordsCountPerMinutes = windowedGroupedWords.aggregate(
                () -> 0L, /* initializer */
                (aggKey, newValue, aggValue) -> add(aggValue),
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("new-counts-store").withValueSerde(Serdes.Long())
        );

        // TODO: 聚合结果console展示
        final KStream<String, Long> wordsCountStream = wordsCountPerMinutes.toStream((windowedWord, count) -> windowedWord.toString());
        wordsCountStream.to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Long add(Long aggValue) {
        return aggValue + 1L;
    }
}
