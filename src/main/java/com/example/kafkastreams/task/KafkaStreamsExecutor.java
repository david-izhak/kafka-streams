package com.example.kafkastreams.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class KafkaStreamsExecutor {

    public static final String APPLICATION_ID_CONFIG = "user_param_combiner_app_id";
    public static final String INPUT_TOPIC_1 = "topic-user-state";
    public static final String INPUT_TOPIC_2 = "topic-user-balance";
    public static final List<String> INPUT_TOPICS = List.of(INPUT_TOPIC_1, INPUT_TOPIC_2);
    public static final String OUTPUT_TOPIC = "out-topic";
    public static final String LOCALHOST_9092 = "localhost:9092";

    public static final String USER_STATE_STORE = "user-state-store";
    public static final String USER_BALANCE_STORE = "user-balance-store";

    // TODO: in development
//    private static ReadOnlyKeyValueStore<String, MessageUserState> userStateStore = null;
//    private static ReadOnlyKeyValueStore<String, MessageUserBalance> userBalanceStore = null;

    public static void main(String[] args) throws InterruptedException {
        final StreamsConfig streamsConfig = getStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();
        createTopology(builder);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);

        // attach shutdown handler to catch control
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("simple-stream-shutdown-hook") {
            @Override
            public void run() {
                log.info("Shutting down the Yelling APP now");
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            kafkaStreams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING && oldState != KafkaStreams.State.RUNNING) {
                    log.debug("Kafka Streams is running!");
                }
            });
            // Get the store
//            userStateStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(USER_STATE_STORE, QueryableStoreTypes.keyValueStore()));
//            userBalanceStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType(USER_BALANCE_STORE, QueryableStoreTypes.keyValueStore()));
            latch.await();
        } catch (final Throwable e) {
            throw new InterruptedException(e.getMessage());
//            System.exit(1);
        }
        System.exit(0);
    }

    static StreamsConfig getStreamsConfig() {
        return new StreamsConfig(getStreamsProperties());
    }

    public static Properties getStreamsProperties() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_CONFIG);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, LOCALHOST_9092);
        return config;
    }

    // TODO: in development

//    public static void createStores(StreamsBuilder builder) {
//        // Create KTable to store user state
//        StoreBuilder<KeyValueStore<String, MessageUserState>> userStateStoreBuilder = Stores.keyValueStoreBuilder(
//                Stores.inMemoryKeyValueStore(USER_STATE_STORE),
//                Serdes.String(),
//                Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>())
//        );
//        builder.addStateStore(userStateStoreBuilder);
//
//        // Create KTable to store user balance
//        StoreBuilder<KeyValueStore<String, MessageUserBalance>> userBalanceStoreBuilder = Stores.keyValueStoreBuilder(
//                Stores.inMemoryKeyValueStore(USER_BALANCE_STORE),
//                Serdes.String(),
//                Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>())
//        );
//        builder.addStateStore(userBalanceStoreBuilder);
//    }

    public static void createTopology(StreamsBuilder builder) {
//        createStores(builder);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<MessageUserState> messageUserStateSerde = StreamsSerdes.messageUserStateSerde();
        final Serde<MessageUserBalance> messageUserBalanceSerde = StreamsSerdes.messageUserBalanceSerde();
        final Serde<MessageOutput> messageOutputSerde = StreamsSerdes.messageOutputSerde();

        KStream<String, MessageUserState> userStateStream = builder.stream(INPUT_TOPIC_1, Consumed.with(stringSerde, messageUserStateSerde));

        // Change KStream into KTable with using userId like a key
        // In this block we check if the new message is newer than the aggregated one using timestamp
        KTable<String, MessageUserState> userStateTable = userStateStream
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                    if (aggValue.timestamp.isBefore(newValue.timestamp)) {
                        return newValue;
                    } else {
                        return aggValue;
                    }
                }, Materialized.as(USER_STATE_STORE));
        userStateTable.toStream().print(Printed.<String, MessageUserState>toSysOut().withLabel("My App User State"));

        KStream<String, MessageUserBalance> userBalanceStream = builder.stream(INPUT_TOPIC_2, Consumed.with(stringSerde, messageUserBalanceSerde));

        // Change KStream into KTable with using userId like a key
        // In this block we check if the new message is newer than the aggregated one using timestamp
        KTable<String, MessageUserBalance> userBalanceTable = userBalanceStream
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                    if (aggValue.timestamp.isBefore(newValue.timestamp)) {
                        return newValue;
                    } else {
                        return aggValue;
                    }
                }, Materialized.as(USER_BALANCE_STORE));
        userBalanceTable.toStream().print(Printed.<String, MessageUserBalance>toSysOut().withLabel("My App User Balance"));

        KStream<String, MessageOutput> messageOutputStateStream = userStateStream
//                .filter((key, value) -> getLatestUserBalance(key) != null)
                .mapValues(messageUserState -> MessageOutput.builder()
                                        .userId(messageUserState.getUserId())
                                        .state(messageUserState.getState())
                                        .accountBalance(null)
                                        .timestamp(messageUserState.getTimestamp())
                                        .build()

                );
        messageOutputStateStream.to(OUTPUT_TOPIC, Produced.with(stringSerde, messageOutputSerde));
        messageOutputStateStream.print(Printed.<String, MessageOutput>toSysOut().withLabel("My App"));


        KStream<String, MessageOutput> messageOutputBalanceStream = userBalanceStream
//                .filter((key, value) -> getLatestUserState(key) != null)
                .mapValues(messageUserBalance -> MessageOutput.builder()
                        .userId(messageUserBalance.getUserId())
                        .state(null)
                        .accountBalance(messageUserBalance.getAccountBalance())
                        .timestamp(messageUserBalance.getTimestamp())
                        .build()
                );
        messageOutputBalanceStream.to(OUTPUT_TOPIC, Produced.with(stringSerde, messageOutputSerde));
        messageOutputBalanceStream.print(Printed.<String, MessageOutput>toSysOut().withLabel("My App"));

    }

    // TODO: in development
//    public static UserState getLatestUserState(String userId) {
//        return userStateStore.get(userId).getState();
//    }
//
//    public static Double getLatestUserBalance(String userId) {
//        return userBalanceStore.get(userId).getAccountBalance();
//    }

}
