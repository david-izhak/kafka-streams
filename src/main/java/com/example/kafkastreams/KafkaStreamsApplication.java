package com.example.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Instant;
import java.util.Objects;

@SpringBootApplication
public class KafkaStreamsApplication {

    public enum UserState {
        ACTIVE,
        SUSPENDED,
        BLOCKED

    }

    private static class Message {
        private String userId;
        private UserState state;
        private Instant timestamp;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args);

        StreamsBuilder builder = new StreamsBuilder();

        // Создаем KTable для хранения состояния пользователя
        StoreBuilder<KeyValueStore<String, UserState>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("user-state-store"),
                // Указываем сериализаторы для ключа и значения, если необходимо
                Serdes.String(),
                Serdes.serdeFrom(UserState.class)
        );
        builder.addStateStore(storeBuilder);

        // Создаем KStream для обработки входящих сообщений
        KStream<String, Message> inputStream = builder.stream("your-topic", Consumed.with(Serdes.String(), Serdes.serdeFrom(Message.class)));

        // Преобразуем KStream в KTable, используя userId в качестве ключа
        KTable<String, UserState> userStateTable = inputStream
                .groupByKey()
                .reduce((value1, value2) -> determineLatestUserState(value1, value2), Materialized.as("user-state-store"));


        // Преобразуем KTable в новый KStream, если необходимо
        KStream<String, UserState> userStateStream = userStateTable.toStream();


        // Обработка KStream
        userStateStream.foreach((key, value) -> {
            // Ваш код для обновления состояния пользователя
            updateUserStateBasedOnMessage(key, value);
        });

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), /* ваш конфигурационный объект */);
        kafkaStreams.start();

        StoreQueryParameters<ReadOnlyKeyValueStore<String, String>> storeQueryParameters = StoreQueryParameters.fromNameAndType(
                "user-state-store", QueryableStoreTypes.keyValueStore());
        ReadOnlyKeyValueStore<String, String> keyValueStore = kafkaStreams.store(storeQueryParameters);
    }

    public UserState getLatestUserState(String userId, KTable<String, UserState> userStateTable) {
        // Ваш код для получения состояния пользователя
        return userStateTable.filter((key, value) -> Objects.equals(key, userId)).toStream().
    }

    private static UserState determineLatestUserState(Message value1, Message value2) {
        // Ваш код для определения последнего состояния пользователя на основе двух сообщений
        return value2.timestamp.isAfter(value1.timestamp) ? value2.state : value1.state;
    }

    private static void updateUserStateBasedOnMessage(String userId, UserState state) {
        // Ваш код для обновления состояния пользователя на основе сообщения
    }

}
