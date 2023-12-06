package com.example.kafkastreams;

import com.example.kafkastreams.task.KafkaStreamsExecutor;
import com.example.kafkastreams.task.MessageOutput;
import com.example.kafkastreams.task.MessageUserBalance;
import com.example.kafkastreams.task.MessageUserState;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;

import static com.example.kafkastreams.task.UserState.ACTIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class MyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, MessageUserState> inputTopic1;
    private TestInputTopic<String, MessageUserBalance> inputTopic2;
    private TestOutputTopic<String, MessageOutput> outputTopic;

    private final Instant recordBaseTime = Instant.parse("2023-12-06T10:00:00Z");
    private final Duration advance1Min = Duration.ofMinutes(1);

    @BeforeEach
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();

        // Create Actual Stream Processing pipeline
        KafkaStreamsExecutor.createTopology(builder);
        testDriver = new TopologyTestDriver(builder.build(), KafkaStreamsExecutor.getStreamsProperties());

        // Trusted packages added to the serializers

        JsonDeserializer<MessageUserState> valueDeserializer1 = new JsonDeserializer<>();
        valueDeserializer1.addTrustedPackages("com.example.kafkastreams.task");

        JsonDeserializer<MessageUserBalance> valueDeserializer2 = new JsonDeserializer<>();
        valueDeserializer2.addTrustedPackages("com.example.kafkastreams.task");

        JsonDeserializer<MessageOutput> outputValueDeserializer = new JsonDeserializer<>();
        outputValueDeserializer.addTrustedPackages("com.example.kafkastreams.task");


        inputTopic1 = testDriver.createInputTopic(KafkaStreamsExecutor.INPUT_TOPIC_1, new StringSerializer(), new JsonSerializer<>(),
                recordBaseTime, advance1Min);
        inputTopic2 = testDriver.createInputTopic(KafkaStreamsExecutor.INPUT_TOPIC_2, new StringSerializer(), new JsonSerializer<>(),
                recordBaseTime, advance1Min);
        outputTopic = testDriver.createOutputTopic(KafkaStreamsExecutor.OUTPUT_TOPIC, new StringDeserializer(), outputValueDeserializer);
    }

    @AfterEach
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

//    @Test
//    void testOnlyValue() {
//        //Feed 9 as key and word "Hello" as value to inputTopic
//        inputTopic1.pipeInput("abc", "Hello");
//        //Read value and validate it, ignore validation of kafka key, timestamp is irrelevant in this case
//        assertThat(outputTopic.readValue()).isEqualTo("HELLO");
//        //No more output in topic
//        assertThat(outputTopic.isEmpty()).isTrue();
//        inputTopic2.pipeInput("xyz", "Buy");
//        assertThat(outputTopic.isEmpty()).isFalse();
//        assertThat(outputTopic.readValue()).isEqualTo("BUY");
//        assertThat(outputTopic.isEmpty()).isTrue();
//    }

    @Test
    void testReadFromTwoTopics() {
        Instant instant = Instant.now();
        String userId = "abc";
        MessageUserState userStateMessage1 = MessageUserState.builder()
                .userId(userId)
                .state(ACTIVE)
                .timestamp(instant)
                .build();
        MessageOutput messageOutput1 = MessageOutput.builder()
                .userId(userId)
                .state(ACTIVE)
                .accountBalance(null)
                .timestamp(instant)
                .build();
        inputTopic1.pipeInput(userId, userStateMessage1);
        //Read value and validate it, ignore validation of kafka key, timestamp is irrelevant in this case
        assertThat(outputTopic.readValue()).isEqualTo(messageOutput1);
        //No more output in topic
        assertThat(outputTopic.isEmpty()).isTrue();

        MessageUserBalance userBalanceMessage1 = MessageUserBalance.builder()
                .userId(userId)
                .accountBalance(100.0)
                .timestamp(instant)
                .build();
        MessageOutput messageOutput2 = MessageOutput.builder()
                .userId(userId)
                .state(null)
                .accountBalance(100.0)
                .timestamp(instant)
                .build();
        inputTopic2.pipeInput(userId, userBalanceMessage1);
        assertThat(outputTopic.readValue()).isEqualTo(messageOutput2);
        assertThat(outputTopic.isEmpty()).isTrue();
    }

    @Test
    void testReadFromEmptyTopic() {
        Instant instant = Instant.now();
        String userId = "abc";
        MessageUserState userStateMessage1 = MessageUserState.builder()
                .userId(userId)
                .state(ACTIVE)
                .timestamp(instant)
                .build();
        MessageOutput messageOutput1 = MessageOutput.builder()
                .userId(userId)
                .state(ACTIVE)
                .accountBalance(null)
                .timestamp(instant)
                .build();
        inputTopic1.pipeInput(userId, userStateMessage1);

        assertThat(outputTopic.readValue()).isEqualTo(messageOutput1);
        //Reading from empty topic generate Exception
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> {
            outputTopic.readValue();
        }).withMessage("Empty topic: %s", KafkaStreamsExecutor.OUTPUT_TOPIC);
    }

}
