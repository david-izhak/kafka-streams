package com.example.kafkastreams.task;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class StreamsSerdes {

    public static Serde<MessageUserState> messageUserStateSerde() {
        return new MessageUserStateSerde();
    }

    public static final class MessageUserStateSerde extends Serdes.WrapperSerde<MessageUserState> {
        public MessageUserStateSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(MessageUserState.class));
        }
    }

    public static Serde<MessageUserBalance> messageUserBalanceSerde() {
        return new MessageUserBalanceSerde();
    }

    public static final class MessageUserBalanceSerde extends Serdes.WrapperSerde<MessageUserBalance> {
        public MessageUserBalanceSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(MessageUserBalance.class));
        }
    }

    public static Serde<MessageOutput> messageOutputSerde() {
        return new MessageOutputSerde();
    }

    public static final class MessageOutputSerde extends Serdes.WrapperSerde<MessageOutput> {
        public MessageOutputSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(MessageOutput.class));
        }
    }
}
