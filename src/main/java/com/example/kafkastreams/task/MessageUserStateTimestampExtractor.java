package com.example.kafkastreams.task;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MessageUserStateTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        MessageUserState messageUserState = (MessageUserState) record.value();
        return messageUserState.getTimestamp().toEpochMilli();
    }
}
