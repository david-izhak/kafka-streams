package com.example.kafkastreams.task;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageUserBalance {
    String userId;
    Double accountBalance;
    Instant timestamp;
}

