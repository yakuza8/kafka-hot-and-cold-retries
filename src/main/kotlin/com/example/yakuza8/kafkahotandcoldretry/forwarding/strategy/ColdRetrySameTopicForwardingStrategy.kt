package com.example.yakuza8.kafkahotandcoldretry.forwarding.strategy

import com.example.yakuza8.kafkahotandcoldretry.common.Constants.Companion.ATTEMPT_COUNT_HEADER_KEY
import com.example.yakuza8.kafkahotandcoldretry.common.Constants.Companion.NEXT_PROCESSING_TIME_HEADER_KEY
import com.example.yakuza8.kafkahotandcoldretry.common.Utilities.Companion.convertValueToByteArray
import com.example.yakuza8.kafkahotandcoldretry.common.Utilities.Companion.getNextProcessingTime
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Instant

class ColdRetrySameTopicForwardingStrategy(
    val record: ConsumerRecord<*, *>,
    val attemptCount: Int,
    val interval: Long
) : TopicForwardingStrategy {
    override fun decideDestinationTopic(): String {
        val headers = record.headers()
        // Set attempt count one more in the next cycle
        headers.add(ATTEMPT_COUNT_HEADER_KEY, convertValueToByteArray(attemptCount + 1))
        headers.add(
            NEXT_PROCESSING_TIME_HEADER_KEY,
            convertValueToByteArray(getNextProcessingTime(from = Instant.now(), interval = interval))
        )
        // And forward to the same topic
        return record.topic()
    }
}