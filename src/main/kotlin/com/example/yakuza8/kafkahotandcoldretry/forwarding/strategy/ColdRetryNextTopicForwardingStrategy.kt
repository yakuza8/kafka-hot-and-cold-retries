package com.example.yakuza8.kafkahotandcoldretry.forwarding.strategy

import com.example.yakuza8.kafkahotandcoldretry.common.Constants.Companion.ATTEMPT_COUNT_HEADER_KEY
import com.example.yakuza8.kafkahotandcoldretry.common.Constants.Companion.INITIAL_ATTEMPT_INDEX
import com.example.yakuza8.kafkahotandcoldretry.common.Constants.Companion.NEXT_PROCESSING_TIME_HEADER_KEY
import com.example.yakuza8.kafkahotandcoldretry.common.Utilities.Companion.convertValueToByteArray
import com.example.yakuza8.kafkahotandcoldretry.common.Utilities.Companion.getNextProcessingTime
import com.example.yakuza8.kafkahotandcoldretry.config.KafkaConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Instant

class ColdRetryNextTopicForwardingStrategy(
    val record: ConsumerRecord<*, *>,
    val topicProperties: KafkaConsumerConfig.TopicProperties,
    val consumerConfig: KafkaConsumerConfig
) : TopicForwardingStrategy {
    override fun decideDestinationTopic(): String {
        val nextTopicProperties = consumerConfig.topics.get(topicProperties.coldRetry!!.nextTopic)!!
        val headers = record.headers()
        // Reset the attempt count in the next new topic
        headers.add(ATTEMPT_COUNT_HEADER_KEY, convertValueToByteArray(INITIAL_ATTEMPT_INDEX))
        headers.add(
            NEXT_PROCESSING_TIME_HEADER_KEY,
            convertValueToByteArray(
                getNextProcessingTime(
                    from = Instant.now(),
                    interval = nextTopicProperties.coldRetry?.interval
                )
            )
        )
        // And redirect to the next topic
        return nextTopicProperties.topicName
    }
}