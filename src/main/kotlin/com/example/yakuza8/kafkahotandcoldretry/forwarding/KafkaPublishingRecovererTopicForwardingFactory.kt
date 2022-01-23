package com.example.yakuza8.kafkahotandcoldretry.forwarding

import com.example.yakuza8.kafkahotandcoldretry.config.KafkaConsumerConfig
import com.example.yakuza8.kafkahotandcoldretry.forwarding.strategy.DeadLetterTopicForwardingStrategy
import com.example.yakuza8.kafkahotandcoldretry.forwarding.strategy.TopicForwardingStrategy
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.support.serializer.DeserializationException
import java.lang.Exception

class KafkaPublishingRecovererTopicForwardingFactory {
    /**
     * If we cannot process the record or cannot find its properties (it includes cold retry as well)
     * - Then forward the topic to the dead letter queue
     *
     * Otherwise, check if we hit the limit for max attempt of the retrial queue
     * - If not reached yet, then forward to the message to the same topic one more time by incrementing attempt count
     * - Otherwise, forward the topic to the next retry queue
     *
     */
    companion object {
        fun decideTopicForwardingStrategy(
            record: ConsumerRecord<*, *>,
            exception: Exception,
            consumerConfig: KafkaConsumerConfig
        ): TopicForwardingStrategy {

            // No need to propagate into middle topics if we cannot deserialize the topic already
            if (exception.cause is DeserializationException) {
                return DeadLetterTopicForwardingStrategy(deadLetterTopicName = getDeadLetterTopicName(consumerConfig))
            }

            // If no configuration is found or no cold retry is wanted, then forward dead letter again
            val topicProperties = consumerConfig.topics.values.firstOrNull { it.topicName == record.topic() }
            if (topicProperties == null || topicProperties.coldRetry == null) {
                return DeadLetterTopicForwardingStrategy(deadLetterTopicName = getDeadLetterTopicName(consumerConfig))
            }

            // Note that header could be null at this point
            val attemptCountHeader = record.headers().lastHeader(ATTEMPT_COUNT_HEADER_KEY)
            val attemptCount =
                if (attemptCountHeader == null) INITIAL_ATTEMPT_INDEX else readValueFromHeader(attemptCountHeader.value()).toInt()

            // We set the initial attempt as zero that is why we should one less maximum attempt
            if (attemptCount < topicProperties.coldRetry.count - 1) {
                return ColdRetrySameTopicForwardingStrategy(
                    record = record,
                    attemptCount = attemptCount,
                    interval = topicProperties.coldRetry.interval
                )
            }
            return ColdRetryNextTopicForwardingStrategy(
                record = record,
                topicProperties = topicProperties,
                consumerConfig = consumerConfig
            )
        }

        fun getDeadLetterTopicName(consumerConfig: KafkaConsumerConfig): String {
            return consumerConfig.topics.get("dead-letter")!!.topicName
        }
    }
}