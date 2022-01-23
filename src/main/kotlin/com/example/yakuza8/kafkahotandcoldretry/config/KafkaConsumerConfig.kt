package com.example.yakuza8.kafkahotandcoldretry.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties(prefix = "spring.kafka.consumer")
data class KafkaConsumerConfig(
    val bootstrapServers: String,
    val groupId: String,
    val autoOffsetReset: String,
    val topics: Map<String, TopicProperties>
) {
    data class TopicProperties(
        val topicName: String,
        val consumerCount: Int = 1,
        val isManuallyAcknowledged: Boolean = false,
        val hotRetry: HotRetryProperties = HotRetryProperties(),
        val coldRetry: ColdRetryProperties?
    ) {
        /**
         * Note that, usage of the default values will cause not having hot retry logic indeed where
         * you will have fixed backoff with zero amount of maximum retry
         */
        data class HotRetryProperties(
            val count: Long = 0,
            val interval: Long = 0
        )

        /**
         * Note that, usage the default values will cause not having cold retry logic. In normal
         * circumstances, the cold retry logic will be applied as the following:
         *
         * - If any amount of cold retry count defined, then failing on the same topic will lead
         * you to send the same message into the same topic up to maximum amount of retries.
         * - If maximum amount of count has been reached, then topic will be forwarded to the next
         * topic already provided by the configuration
         *
         * @param interval Possibly long amount of time given in ms that will cause message
         * re-population on the same topic instead of fixed backoff style of retrying.
         */
        data class ColdRetryProperties(
            val nextTopic: String,
            val count: Long = 0,
            val interval: Long = 0
        )
    }
}
