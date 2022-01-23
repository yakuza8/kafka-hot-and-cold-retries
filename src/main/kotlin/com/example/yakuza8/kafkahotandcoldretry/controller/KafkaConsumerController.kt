package com.example.yakuza8.kafkahotandcoldretry.controller

import com.example.yakuza8.kafkahotandcoldretry.common.Constants.Companion.ATTEMPT_COUNT_HEADER_KEY
import com.example.yakuza8.kafkahotandcoldretry.common.Constants.Companion.NACK_SEEK_PERIOD
import com.example.yakuza8.kafkahotandcoldretry.common.Constants.Companion.NEXT_PROCESSING_TIME_HEADER_KEY
import com.example.yakuza8.kafkahotandcoldretry.common.Utilities.Companion.readValueFromHeader
import com.example.yakuza8.kafkahotandcoldretry.service.KafkaConsumerService
import net.logstash.logback.argument.StructuredArguments
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.messaging.handler.annotation.Headers
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

@Component
class KafkaConsumerController(
    @Autowired private val kafkaConsumerService: KafkaConsumerService
) {
    @KafkaListener(
        topics = ["\${spring.kafka.consumer.topics.main.topic-name}"],
        containerFactory = "paymentVendorNotificationEventKafkaMainListenerContainerFactory"
    )
    fun consumeFromMainTopic(event: String) {
        logAndTrackMessages(consumerName = "main")
        kafkaConsumerService.processPaymentVendorNotificationEvent(event = event)
    }

    @KafkaListener(
        topics = ["\${spring.kafka.consumer.topics.retry-1.topic-name}"],
        containerFactory = "paymentVendorNotificationEventKafkaRetry1ListenerContainerFactory"
    )
    fun consumeFromRetry1Topic(
        event: String,
        @Headers kafkaHeaders: Map<String, ByteArray>,
        acknowledgment: Acknowledgment
    ) {
        if (shouldIgnoreEventByNextProcessingTime(kafkaHeaders, acknowledgment))
            return
        logAndTrackMessages(consumerName = "retry-1", kafkaHeaders = kafkaHeaders)
        kafkaConsumerService.processPaymentVendorNotificationEvent(event = event, acknowledgment = acknowledgment)
    }

    @KafkaListener(
        topics = ["\${spring.kafka.consumer.topics.retry-2.topic-name}"],
        containerFactory = "paymentVendorNotificationEventKafkaRetry2ListenerContainerFactory"
    )
    fun consumeFromRetry2Topic(
        event: String,
        @Headers kafkaHeaders: Map<String, ByteArray>,
        acknowledgment: Acknowledgment
    ) {
        if (shouldIgnoreEventByNextProcessingTime(kafkaHeaders, acknowledgment))
            return
        logAndTrackMessages(consumerName = "retry-2", kafkaHeaders = kafkaHeaders)
        kafkaConsumerService.processPaymentVendorNotificationEvent(event = event, acknowledgment = acknowledgment)
    }


    /**
     * The procedure which checks the obtained topic's next processing time and return negative acknowledgement with
     * respect to not reaching out to that time. Note that, the header values are passed as byte array that is why we
     * need to cast them into <code>String</code> first and then <code>Instant</code>.
     *
     * Kindly see that we provide amount of time when to poll the same record in tne next cycle. There is a minimum
     * value assigned by us which should be less than <code>max.poll.interval.ms</code> in order not to get re-balance
     * error from Kafka (because of blocking on the processing thread)
     */
    private fun shouldIgnoreEventByNextProcessingTime(
        kafkaHeaders: Map<String, ByteArray>,
        acknowledgment: Acknowledgment
    ): Boolean {
        val nextProcessingTimeAsBytes = kafkaHeaders.get(NEXT_PROCESSING_TIME_HEADER_KEY)
        nextProcessingTimeAsBytes?.let {
            val now = Instant.now()
            val nextProcessingTimeAsInstant = Instant.parse(readValueFromHeader(it))
            if (now < nextProcessingTimeAsInstant) {
                val waitPeriod = Duration.between(now, nextProcessingTimeAsInstant).toMillis()
                acknowledgment.nack(Math.min(NACK_SEEK_PERIOD, waitPeriod))
                return true
            }
        }
        return false
    }

    private fun getCustomHeadersFromTopic(kafkaHeaders: Map<String, ByteArray>?): Map<String, String> {
        return kafkaHeaders?.filterKeys {
            it == ATTEMPT_COUNT_HEADER_KEY || it == NEXT_PROCESSING_TIME_HEADER_KEY
        }!!.mapValues { readValueFromHeader(it.value) }
    }

    private fun logAndTrackMessages(
        consumerName: String,
        kafkaHeaders: Map<String, ByteArray>? = null,
    ) {
        logger.info(
            "Read a topic from $consumerName queue",
            StructuredArguments.kv("headers", getCustomHeadersFromTopic(kafkaHeaders = kafkaHeaders))
        )
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(KafkaConsumerController::class.java)
    }
}