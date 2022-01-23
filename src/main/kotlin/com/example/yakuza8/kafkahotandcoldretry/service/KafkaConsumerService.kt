package com.example.yakuza8.kafkahotandcoldretry.service

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Service
import net.logstash.logback.argument.StructuredArguments.kv

@Service
class KafkaConsumerService {

    fun processPaymentVendorNotificationEvent(event: String, acknowledgment: Acknowledgment? = null) {
        try {
            logger.info("Processing event message successfully", kv("payload", event))
        } catch (exception: Exception) {
            logger.error("An error encountered while processing an event message", kv("payload", event), exception)
            throw exception
        } finally {
            /**
             * We need to acknowledge even if any errors since we rely on retries topics to suspend and process failed
             * topics in the next steps.
             *
             * If there is no acknowledgement occur, then the consumers won't commit offsets and cause to re-processing
             * of retried and failed messages in case of restarting the service or consumers    .
             */
            acknowledgment?.acknowledge()
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(KafkaConsumerService::class.java)
    }
}