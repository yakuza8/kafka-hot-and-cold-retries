package com.example.yakuza8.kafkahotandcoldretry.factory

import com.example.yakuza8.kafkahotandcoldretry.config.KafkaConsumerConfig
import com.example.yakuza8.kafkahotandcoldretry.forwarding.KafkaPublishingRecovererTopicForwardingFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaOperations
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.SeekToCurrentErrorHandler
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
import org.springframework.util.backoff.FixedBackOff

@EnableKafka
@Configuration
class KafkaConsumerFactory(private val consumerConfig: KafkaConsumerConfig) {
    private fun getCommonConsumerConfigs(): Map<String, Any> =
        mutableMapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to consumerConfig.bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to consumerConfig.groupId,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to consumerConfig.autoOffsetReset
        )

    /**
     * Generic factory to process events
     */
    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        val props = getCommonConsumerConfigs()
        val paymentVendorNotificationEventDeserializer = StringDeserializer()

        // Deserializer that catches exceptions while delegating the existing deserializer
        val errorHandlingDeserializer = ErrorHandlingDeserializer(paymentVendorNotificationEventDeserializer)

        return DefaultKafkaConsumerFactory(props, StringDeserializer(), errorHandlingDeserializer)
    }

    /**
     * Generic consumer factory creator where you need to give the <code>topicKey</code> as defined
     * inside the <code>application.yml</code>
     *
     * It tries to set hot retry logic by default and handles topic forwarding logic inside
     * <code>deadLetterPublishingRecoverer</code>
     */
    fun createPaymentVendorNotificationEventKafkaListenerContainerFactory(
        topicKey: String,
        consumerFactory: ConsumerFactory<String, String>,
        deadLetterPublishingRecoverer: DeadLetterPublishingRecoverer
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        // Make sure that the topic exists in the application configurations
        val topicProperties = consumerConfig.topics.get(topicKey)!!

        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory
        factory.setConcurrency(topicProperties.consumerCount)
        factory.setErrorHandler(
            SeekToCurrentErrorHandler(
                deadLetterPublishingRecoverer,
                FixedBackOff(
                    topicProperties.hotRetry.interval,
                    topicProperties.hotRetry.count
                )
            )
        )
        if (topicProperties.isManuallyAcknowledged)
            factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE
        return factory
    }

    @Bean
    fun paymentVendorNotificationEventKafkaMainListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>,
        deadLetterPublishingRecoverer: DeadLetterPublishingRecoverer
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        return createPaymentVendorNotificationEventKafkaListenerContainerFactory(
            topicKey = "main",
            consumerFactory = consumerFactory,
            deadLetterPublishingRecoverer = deadLetterPublishingRecoverer
        )
    }

    @Bean
    fun paymentVendorNotificationEventKafkaRetry1ListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>,
        deadLetterPublishingRecoverer: DeadLetterPublishingRecoverer
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        return createPaymentVendorNotificationEventKafkaListenerContainerFactory(
            topicKey = "retry-1",
            consumerFactory = consumerFactory,
            deadLetterPublishingRecoverer = deadLetterPublishingRecoverer
        )
    }

    @Bean
    fun paymentVendorNotificationEventKafkaRetry2ListenerContainerFactory(
        consumerFactory: ConsumerFactory<String, String>,
        deadLetterPublishingRecoverer: DeadLetterPublishingRecoverer
    ): ConcurrentKafkaListenerContainerFactory<String, String> {
        return createPaymentVendorNotificationEventKafkaListenerContainerFactory(
            topicKey = "retry-2",
            consumerFactory = consumerFactory,
            deadLetterPublishingRecoverer = deadLetterPublishingRecoverer
        )
    }

    @Bean
    fun deadLetterPublishingRecoverer(kafkaOperations: KafkaOperations<String, String>): DeadLetterPublishingRecoverer {
        return DeadLetterPublishingRecoverer(kafkaOperations) { record, exception ->
            // Get the proper forwarding strategy
            val topicForwardingStrategy = KafkaPublishingRecovererTopicForwardingFactory
                .decideTopicForwardingStrategy(record, exception, consumerConfig)

            // Chosen to forward to the same partition if exists
            TopicPartition(
                topicForwardingStrategy.decideDestinationTopic(), record.partition()
            )
        }
    }

    @Bean
    fun deadLetterProducerFactory(): ProducerFactory<String, String> {
        val props = mutableMapOf<String, Any>(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to consumerConfig.bootstrapServers
        )
        return DefaultKafkaProducerFactory(props, StringSerializer(), StringSerializer())
    }

    @Bean
    fun deadLetterKafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(deadLetterProducerFactory())
    }
}