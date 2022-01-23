package com.example.yakuza8.kafkahotandcoldretry.forwarding.strategy

class DeadLetterTopicForwardingStrategy(
    val deadLetterTopicName: String
) : TopicForwardingStrategy {
    override fun decideDestinationTopic(): String {
        return deadLetterTopicName
    }
}
