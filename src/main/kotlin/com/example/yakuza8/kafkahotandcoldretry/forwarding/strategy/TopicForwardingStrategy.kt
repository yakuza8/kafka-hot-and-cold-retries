package com.example.yakuza8.kafkahotandcoldretry.forwarding.strategy

interface TopicForwardingStrategy {
    fun decideDestinationTopic(): String
}