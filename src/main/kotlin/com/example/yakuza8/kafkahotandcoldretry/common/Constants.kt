package com.example.yakuza8.kafkahotandcoldretry.common

class Constants {
    companion object {
        /**
         * Kafka custom header fields and related constants
         */
        const val INITIAL_ATTEMPT_INDEX = 0
        const val ATTEMPT_COUNT_HEADER_KEY = "attempt_count"
        const val NEXT_PROCESSING_TIME_HEADER_KEY = "next_processing_time"

        // Kafka listener/consumer configuration constants
        /**
         * 30 seconds long idle period to seek next topic.
         *
         * Attention: it should be less than <code>max.poll.interval.ms</code> otherwise Kafka thread will be blocked
         * too much and will raise re-balance errors
         */
        const val NACK_SEEK_PERIOD = 30000L
    }
}