package com.example.yakuza8.kafkahotandcoldretry.common

import java.time.Instant

class Utilities {
    companion object {
        fun <T> convertValueToByteArray(value: T): ByteArray {
            return value.toString().toByteArray()
        }

        fun readValueFromHeader(value: ByteArray): String {
            return String(value)
        }

        fun getNextProcessingTime(from: Instant, interval: Long?): Instant {
            val period = interval ?: 0L
            return from.plusMillis(period)
        }
    }
}