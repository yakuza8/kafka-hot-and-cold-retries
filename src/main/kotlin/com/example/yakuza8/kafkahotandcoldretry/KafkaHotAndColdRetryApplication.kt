package com.example.yakuza8.kafkahotandcoldretry

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaHotAndColdRetryApplication

fun main(args: Array<String>) {
	runApplication<KafkaHotAndColdRetryApplication>(*args)
}
