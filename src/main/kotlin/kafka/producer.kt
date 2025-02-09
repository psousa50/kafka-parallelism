package com.psousa50.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class SomeMessageProducer(
    private val topic: String,
) {
    private val producer = createProducer()

    fun send(message: SomeMessage) {
        val record = producer.send(producerRecord(message))
        record.get()
    }

    private fun producerRecord(message: SomeMessage) = ProducerRecord(topic, message.id, message)

    private fun createProducer(): KafkaProducer<String, SomeMessage> {
        println("Creating producer")
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.BOOTSTRAP_SERVERS)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SomeMessageSerializer::class.java.name)
        }
        return KafkaProducer(props)
    }

    fun close() {
        producer.close()
    }
}

