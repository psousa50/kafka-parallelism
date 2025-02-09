package com.psousa50.kafka

import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

fun createConsumer(groupId: String): KafkaConsumer<String, SomeMessage> {
    val props = Properties().apply {
        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.BOOTSTRAP_SERVERS)
        put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SomeMessageDeserializer::class.java.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
    }

    return KafkaConsumer(props)
}

fun startConsumer(
    name: String,
    groupId: String,
    topic: String,
    executor: ExecutorService?,
    processor: (MessageReceived) -> Unit
) {
    println("Starting consumer $name for topic $topic")
    val consumer = createConsumer(groupId)
    consumer.subscribe(listOf(topic))
    Thread.sleep(1000)

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        val assignedPartitions = consumer.assignment().map { it.partition() }.toSet()
        println(
            "${System.currentTimeMillis().prettyTime()}: Received ${records.count()} records in consumer $name (partitions: $assignedPartitions)"
        )
        records.forEach {
            executor?.submit {
                processMessage(it, name, assignedPartitions, processor)
            } ?: processMessage(it, name, assignedPartitions, processor)
        }
    }
}

private fun processMessage(
    record: ConsumerRecord<String, SomeMessage>,
    name: String,
    assignedPartitions: Set<Int>,
    processor: (MessageReceived) -> Unit
) {
    val message = record.value()
    val messageReceived = MessageReceived(
        message = message,
        receivedAt = System.currentTimeMillis(),
        consumer = name,
        partitions = assignedPartitions,
        partition = record.partition()
    )
    processor(messageReceived)
}

data class MessageReceived(
    val message: SomeMessage,
    val receivedAt: Long,
    val consumer: String,
    val partitions: Set<Int>,
    val partition: Int
) {
    val duration: Long
        get() = receivedAt - message.createdAt

    override fun toString() =
        "Message ${message.id} created at ${message.createdAt.prettyTime()} received at ${receivedAt.prettyTime()} by consumer $consumer in partition $partition processed in ${duration.prettyDuration()} (partitions: $partitions)"
}
