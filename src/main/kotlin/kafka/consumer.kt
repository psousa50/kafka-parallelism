package com.psousa50.kafka

import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

class SomeMessageConsumer(
    private val bootstrapServer: String,
    private val name: String,
    private val groupId: String,
    private val topic: String,
    private val executor: ExecutorService?,
    private val processMessage: (MessageReceived) -> Unit
) {
    fun start() {
        println("Starting consumer $name with group $groupId in topic $topic")
        val consumer = createKafkaConsumer()
        consumer.subscribe(listOf(topic))

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            val assignedPartitions = consumer.assignment().map { it.partition() }.toSet()
            println(
                "${System.currentTimeMillis().prettyTime()}: Received ${records.count()} records in consumer $name (partitions: $assignedPartitions)"
            )
            records.forEach {
                executor?.submit {
                    buildAndProcessMessage(it, name, assignedPartitions, processMessage)
                } ?: buildAndProcessMessage(it, name, assignedPartitions, processMessage)
            }
        }
    }

    private fun createKafkaConsumer(): KafkaConsumer<String, SomeMessage> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SomeMessageDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1")
        }

        return KafkaConsumer(props)
    }

    private fun buildAndProcessMessage(
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
}

open class MessageBase(
    val message: SomeMessage,
    val consumer: String,
    val partitions: Set<Int>,
    val partition: Int,
    val receivedAt: Long,
)

open class MessageReceived(
    message: SomeMessage,
    consumer: String,
    partitions: Set<Int>,
    partition: Int,
    receivedAt: Long,
) : MessageBase(message, consumer, partitions, partition, receivedAt) {
    override fun toString() =
        "Message ${message.id} created at ${message.createdAt.prettyTime()} received at ${receivedAt.prettyTime()} by consumer $consumer in partition $partition (partitions: $partitions)"
}

class MessageCompleted(
    message: SomeMessage,
    consumer: String,
    partitions: Set<Int>,
    partition: Int,
    receivedAt: Long,
    val completedAt: Long,
) : MessageReceived(message, consumer, partitions, partition, receivedAt) {
    private val duration: Long
        get() = completedAt - message.createdAt

    override fun toString() =
        "Message ${message.id} created at ${message.createdAt.prettyTime()} received at ${receivedAt.prettyTime()} completed at ${completedAt.prettyTime()} by consumer $consumer in partition $partition processed in ${duration.prettyDuration()} (partitions: $partitions)"

}

fun MessageReceived.toCompleted(completedAt: Long) = MessageCompleted(message, consumer, partitions, partition, receivedAt, completedAt)