package com.psousa50

import com.psousa50.kafka.MessageReceived
import com.psousa50.kafka.SomeMessage
import com.psousa50.kafka.SomeMessageConsumer
import com.psousa50.kafka.SomeMessageProducer
import com.psousa50.kafka.createTopic
import com.psousa50.kafka.kafkaProperties
import com.psousa50.kafka.prettyDuration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.util.concurrent.Executors

const val NUMBER_OF_MESSAGES_TO_SEND = 10
const val PROCESSING_TIME = 5000L
const val NUMBER_OF_CONSUMERS = 2
const val NUMBER_OF_PARTITIONS = 10
const val NUMBER_OF_GLOBAL_THREADS = 20

fun main() = runBlocking {

    val random = java.util.Random()
    val groupId = "test-group-${random.nextInt(10000)}"
    val topic = "test-topic-${random.nextInt(10000)}"
    createTopic(topic, NUMBER_OF_PARTITIONS)

    val scenario = SimulationScenario(
        groupId = groupId,
        topic = topic,
        numberOfMessagesToSend = NUMBER_OF_MESSAGES_TO_SEND,
        processingTime = PROCESSING_TIME,
        numberOfConsumers = NUMBER_OF_CONSUMERS,
        numberOfThreads = NUMBER_OF_GLOBAL_THREADS,
    )
    val result = runSimulation(scenario)

    println("-------------------------")
    println("-------------------------")
    println("-------------------------")

    result.messages
        .sortedBy { it.message.createdAt }
        .forEach { println("Received message: $it") }

    println("-------------------------")

    println("Total time: ${result.totalTime.prettyDuration()}")
}

private suspend fun runSimulation(
    scenario: SimulationScenario,
) = with(scenario) {
    val executor = lazy { Executors.newFixedThreadPool(NUMBER_OF_GLOBAL_THREADS) }

    val messageProcessor = MessageProcessor(processingTime)
    repeat(numberOfConsumers) {
        val consumer = SomeMessageConsumer(
            bootstrapServer = kafkaProperties.BOOTSTRAP_SERVERS,
            name = "$it",
            groupId = groupId,
            topic = topic,
            executor = if (numberOfThreads > 0) executor.value else null,
            processMessage = messageProcessor::processMessage
        )
        CoroutineScope(Dispatchers.IO).launch { consumer.start() }
    }

    val producer = SomeMessageProducer(topic)

    repeat(numberOfMessagesToSend) {
        val message = SomeMessage(it.toString(), System.currentTimeMillis())
        producer.send(message)
        delay(50)
    }

    withTimeout(100000) {
        while (messageProcessor.count() < 9) {
            delay(500)
        }
    }

    producer.close()
    if (executor.isInitialized()) executor.value.shutdown()
    SimulationResult(
        messages = messageProcessor.messages,
        totalTime = messageProcessor.totalTime()
    )
}


data class SimulationScenario(
    val groupId: String,
    val topic: String,
    val numberOfMessagesToSend: Int,
    val processingTime: Long,
    val numberOfConsumers: Int,
    val numberOfThreads: Int,
)

data class SimulationResult(
    val messages: List<MessageReceived>,
    val totalTime: Long,
)
