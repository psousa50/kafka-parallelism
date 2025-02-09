package com.psousa50.kafka

import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic

object kafkaProperties {
    const val BOOTSTRAP_SERVERS = "localhost:12300"
}

fun createTopic(topicName: String, numPartitions: Int, replicationFactor: Short = 1) {
    val adminClient =
        AdminClient.create(
            mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaProperties.BOOTSTRAP_SERVERS)
        )

    val existingTopics = adminClient.listTopics().names().get()
    if (existingTopics.contains(topicName)) {
        error("Topic '$topicName' already exists.")
    }

    val newTopic = NewTopic(topicName, numPartitions, replicationFactor)
    adminClient.createTopics(listOf(newTopic)).all().get(10, TimeUnit.SECONDS)

    println("Topic '$topicName' created with $numPartitions partitions.")
    adminClient.close()
}

fun Long.prettyTime(): String {
    return String.format(
        "%02d:%02d:%02d.%03d",
        TimeUnit.MILLISECONDS.toHours(this) % TimeUnit.DAYS.toHours(1),
        TimeUnit.MILLISECONDS.toMinutes(this) % TimeUnit.HOURS.toMinutes(1),
        TimeUnit.MILLISECONDS.toSeconds(this) % TimeUnit.MINUTES.toSeconds(1),
        TimeUnit.MILLISECONDS.toMillis(this) % TimeUnit.SECONDS.toMillis(1)
    )
}

fun Long.prettyDuration(): String {
    return String.format(
        "%d.%d",
        TimeUnit.MILLISECONDS.toSeconds(this) % TimeUnit.MINUTES.toSeconds(1),
        TimeUnit.MILLISECONDS.toMillis(this) % TimeUnit.SECONDS.toMillis(1)
    )
}
