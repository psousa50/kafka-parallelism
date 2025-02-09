package com.psousa50.kafka

data class SomeMessage(
    val id: String,
    val createdAt: Long,
    )

class SomeMessageSerializer : JacksonKafkaSerializer<SomeMessage>()
class SomeMessageDeserializer : JacksonKafkaDeserializer<SomeMessage>(SomeMessage::class.java)