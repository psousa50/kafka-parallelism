package com.psousa50.kafka

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.Deserializer

open class JacksonKafkaSerializer<T> : Serializer<T> {
    private val objectMapper = jacksonObjectMapper()

    override fun serialize(topic: String?, data: T?): ByteArray? {
        return objectMapper.writeValueAsBytes(data)
    }
}

open class JacksonKafkaDeserializer<T>(private val targetType: Class<T>) : Deserializer<T> {
    private val objectMapper = jacksonObjectMapper()

    override fun deserialize(topic: String?, data: ByteArray?): T? {
        return data?.let { objectMapper.readValue(it, targetType) }
    }
}