package com.psousa50

import com.psousa50.kafka.MessageCompleted
import com.psousa50.kafka.MessageReceived
import com.psousa50.kafka.toCompleted
import java.util.*

class MessageProcessor(val processingTime: Long) {

    private val messagesReceived = Collections.synchronizedList(mutableListOf<MessageCompleted>())

    fun processMessage(messageReceived: MessageReceived) {
        Thread.sleep(processingTime)
        messagesReceived.add(messageReceived.toCompleted(System.currentTimeMillis()))
    }

    fun totalTime() =
        synchronized(messagesReceived) {
            if (messagesReceived.isEmpty()) 0
            else
                messagesReceived.maxOf { it.completedAt } -
                    messagesReceived.minOf { it.message.createdAt }
        }

    val messages get() = messagesReceived.toList()
    fun count() = messagesReceived.size
}
