package com.psousa50

import com.psousa50.kafka.MessageReceived
import java.util.*

class MessageProcessor(val processingTime: Long) {

  private val messagesReceived = Collections.synchronizedList(mutableListOf<MessageReceived>())

  fun processMessage(messageReceived: MessageReceived) {
    println("Received message: $messageReceived")
    messagesReceived.add(messageReceived)
    Thread.sleep(processingTime)
  }

  fun totalTime() =
      synchronized(messagesReceived) {
        if (messagesReceived.isEmpty()) 0
        else
            messagesReceived.maxOf { it.receivedAt } -
                messagesReceived.minOf { it.message.createdAt }
      }

    val messages get() = messagesReceived
    fun count() = messagesReceived.size
}
