/*
 * Copyright 2017 Mediative
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mediative.amadou
package monitoring

import java.util.Properties
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

class KafkaMessagingSystem(config: Config) extends MessagingSystem with Logging {
  private val properties  = KafkaMessagingSystem.readProperties(config)
  private val producer    = new KafkaProducer[String, String](properties)
  private val topicPrefix = properties.getProperty("topic.prefix")

  override def publish(topic: String, message: String): Unit = {
    val topicName = s"$topicPrefix-$topic"

    logger.info(s"Publishing to $topicName :\n$message\n")

    producer.send(new ProducerRecord[String, String](topicName, message), new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        if (exception != null) {
          logger
            .error(s"Cannot publish to $topicName. Caused by: ${exception.getMessage}", exception)
        }
    })
    ()
  }

  override def stop(): Unit =
    producer.close()
}

object KafkaMessagingSystem {
  def readProperties(config: Config): Properties = {
    val propertiesKeys = Seq(
      "bootstrap.servers",
      "acks",
      "retries",
      "batch.size",
      "linger.ms",
      "buffer.memory",
      "key.serializer",
      "value.serializer",
      "topic.prefix")

    val properties = new Properties()
    propertiesKeys.foreach(key => properties.setProperty(key, config.getString(key)))

    properties
  }
}
