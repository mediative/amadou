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

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.HttpClients
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write}

object Slack {
  case class PostException(msg: String) extends RuntimeException(msg)

  case class Payload(
      channel: String,
      text: String,
      username: String,
      icon_emoji: String,
      link_names: Boolean)
}

/**
 * Wrap the functionality to send messages to a Slack channel.
 */
case class Slack(url: String, channel: String, user: String, icon: String) extends Logging {
  import Slack._

  implicit val formats = Serialization.formats(NoTypeHints)

  /**
   * Post the given message to a Slack channel.
   *
   * @param message the text of the message to send.
   * @param icon optional icon for the message.
   */
  def post(message: String, icon: String = this.icon): Unit = {
    val payload = Payload(channel, message, user, icon, true)
    logger.info(s"Posting $payload to $url")

    val client        = HttpClients.createDefault()
    val requestEntity = new StringEntity(write(payload), ContentType.APPLICATION_JSON)
    val postMethod    = new HttpPost(url)
    postMethod.setEntity(requestEntity)

    val response = client.execute(postMethod)
    client.close()
    val status = response.getStatusLine
    if (status.getStatusCode != 200)
      throw PostException(
        s"$url replied with status ${status.getStatusCode}: ${status.getReasonPhrase}")
  }
}
