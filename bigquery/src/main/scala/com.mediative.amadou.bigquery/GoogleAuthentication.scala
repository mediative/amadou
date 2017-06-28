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

package com.mediative.amadou.bigquery

import java.io.{File, FileReader}
import scala.collection.JavaConversions._
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver
import com.google.api.client.googleapis.auth.oauth2.{
  GoogleAuthorizationCodeFlow,
  GoogleClientSecrets
}
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import org.apache.spark.sql.SparkSession

sealed abstract class GoogleAuthentication(val scopes: String*)

object GoogleAuthentication {
  lazy val HTTP_TRANSPORT = new NetHttpTransport()
  lazy val JSON_FACTORY   = new JacksonFactory()

  case object Dbm
      extends GoogleAuthentication("https://www.googleapis.com/auth/doubleclickbidmanager")

  def apply(auth: GoogleAuthentication, spark: SparkSession): HttpRequestInitializer = auth match {
    case Dbm =>
      val clientFilePath = spark.conf.get("spark.google.cloud.auth.client.file")
      require(clientFilePath != null, "'google.cloud.auth.client.file' not configured")

      val clientFile = new File(clientFilePath)
      require(clientFile.exists, s"$clientFilePath does not exists")

      val clientSecrets    = GoogleClientSecrets.load(JSON_FACTORY, new FileReader(clientFile))
      val dataStoreFactory = new FileDataStoreFactory(clientFile.getParentFile)

      val flow = new GoogleAuthorizationCodeFlow.Builder(
        HTTP_TRANSPORT,
        JSON_FACTORY,
        clientSecrets,
        auth.scopes)
        .setDataStoreFactory(dataStoreFactory)
        .build()

      val cred = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver())
        .authorize("user")
      new CustomHttpRequestInitializer(cred)
  }

  class CustomHttpRequestInitializer(wrapped: HttpRequestInitializer)
      extends HttpRequestInitializer {
    override def initialize(httpRequest: HttpRequest) = {
      wrapped.initialize(httpRequest)
      httpRequest.setConnectTimeout(10 * 60000) // 10 minutes connect timeout
      httpRequest.setReadTimeout(10 * 60000)    // 10 minutes read timeout
      ()
    }
  }
}
