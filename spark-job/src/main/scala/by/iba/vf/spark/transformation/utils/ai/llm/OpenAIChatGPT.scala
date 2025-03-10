/*
 * Copyright (c) 2021 IBA Group, a.s. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package by.iba.vf.spark.transformation.utils.ai.llm

import by.iba.vf.spark.transformation.ResultLogger
import net.liftweb.json
import ujson.Obj
import requests.RequestAuth.Bearer
import requests.{MultiPart, RequestFailedException, Response}

import scala.math.{exp, round}

case class Usage(prompt_tokens: Int, completion_tokens: Int, total_tokens: Int)
case class Message(role: String, content: String)
case class Choice(index: Int, message: Message, logprobs: Option[Int], finish_reason: String)
case class Completion(id: String, model: String, choices: Seq[Choice], usage: Usage, system_fingerprint: Option[String])
case class Transcription(text: String)

object OpenAIChatGPT extends ResultLogger {
  private implicit val Formats: json.DefaultFormats.type = json.DefaultFormats
  private val totalAttempts = 7

  def processRequest(endpoint: String,
                     model: String,
                     apiKey: String,
                     temperature: String,
                     systemMessage: String,
                     userMessage: String): String = {
    logger.info("Processing request to ChatGPT")
    val data = Obj(
      "model" -> model,
      "messages" -> Seq(
        Map(
          "role" -> "system",
          "content" -> systemMessage
        ),
        Map(
          "role" -> "user",
          "content" -> userMessage
        )
      ),
      "temperature" -> temperature.toDouble
    )
    val r = retryRequest(totalAttempts)(tryRequest(data, endpoint, apiKey))
    logger.info(s"Response: status code: ${r.statusCode}, status message: ${r.statusMessage}")
    json.parse(r.text()).extract[Completion].choices.asInstanceOf[List[Choice]].head.message.content
  }

  def processMultiPartRequest(endpoint: String,
                              model: String,
                              apiKey: String,
                              temperature: String,
                              fileName: String,
                              content: Array[Byte]): String = {
    logger.info(s"Processing request to ChatGPT, file $fileName")
    val data = requests.MultiPart(
      requests.MultiItem("model", model),
      requests.MultiItem("temperature", temperature),
      requests.MultiItem("file", content, fileName)
    )
    val r = retryRequest(totalAttempts)(tryMultiPartRequest(data, endpoint, apiKey))
    logger.info(s"Response: status code: ${r.statusCode}, status message: ${r.statusMessage}")
    json.parse(r.text()).extract[Transcription].text
  }

  @annotation.tailrec
  private def retryRequest(n: Int)(fn: => Response): Response = {
    util.Try{ fn } match {
      case util.Success(x) => x
      case util.Failure(e: RequestFailedException)
        if (List(500, 503).contains(e.response.statusCode) ||
          (e.response.statusCode == 429 && e.response.statusMessage == "Rate limit reached for requests"))
          && n > 1 => {
        logger.info(s"Request is failed. Retrying in ${round(exp(totalAttempts - n + 1) * 100)} milliseconds. Attempt ${totalAttempts - n + 2}/$totalAttempts")
        Thread.sleep(round(exp(totalAttempts - n + 1) * 100))
        retryRequest(n - 1)(fn)
      }
      case util.Failure(_: java.net.UnknownHostException | _: java.net.SocketTimeoutException) if n > 1 => retryRequest(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }

  private def tryRequest(data: ujson.Obj, endpoint: String, apiKey: String): Response = {
    requests.post(endpoint,
      auth=Bearer(apiKey),
      headers=Map("Content-Type" -> "application/json"),
      data=data.render())
  }

  private def tryMultiPartRequest(data: MultiPart, endpoint: String, apiKey: String): Response = {
    requests.post(endpoint,
      auth=Bearer(apiKey),
      data=data)
  }

}
