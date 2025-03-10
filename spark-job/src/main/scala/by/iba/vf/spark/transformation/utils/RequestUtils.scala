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
package by.iba.vf.spark.transformation.utils

import by.iba.vf.spark.transformation.ResultLogger
import by.iba.vf.spark.transformation.utils.ai.llm.OpenAIChatGPT.{logger, totalAttempts}
import requests.{RequestFailedException, Response}

import scala.math.{exp, round}

object RequestUtils extends ResultLogger {

  private val totalAttempts = 7
  private val defaultStatusCodes = List(408, 425, 429, 500, 503, 504)

  @annotation.tailrec
  def retryRequest(n: Int, statusCodes: List[Int] = defaultStatusCodes)(fn: => Response): Response = {
    util.Try{ fn } match {
      case util.Success(x) => x
      case util.Failure(e: RequestFailedException)
        if statusCodes.contains(e.response.statusCode) && n > 1 =>
          logger.info(s"Request is failed.\nStatus code: ${e.response.statusCode}.\nStatus message: ${e.response.statusMessage}.\nURL: ${e.response.url}.\nRetrying in ${round(exp(totalAttempts - n + 1) * 100)} milliseconds. Attempt ${totalAttempts - n + 2}/$totalAttempts")
          Thread.sleep(round(exp(totalAttempts - n + 1) * 100))
          retryRequest(n - 1)(fn)
      case util.Failure(_: java.net.UnknownHostException | _: java.net.SocketTimeoutException) if n > 1 => retryRequest(n - 1)(fn)
      case util.Failure(e) => throw e
    }
  }

}
