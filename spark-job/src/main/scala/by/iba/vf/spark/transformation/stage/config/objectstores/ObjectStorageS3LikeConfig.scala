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
package by.iba.vf.spark.transformation.stage.config.objectstores

protected trait ObjectStorageS3LikeConfig extends ObjectStorageConfig {
  final val cosStorage = "cos"
  final protected val objectStorage = "ObjectStorage"
  final val s3Storage = "s3"
  final protected val service = "service"

  final protected val fieldAccessKey = "accessKey"
  final protected val fieldBucket = "bucket"
  final protected val fieldEndpoint = "endpoint"
  final protected val fieldSecretKey = "secretKey"
  final protected val fieldIamApiKey = "iamApiKey"
  final protected val fieldIamServiceId = "iamServiceId"
  final protected val fieldAnonymousAccess = "anonymousAccess"
  final protected val fieldAuthType = "authType"
  final protected val fieldSsl = "ssl"
  final protected val authHMAC = "HMAC"
  final protected val authIAM = "IAM"

  def validate(config: Map[String, String]): Boolean = {
    (
      config.contains(fieldAuthType) &&
        ((authHMAC.equals(config(fieldAuthType)) && config.contains(fieldAccessKey) && config.contains(fieldSecretKey)) ||
          (authIAM.equals(config(fieldAuthType)) && config.contains(fieldIamApiKey) && config.contains(fieldIamServiceId)))
        && config.contains(fieldBucket) && config.contains(fieldPath) && config.contains(fieldFormat)) ||
      (config.contains(fieldBucket) && config.contains(fieldPath) && config.contains(fieldFormat))
  }

}
