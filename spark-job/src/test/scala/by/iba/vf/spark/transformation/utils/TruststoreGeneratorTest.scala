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

import java.io.File
import java.security.KeyStore
import java.util.Base64
import java.util.Collections

import com.karasiq.tls.TLS
import com.karasiq.tls.x509.CertExtension
import com.karasiq.tls.x509.CertificateGenerator
import com.karasiq.tls.x509.X509Utils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.PrivateMethodTester
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers._

class TruststoreGeneratorTest extends AnyFunSpec with MockitoSugar with PrivateMethodTester {

  it("createTruststoreWithGivenCert") {
    val keyGenerator = CertificateGenerator()
    val caSubject = X509Utils.subject(commonName = "TEST", organization = "testers")
    val cert: TLS.Certificate =
      keyGenerator.generate(caSubject, "RSA", 2048, extensions = CertExtension.certificationAuthorityExtensions()).certificate

    val pass = "some password"
    TruststoreGenerator.createTruststoreWithGivenCert(pass, cert.getEncoded, "X.509", "JKS", "test-truststore.jks")

    val keyStore = KeyStore.getInstance("JKS")
    val file = new File("test-truststore.jks")
    val inputStream = java.nio.file.Files.newInputStream(file.toPath)
    keyStore.load(inputStream, pass.toCharArray)
    val keyStoreAliases = keyStore.aliases

    Collections.list(keyStoreAliases) should contain("ca-cert")
    keyStore.getCertificate("ca-cert").getEncoded should be(cert.getEncoded)
    inputStream.close()
    file.delete()
  }

  it("with certificate") {
    val keyGenerator = CertificateGenerator()
    val caSubject = X509Utils.subject(commonName = "TEST", organization = "testers")
    val ca: TLS.CertificateKey =
      keyGenerator.generate(caSubject, "RSA", 2048, extensions = CertExtension.certificationAuthorityExtensions())
    val cert64 = new String(Base64.getEncoder.encode(ca.certificate.getEncoded))
    val pass = "some password"
    val spark = mock[SparkSession]
    val context = mock[SparkContext]
    val conf = mock[SparkConf]
    val rdd = mock[RDD[Int]]

    when(spark.sparkContext).thenReturn(context)
    when(context.getConf).thenReturn(conf)
    when(conf.get("spark.executor.instances", "2")).thenReturn("7")
    when(context.parallelize(1 to 700)).thenReturn(rdd)
    when(rdd.repartition(70)).thenReturn(rdd)
    doNothing.when(rdd).foreachPartition(any())

    TruststoreGenerator.createTruststoreOnExecutors(
      spark,
      Some(cert64, pass, "jdbc-source-truststore.jks")
    )
    val file = new File("jdbc-source-truststore.jks")
    file.delete()
  }

  it("without certificate") {
    val spark: SparkSession = mock[SparkSession]
    TruststoreGenerator.createTruststoreOnExecutors(
      spark,
      None
    )
  }

}
