/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.api.sgx

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.nio.charset.StandardCharsets

import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.{SparkConf, SparkFunSuite}


class SGXRDDSuite extends SparkFunSuite {

  test("Writing large strings to the worker") {
    val iteratorSerializer = new JavaSerializer(new SparkConf()).newInstance()
    val input: List[String] = List("a"*100000)
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    SGXRDD.writeIteratorToStream(input.iterator, iteratorSerializer, buffer)
  }

  test("Handle nulls gracefully") {
    val buffer = new DataOutputStream(new ByteArrayOutputStream)
    val iteratorSerializer = new JavaSerializer(new SparkConf()).newInstance()
    // Should not have NPE when write an Iterator with null in it
    // The correctness will be tested in Python
    SGXRDD.writeIteratorToStream(Iterator("a", null), iteratorSerializer, buffer)
    SGXRDD.writeIteratorToStream(Iterator(null, "a"), iteratorSerializer, buffer)
    SGXRDD.writeIteratorToStream(Iterator("a".getBytes(StandardCharsets.UTF_8), null), iteratorSerializer, buffer)
    SGXRDD.writeIteratorToStream(Iterator(null, "a".getBytes(StandardCharsets.UTF_8)), iteratorSerializer, buffer)
    SGXRDD.writeIteratorToStream(Iterator((null, null), ("a", null), (null, "b")), iteratorSerializer, buffer)
    SGXRDD.writeIteratorToStream(Iterator(
      (null, null),
      ("a".getBytes(StandardCharsets.UTF_8), null),
      (null, "b".getBytes(StandardCharsets.UTF_8))), iteratorSerializer, buffer)
  }

//  test("python server error handling") {
//    val authHelper = new SocketAuthHelper(new SparkConf())
//    val errorServer = new ExceptionPythonServer(authHelper)
//    val client = new Socket(InetAddress.getLoopbackAddress(), errorServer.port)
//    authHelper.authToServer(client)
//    val ex = intercept[Exception] { errorServer.getResult(Duration(1, "second")) }
//    assert(ex.getCause().getMessage().contains("exception within handleConnection"))
//  }
//
//  class ExceptionPythonServer(authHelper: SocketAuthHelper)
//      extends PythonServer[Unit](authHelper, "error-server") {
//
//    override def handleConnection(sock: Socket): Unit = {
//      throw new Exception("exception within handleConnection")
//    }
//  }
}
