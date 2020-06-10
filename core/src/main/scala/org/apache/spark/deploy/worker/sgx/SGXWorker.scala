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

package org.apache.spark.deploy.worker.sgx

import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException, File, IOException}
import java.net.{InetAddress, Socket, URL}
import java.nio.ByteBuffer
import java.nio.file.Files

import org.apache.spark.api.sgx.Types.SGXFunction
import org.apache.spark.api.sgx.{SGXException, SGXFunctionType, SGXRDD, SpecialSGXChars}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.util.{MutableURLClassLoader, Utils}
import org.apache.spark.{Aggregator, SGXPartitioner, SparkConf, SparkException, TaskContext}

import scala.collection.mutable
import scala.reflect.ClassTag

private[spark] class SGXWorker(dataSer: SerializerInstance) extends Logging {
  val SYSTEM_NAME = "SparkSGXWorker"
  val ENDPOINT_NAME = "SecureWorker"
  var closureSerializer: SerializerInstance = new JavaSerializer(SGXWorker.conf).newInstance()

  private val jarsLoaded = mutable.HashSet[URL]()

  if (dataSer == null) {
    throw new SGXException("Worker data serializer not set", new RuntimeException)
  }

  val shuffleMemoryBytesSpilled: Int = 0
  val shuffleDiskBytesSpilled: Int = 0

  def process(inSock: DataInputStream, outSock: DataOutputStream): Unit = {
    val bootTime = System.nanoTime()

    val splitIndex = inSock.readInt()
    if (splitIndex == -1) {
      System.exit(-1)
    }
    val sgxVersion = SGXRDD.readUTF(inSock)
    logInfo(s"SGXWorker version ${sgxVersion}")
    val boundPort = inSock.readInt()
    val taskContext = TaskContext.get()
    val stageId = inSock.readInt()
    val numOfPartitions = inSock.readInt()
    val partitionId = inSock.readInt()
    val attemptId = inSock.readInt()
    val taskAttemptId = inSock.readLong()

    val localProps = new mutable.HashMap[String, String]()
    for (_ <- 0 until inSock.readInt()) {
      val k = SGXRDD.readUTF(inSock)
      val v = SGXRDD.readUTF(inSock)
      localProps(k) = v
    }
    val sparkFilesDir = SGXRDD.readUTF(inSock)

    // Read the JAR hash
    var endOfJars = false
    var newJars = 0
    while (!endOfJars) {
      inSock.readInt() match {
        case jarLength if jarLength > 0 =>
          val hash = SGXRDD.readUTF(inSock)
          val jarFilename = s"/${hash}.jar"

          // Write the new file
          val jarFile = new File(jarFilename)
          jarFile.createNewFile
          val jarBytes = new Array[Byte](jarLength)
          inSock.readFully(jarBytes)
          Files.write(jarFile.toPath, jarBytes)

          jarsLoaded.add(jarFile.toURI.toURL)
          newJars += 1

        case SpecialSGXChars.END_OF_JARS =>
          endOfJars = true
      }
    }

    logInfo(s"Received ${newJars} new jar files")
    if (newJars > 0) {
      closureSerializer = new JavaSerializer(SGXWorker.conf)
        .setDefaultClassLoader(new MutableURLClassLoader(jarsLoaded.toArray, getClass.getClassLoader))
        .newInstance()
    }

    // Read Function Type & Function
    val initTime = System.nanoTime()
    val evalType = inSock.readInt()

    val funcArray = readFunction(inSock)
    val aggregator = readAggregator(inSock)
    val ordering = readOrdering(inSock)

    logInfo(s"Executing ${funcArray.size} (pipelined) funcs")

    evalType match {

      case SGXFunctionType.SHUFFLE_MAP_BYPASS =>
        logInfo(s"ShuffleMap Bypass with ${numOfPartitions} partition(s)")
        val iterator = new ReaderIterator(inSock, dataSer).asInstanceOf[Iterator[(Any, Any)]]
        val sgxPartitioner = new SGXPartitioner(numOfPartitions)
        // Mapping of encrypted keys to partitions (needed by the shuffle Writer)
        val keyMap = iterator.map { case (k, _) =>
          (k, sgxPartitioner.getPartition(k))
        }
        SGXRDD.writeIteratorToStream(keyMap, dataSer, outSock)
        outSock.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
        outSock.flush()

      case SGXFunctionType.SHUFFLE_REDUCE =>
        logInfo(s"Shuffle Reduce with ${numOfPartitions} partition(s)")
        val iterator = new ReaderIterator(inSock, dataSer).asInstanceOf[Iterator[(Any, Any)]]
        val sgxPartitioner = new SGXPartitioner(numOfPartitions)

        // Perform the sorting, aggregation and pair with reduce-side partition
        val sorter = new MinimalExternalSorter(aggregator, Some(sgxPartitioner), ordering)
        sorter.insertAll(iterator)
        val recordMap = sorter.iterator.map { case (k, v) =>
          ((k, v), sgxPartitioner.getPartition(k))
        }

        // Write aggregated and sorted records
        SGXRDD.writeIteratorToStream(recordMap, dataSer, outSock)
        outSock.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
        outSock.flush()

      case SGXFunctionType.NON_UDF =>
        logInfo("Non-UDF")
        // Read Iterator
        val iterator = new ReaderIterator(inSock, dataSer)
        val res = funcArray.head match {
          case Left(a) => a(iterator)
          case Right(b) => b(partitionId, iterator)
        }
        SGXRDD.writeIteratorToStream[Any](res.asInstanceOf[Iterator[Any]], dataSer, outSock)
        outSock.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
        outSock.flush()

      case SGXFunctionType.PIPELINED =>
        logInfo("Pipelined")
        val iterator = new ReaderIterator(inSock, dataSer)

        // Fix the case where the first closure completely ignores the iterator,
        // and therefore we never have a call to ReaderIterator.read() to
        // check for the SpecialSGXChars.END_OF_DATA_SECTION.
        iterator.hasNext

        var res: Iterator[Any] = null
        for (func <- funcArray) {
          val input = if (res == null) iterator else res
          func match {
            case Left(a) =>
              logDebug(s"Running func ${a.getClass}")
              res = a(input).asInstanceOf[Iterator[Any]]
            case Right(b) =>
              logDebug(s"Running func ${b.getClass}")
              res = b(partitionId, input).asInstanceOf[Iterator[Any]]
          }
        }
        logInfo("Writing results to stream")
        SGXRDD.writeIteratorToStream[Any](res, dataSer, outSock)
        outSock.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
        outSock.flush()

      case _ =>
        logError(s"Unsupported FunctionType ${evalType}")
    }

    val finishTime = System.nanoTime()

    val expectedEOS = inSock.readInt()
    if (expectedEOS != SpecialSGXChars.END_OF_STREAM) {
      logError(s"Expected END_OF_STREAM, got ${expectedEOS}")
    }

    // Write reportTimes AND Shuffle timestamps
    outSock.writeInt(SpecialSGXChars.END_OF_STREAM)
    outSock.flush()
    // send metrics etc
  }

  def reportMetrics(outFile: DataOutputStream, shuffleMemoryBytesSpilled: Int, shuffleDiskBytesSpilled: Int ): Unit = {
    outFile.writeInt(shuffleMemoryBytesSpilled)
    outFile.writeInt(shuffleDiskBytesSpilled)
  }

  def reportTimes(outfile: DataOutputStream, bootTime: Long, initTime: Long, finishTime: Long): Unit = {
    outfile.writeInt(SpecialSGXChars.TIMING_DATA)
    outfile.writeLong(bootTime)
    outfile.writeLong(initTime)
    outfile.writeLong(finishTime)
  }

  def readAggregator(inSock: DataInputStream): Option[Aggregator[Any, Any, Any]] = {
    val hasAggregator = inSock.readBoolean()
    if (hasAggregator) {
      inSock.readInt() match {
        case aggregatorSize if aggregatorSize > 0 =>
          val obj = new Array[Byte](aggregatorSize)
          inSock.readFully(obj)
          closureSerializer.deserialize[Option[Aggregator[Any, Any, Any]]](ByteBuffer.wrap(obj))
      }
    }
    None
  }

  def readOrdering(inSock: DataInputStream): Option[Ordering[Any]] = {
    val hasOrdering = inSock.readBoolean()
    if (hasOrdering) {
      inSock.readInt() match {
        case orderingSize if orderingSize > 0 =>
          val obj = new Array[Byte](orderingSize)
          inSock.readFully(obj)
          return closureSerializer.deserialize[Option[Ordering[Any]]](ByteBuffer.wrap(obj))
      }
    }
    None
  }

  def readFunction(inSock: DataInputStream): mutable.ArrayBuffer[SGXFunction] = {
    val functionArr = mutable.ArrayBuffer[SGXFunction]()
    var done = false
    while (!done) {
      inSock.readInt() match {
        case func_size if func_size > 0 =>
          val obj = new Array[Byte](func_size)
          inSock.readFully(obj)
          val closure = closureSerializer.deserialize[SGXFunction](ByteBuffer.wrap(obj))
          logInfo(s"Successfully read ${closure}")
          functionArr.append(closure)
        case SpecialSGXChars.END_OF_FUNC_SECTION =>
          logInfo(s"Read ${functionArr.size} functions Done")
          done = true
      }
    }
    functionArr
  }
}

// Data is encrypted thus Array[Byte] - we should decode them to a type[IN]
private[spark] class ReaderIterator[IN: ClassTag](stream: DataInputStream, dataSer: SerializerInstance) extends Iterator[IN] with Logging {

  private var nextObj: IN = _
  private var eos = false

  override def hasNext: Boolean = nextObj != null || {
    if (!eos) {
      nextObj = read()
      hasNext
    } else {
      false
    }
  }

  override def next(): IN = {
    if (hasNext) {
      val obj = nextObj
      nextObj = null.asInstanceOf[IN]
      logDebug(s"Next is: ${obj}")
      obj
    } else {
      Iterator.empty.next()
    }
  }

  override def size(): Int = {
    throw new SparkException("Not implemented!")
  }

  // FramedSerializer
  /**
    * Reads next object from the stream.
    * When the stream reaches end of data, needs to process the following sections,
    * and then returns null.
    */
  protected def read(): IN = {
    try {
      stream.readInt() match {
        case length if length > 0 =>
          val obj = new Array[Byte](length)
          stream.readFully(obj)
          val elem = dataSer.deserialize[IN](ByteBuffer.wrap(obj))
          return elem.asInstanceOf[IN]
        case SpecialSGXChars.END_OF_DATA_SECTION =>
          eos = true
        case SpecialSGXChars.NULL =>
      }
    } catch {
      case ex: Exception =>
        logError(s"SGXWorker Failed to get Data ${ex}")
    }
    null.asInstanceOf[IN]
  }
}


private[deploy] object SGXWorker extends Logging {
  val conf = new SparkConf(loadDefaults = false)
  var dataSerializer: SerializerInstance = _

  def localConnect(address: String, port: Int): Socket = {
    try {
      val ia = InetAddress.getByName(address)
      val socket = new Socket(ia, port)
      socket
    } catch {
      case e: IOException =>
        logError(s"Could not open socket on port:${port}")
        null
    }
  }

  def instantiateClass[T](className: String): T = {
    val cls = Utils.classForName(className)
    // Look for a constructor taking a SparkConf and a boolean isDriver, then one taking just
    // SparkConf, then one taking no arguments
    try {
      cls.getConstructor(classOf[SparkConf], java.lang.Boolean.TYPE)
        .newInstance(conf, new java.lang.Boolean(false))
        .asInstanceOf[T]
    } catch {
      case _: NoSuchMethodException =>
        try {
          cls.getConstructor(classOf[SparkConf]).newInstance(conf).asInstanceOf[T]
        } catch {
          case _: NoSuchMethodException =>
            cls.getConstructor().newInstance().asInstanceOf[T]
        }
    }
  }

  def main(args: Array[String]): Unit = {
    Utils.initDaemon(log, inEnclave = true)
    val workerDebugEnabled = sys.env("SGX_WORKER_DEBUG").toBoolean
    val workerSerializer = sys.env("SGX_WORKER_SERIALIZER")
    dataSerializer = SGXWorker.instantiateClass[Serializer](workerSerializer).newInstance()

    val worker = new SGXWorker(dataSerializer)

    val address = sys.env("SGXLKL_GW4")
    val port = if (workerDebugEnabled) 65000 else sys.env("SGX_WORKER_FACTORY_PORT").toInt
    val socket = localConnect(address, port)
    socket.setSoTimeout(0)
    socket.setSendBufferSize(65536)
    socket.setReceiveBufferSize(65536)

    val outStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536))
    val inStream = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 65536))

    var runWorker = true
    while (runWorker) {
      try {
        val signal = inStream.readInt()
        if (signal == SpecialSGXChars.BEGIN_PROCESSING) {
          worker.process(inStream, outStream)

          // Wait for the FINISH_PROCESSING signal
          inStream.readInt() match {
            case SpecialSGXChars.FINISH_PROCESSING =>
              logInfo("Received da FINISHED_PROCESSING status")
            case status =>
              runWorker = false
              logError(s"Unexpected status received: ${status}")
          }
        } else {
          runWorker = false
          logError(s"Expected BEGIN_PROCESSING but got ${signal}")
        }
      } catch {
        case _: EOFException =>
          runWorker = false
          logInfo("EOFException, SGXWorker shutting down")
      }
      System.gc()
    }
    socket.close()
    logInfo("Shutting down SGXWorker")
  }
}
