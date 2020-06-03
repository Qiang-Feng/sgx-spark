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

import java.io._
import java.net.Socket

import org.apache.spark._
import org.apache.spark.api.sgx.Types.SGXFunction

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

private[spark] object SGXRunner {
  def apply(func: SGXFunction): SGXRunner = {
    new SGXRunner(func, SGXFunctionType.NON_UDF, ArrayBuffer.empty)
  }

  def apply(func: SGXFunction, funcType: Int): SGXRunner = {
    new SGXRunner(func, funcType, ArrayBuffer.empty)
  }

  def apply(func: SGXFunction, funcType: Int, funcs: ArrayBuffer[SGXFunction]): SGXRunner = {
    new SGXRunner(func, funcType, funcs)
  }
}

/** Helper class to run a function in SGX Spark */
private[spark] class SGXRunner(func: SGXFunction, funcType: Int, funcs: ArrayBuffer[SGXFunction])
  extends SGXBaseRunner[Array[Byte], Array[Byte]](func, funcType, funcs) {

  override protected def sgxWriterThread(env: SparkEnv,
                                         worker: Socket,
                                         workerJars: mutable.Set[String],
                                         inputIterator: Iterator[Array[Byte]],
                                         numOfPartitions: Int,
                                         partitionIndex: Int,
                                         context: TaskContext,
                                         aggregator: Option[Aggregator[Any, Any, Any]],
                                         ordering: Option[Ordering[Any]]): WriterIterator = {
    new WriterIterator(env, worker, workerJars, inputIterator, numOfPartitions, partitionIndex, context, aggregator, ordering) {
      /** Writes a command section to the stream connected to the SGX worker */
      override protected def writeFunction(dataOut: DataOutputStream): Unit = {
        logInfo(s"Ser ${funcs.size + 1} closures")
        for (currFunc <- funcs) {
          logDebug(s"Ser closure: ${getClosureClass(currFunc)}")
          val command = closureSer.serialize(currFunc)
          dataOut.writeInt(command.array().size)
          dataOut.write(command.array())
        }
        val command = closureSer.serialize(func)
        logDebug(s"Ser closure: ${getClosureClass(func)}")
        dataOut.writeInt(command.array().size)
        dataOut.write(command.array())
        dataOut.writeInt(SpecialSGXChars.END_OF_FUNC_SECTION)
        dataOut.flush()
      }

      /** Writes input data to the stream connected to the SGX worker */
      override protected def writeIteratorToStream(dataOut: DataOutputStream): Unit = {
        SGXRDD.writeIteratorToStream(inputIterator, iteratorSer.get, dataOut)
        dataOut.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
        dataOut.flush()
      }

      private def getClosureClass(func: SGXFunction): String = {
        func match {
          case Left(a) => a.getClass.toString
          case Right(b) => b.getClass.toString
        }
      }

      /** Writes an aggregator to the stream connected to the SGX worker */
      override protected def writeAggregator(dataOut: DataOutputStream): Unit = {
        dataOut.writeBoolean(aggregator.isDefined)
        if (aggregator.isDefined) {
          logInfo(s"Writing aggregator")
          val agg = closureSer.serialize(aggregator)
          dataOut.writeInt(agg.array().length)
          dataOut.write(agg.array())
        }
        dataOut.flush()
      }

      /** Writes an ordering to the stream connected to the SGX worker */
      override protected def writeOrdering(dataOut: DataOutputStream): Unit = {
        dataOut.writeBoolean(ordering.isDefined)
        if (ordering.isDefined) {
          logInfo(s"Writing ordering")
          val ord = closureSer.serialize(ordering)
          dataOut.writeInt(ord.array().length)
          dataOut.write(ord.array())
        }
        dataOut.flush()
      }
    }
  }
}
