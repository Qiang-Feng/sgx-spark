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

package org.apache.spark.util

import scala.collection.mutable


object SGXUtils {
  /** Closures used for SGX tests should be written here (not in Tests) as SGXRunner is using
    *  classLoader to find the appropriate anonymous function */
  val filterEvenNumFunc = (t: Int) => t % 2 == 0
  val sumFunc = (a: Int, b: Int) => a + b
  val multiplyFunc = (a: Int, b: Int) => a * b

  val mapIncrementOneFunc = (v: Int) => v + 1
  val mapMultiplyByTwoFunc = (v: Int) => v * 2

  val mapPartitionsSum = (iter: Iterator[Int]) => Iterator(iter.sum)

  val mapPartitionsWithIndex = (split: Int, iter: Iterator[Int]) => Iterator((split, iter.sum))
  val mapToList = (iter: Array[Int]) => iter.toList

  val flatMapOneToVal: (Int) => TraversableOnce[Int] = (x: Int) => 1 to x


  val groupBySum = (s: (String, scala.Iterable[Int])) => (s._1, (s._2.sum))
  val sumGroup = (elem: (String, Iterable[Int])) => (elem._1, elem._2.sum)

  val mapKeyOddEven = (v: (Int, Int)) => (v._1 % 2, v._2)
  val mapKeys = (t: (String, Int)) => t match {
    case ("USA", v) => ("test1", v)
    case ("India", v) => ("test1", v)
    case ("UK", v) => ("test1", v)
    case ("Russia", v) => ("test2", v)
  }
  val mapKeysToInt = (t: (String, Int)) => t match {
    case ("USA", v) => (1, v)
    case ("India", v) => (2, v)
    case ("UK", v) => (3, v)
    case ("Russia", v) => (4, v)
  }


  /**
    * Dummy closure to maintain API consisent (used for shuffles - even though not used)
    */
  val toIteratorSizeSGXFunc = (itr: Iterator[Any]) => itr
}
