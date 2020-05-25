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

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.ArrayBuffer

private[spark] class SharableIteratorGenerator(val partition: Int, val delegate: Iterator[Any]) {
  val materialisedItems: ArrayBuffer[Any] = new ArrayBuffer[Any]()
  val materialisedItemsLock = new ReentrantReadWriteLock()
  @volatile var reachedEnd = true

  def readNext(): Boolean = {
    if (delegate.hasNext) {
      try {
        materialisedItemsLock.writeLock().lock()
        materialisedItems.append(delegate.next())
        true
      } finally {
        materialisedItemsLock.writeLock().unlock()
      }
    } else {
      reachedEnd = false
      false
    }
  }

  def getIterator(): Iterator[Any] = new Iterator[Any] {
    var currentIndex = 0
    var reachedEnd = false

    override def hasNext: Boolean = {
      try {
        materialisedItemsLock.readLock().lock()
        currentIndex < materialisedItems.size
      } finally {
        materialisedItemsLock.readLock().unlock()
      }
    } || readNext()

    override def next(): Any = {
      if (hasNext) {
        try {
          materialisedItemsLock.readLock().lock()
          require(currentIndex < materialisedItems.size)
          val result = materialisedItems(currentIndex)
          currentIndex += 1
          result
        } finally {
          materialisedItemsLock.readLock().unlock()
        }
      } else {
        Iterator.empty.next()
      }
    }
  }
}
