package org.apache.spark.sgx.iterator

import scala.collection.mutable.ArrayBuffer

import scala.util.control.Breaks._

import org.apache.commons.lang3.SerializationUtils
import org.apache.commons.lang3.SerializationException

import org.apache.spark.InterruptibleIterator
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator
import org.apache.spark.sgx.Encrypt
import org.apache.spark.sgx.Encrypted
import org.apache.spark.sgx.SgxCommunicator
import org.apache.spark.sgx.SgxSettings
import org.apache.spark.sgx.shm.ShmCommunicationManager
import org.apache.spark.rdd.HadoopPartition
import org.apache.spark.executor.InputMetrics
import org.apache.spark.Partition
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapreduce.lib.input.UncompressedSplitLineReader
import org.apache.spark.sgx.shm.MappedDataBufferManager
import org.apache.spark.sgx.shm.MappedDataBuffer
import org.apache.spark.sgx.Serialization
import org.apache.spark.sgx.SgxCallable
import org.apache.spark.util.collection.WritablePartitionedIterator
import org.apache.spark.storage.DiskBlockObjectWriter
import org.apache.spark.sgx.shm.MallocedMappedDataBuffer
import org.apache.spark.sgx.shm.RingBuffProducer

abstract class SgxIteratorProv[T] extends InterruptibleIterator[T](null, null) with SgxIterator[T] with Logging {
  def getIdentifier: SgxIteratorProvIdentifier[T]
  
  def writeNext(writer: DiskBlockObjectWriter): Unit = {
    logError("Method writeNext() must be implemented by subclass")
    throw new RuntimeException("Method writeNext() must be implemented by subclass")
  }  
}

class SgxIteratorProvider[T](delegate: Iterator[T], doEncrypt: Boolean) extends SgxIteratorProv[T] with SgxCallable[Unit] {
  
  logDebug("xxx creating " + this)

	private val com = ShmCommunicationManager.get().newShmCommunicator(false)

	private val identifier = new SgxIteratorProviderIdentifier[T](com.getMyPort)

	private def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])

	override final def hasNext: Boolean = delegate.hasNext

	/**
	 * Always throws an UnsupportedOperationException. Access this Iterator via the message interface.
	 * Note: We allow calls to hasNext(), since they are, e.g., used by the superclass' toString() method.
	 */
	override final def next(): T = throw new UnsupportedOperationException("Access this special Iterator via messages.")

	override def getIdentifier = identifier

	def call(): Unit = {
		val com = do_accept
		logDebug(this + " got connection: " + com)

		while (isRunning) {
			com.recvOne() match {
				case num: MsgIteratorReqNextN => {
					val q = new ArrayBuffer[T](num.num)
					if (delegate.isInstanceOf[NextIterator[T]] && delegate.hasNext) {
						for (i <- 0 to num.num - 1 if delegate.hasNext) {
							val n = delegate.next
						  logDebug("Providing: " + n)
							// Need to clone the object. Otherwise the same object will be sent multiple times.
							// It seems that this is due to optimizations in the implementation of class NextIterator.
							// If cloning is not possible, just add this one object and send it individually.
							// This should never be the case, as the object _must_ be sent to a consumer.
						  q.insert(i, SerializationUtils.clone(n.asInstanceOf[Serializable]).asInstanceOf[T])
						}
					} else {
						for (i <- 0 to num.num - 1 if delegate.hasNext) {
						  val n = delegate.next
						  logDebug("Providing: " + n)
							q.insert(i, n)
						}
					}
					val qe = if (doEncrypt) Encrypt(q) else q
					com.sendOne(qe)
				}
				case MsgIteratorReqClose =>
					stop
					com.close()

				case x: Any => logDebug(this + ": Unknown input message provided.")
			}
		}
	}

	override def toString() = this.getClass.getSimpleName + "(identifier=" + identifier + ", com=" + com + ")"
}

class SgxShmIteratorProvider[K,V](delegate: NextIterator[(K,V)], recordReader: RecordReader[K,V], theSplit: Partition, inputMetrics: InputMetrics, splitLength: Long, splitStart: Long, delimiter: Array[Byte]) extends SgxIteratorProv[(K,V)] with SgxCallable[Unit] {
  
  private val com = ShmCommunicationManager.get().newShmCommunicator(false)

	private val identifier = new SgxShmIteratorProviderIdentifier[K,V](com.getMyPort, recordReader.getLineReader.getBufferOffset(), recordReader.getLineReader.getBufferSize(), theSplit, inputMetrics, splitLength, splitStart, delimiter)

	private def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])
  
  logDebug("Creating " + this)

	override def getIdentifier = identifier
	
	def call(): Unit = {
	  val com = do_accept
	  
	  logDebug(this + " got connection: " + com)

	  while (isRunning) {
	    val ret = com.recvOne() match {
			  case c: SgxShmIteratorConsumerClose =>
			    stop
			    delegate.closeIfNeeded()
			  case f: SgxShmIteratorConsumerFillBufferMsg =>
			    recordReader.getLineReader.fillBuffer(f.inDelimiter)
	    }
	    if (ret != Unit) {
	      com.sendOne(ret)
	    }
	  }
  }
	
	override def toString() = this.getClass.getSimpleName + "(identifier=" + identifier + ")"
}

class SgxObj extends Serializable {}

class SgxPair[K,V](key: K, value: V) extends SgxObj {}

class SgxPartition extends SgxObj {}


class SgxWritablePartitionedIteratorProvider[K,V](@transient it: Iterator[Product2[Product2[Int,K],V]], offset: Long, size: Int) extends WritablePartitionedIterator with SgxCallable[Unit] with Logging {
  
  private val buffer = new MallocedMappedDataBuffer(MappedDataBufferManager.get().startAddress() + offset, size)
  val writer = new RingBuffProducer(buffer, Serialization.serializer)
  
  private val com = ShmCommunicationManager.get().newShmCommunicator(false)

	private val identifier = new SgxWritablePartitionedIteratorProviderIdentifier[K,V](com.getMyPort, offset, size)

	private def do_accept() = com.connect(com.recvOne.asInstanceOf[Long])
  
  logDebug("Creating " + this)

	def getIdentifier = identifier
	
  private[this] var cur = if (it.hasNext) it.next() else null

  def writeNext(writer: DiskBlockObjectWriter): Unit = {
    writer.write(cur._1._2, cur._2)
    cur = if (it.hasNext) it.next() else null
  }

  def hasNext(): Boolean = cur != null

  def nextPartition(): Int = cur._1._1
  
  def fill() = {
    logDebug("filling " + this)

//          while (it.hasNext) {
//        val partitionId = it.nextPartition()
//        while (it.hasNext && it.nextPartition() == partitionId) {
//          it.writeNext(writer)
//        }
//        val segment = writer.commitAndGet()
//        lengths(partitionId) = segment.length
//      }
    
    while (it.hasNext) {
      val partitionId = nextPartition()
      while (hasNext && nextPartition() == partitionId) {
        logDebug("write("+cur._1._2+","+cur._2+")")
        writer.write(new SgxPair(cur._1._2, cur._2))
        logDebug("wrote("+cur._1._2+","+cur._2+")")
        cur = if (it.hasNext) it.next() else null
        logDebug("cur = " + cur)  
      }
      val x = new SgxPartition
      logDebug("write("+ x +")")
      writer.write(x)
        logDebug("wrote("+x+")")
    }
  }
	
	def call(): Unit = {
	  val com = do_accept
	  
	  logDebug(this + " got connection: " + com)
	  
	  fill()	  

	  while (isRunning) {
	    val r = com.recvOne()
	    logDebug("received: " + r)
	    val ret: Any = null
	    
//	    match {
//			  case c: SgxShmIteratorConsumerClose =>
//			    stop
//			    delegate.closeIfNeeded()
//			  case f: SgxShmIteratorConsumerFillBufferMsg =>
//			    recordReader.getLineReader.fillBuffer(f.inDelimiter)
//	    }
	    if (ret != null) {
	      com.sendOne(ret)
	    }
	  }
  }
	
	override def toString() = this.getClass.getSimpleName + "(identifier=" + identifier + ", buffer=" + buffer + ")"
}


