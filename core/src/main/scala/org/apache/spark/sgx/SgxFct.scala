package org.apache.spark.sgx

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import java.util.Comparator

import org.apache.spark.Partitioner
import org.apache.spark.util.collection.PartitionedAppendOnlyMap
import org.apache.spark.util.collection.PartitionedAppendOnlyMapIdentifier
import org.apache.spark.util.collection.WritablePartitionedIterator

import org.apache.spark.storage.DiskBlockObjectWriter

import org.apache.spark.sgx.iterator.SgxWritablePartitionedFakeIterator

object SgxFct {

//def diskBlockObjectWriterWriteKeyValue(writer: DiskBlockObjectWriter, key: Any, value: Any) =
//	new DiskBlockObjectWriterWriteKeyValue(writer, key, value).send()

	def externalSorterInsertAllCreateKey[K](partitioner: Partitioner, pair: Product2[K,Any]) =
		new ExternalSorterInsertAllCreateKey[K](partitioner, pair).send()

	def partitionedAppendOnlyMapCreate[K,V]() =
		new PartitionedAppendOnlyMapCreate().send()

	def partitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](
			id: PartitionedAppendOnlyMapIdentifier,
			keyComparator: Option[Comparator[K]]) =
		new PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](id, keyComparator).send()

	def writablePartitionedIteratorGetNext[K,V,T](it: SgxWritablePartitionedFakeIterator[K,V]) =
		new WritablePartitionedIteratorGetNext[K,V,T](it).send()

	def writablePartitionedIteratorHasNext[K,V](it: SgxWritablePartitionedFakeIterator[K,V]) =
		new WritablePartitionedIteratorHasNext(it).send()

	def writablePartitionedIteratorWriteNext[K,V](it: SgxWritablePartitionedFakeIterator[K,V], writer: DiskBlockObjectWriter) =
		new WritablePartitionedIteratorWriteNext(it, writer).send()

	def writablePartitionedIteratorNextPartition[K,V](it: SgxWritablePartitionedFakeIterator[K,V]) =
		new WritablePartitionedIteratorNextPartition(it).send()

	def fct0[Z](fct: () => Z) = new SgxFct0[Z](fct).send()

	def fct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) = new SgxFct2[A, B, Z](fct, a, b).send()
}

private case class ExternalSorterInsertAllCreateKey[K](
	partitioner: Partitioner,
	pair: Product2[K,Any]) extends SgxMessage[(Int,K)] {

	def execute() = Await.result( Future {
		(partitioner.getPartition(pair.asInstanceOf[Encrypted].decrypt[Product2[_,_]]._1), pair._1)
	}, Duration.Inf)

	override def toString = this.getClass.getSimpleName + "(partitioner=" + partitioner + ", pair=" + pair + ")"
}

private case class PartitionedAppendOnlyMapCreate[K,V]() extends SgxMessage[PartitionedAppendOnlyMapIdentifier] {

	def execute() = Await.result( Future {
		new PartitionedAppendOnlyMap().id
	}, Duration.Inf)
}

private case class PartitionedAppendOnlyMapDestructiveSortedWritablePartitionedIterator[K,V](
	id: PartitionedAppendOnlyMapIdentifier,
	keyComparator: Option[Comparator[K]]) extends SgxMessage[WritablePartitionedIterator] {

	def execute() = SgxWritablePartitionedFakeIterator[K,V](
		Await.result( Future {
			id.getMap.destructiveSortedWritablePartitionedIterator(keyComparator)
		}, Duration.Inf)
	)
}

private case class WritablePartitionedIteratorHasNext[K,V](
	it: SgxWritablePartitionedFakeIterator[K,V]) extends SgxMessage[Boolean] {

	def execute() = Await.result( Future {
		it.getIterator.hasNext()
	}, Duration.Inf)
}

private case class WritablePartitionedIteratorNextPartition[K,V](
	it: SgxWritablePartitionedFakeIterator[K,V]) extends SgxMessage[Int] {

	def execute() = Await.result( Future {
		it.getIterator.nextPartition()
	}, Duration.Inf)
}

private case class WritablePartitionedIteratorWriteNext[K,V](
	it: SgxWritablePartitionedFakeIterator[K,V],
	writer: DiskBlockObjectWriter) extends SgxMessage[Unit] {

	def execute() = Await.result( Future {
		it.getIterator.writeNext(writer)
	}, Duration.Inf)
}

private case class WritablePartitionedIteratorGetNext[K,V,T](
	it: SgxWritablePartitionedFakeIterator[K,V]) extends SgxMessage[T] {

	def execute() = Await.result( Future {
		it.getIterator.getNext[T]()
	}, Duration.Inf)
}

//private case class DiskBlockObjectWriterWriteKeyValue(
//	writer: DiskBlockObjectWriter,
//	key: Any,
//	value: Any) extends SgxMessage[Unit] {
//
//	def execute() = Await.result( Future {
//		writer.get.write(key, value)
//	}, Duration.Inf)
//}


private case class SgxFct0[Z](fct: () => Z) extends SgxMessage[Z] {
	def execute() = Await.result(Future { fct() }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "))"
}

private case class SgxFct2[A, B, Z](fct: (A, B) => Z, a: A, b: B) extends SgxMessage[Z] {
	def execute() = Await.result(Future { fct(a, b) }, Duration.Inf)
	override def toString = this.getClass.getSimpleName + "(fct=" + fct + " (" + fct.getClass.getSimpleName + "), a=" + a + ", b=" + b + ")"
}

