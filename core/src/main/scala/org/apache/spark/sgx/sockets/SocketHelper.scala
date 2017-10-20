package org.apache.spark.sgx.sockets

import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.Socket
import scala.collection.mutable.ListBuffer

import org.apache.spark.sgx.SgxCommunicationInterface
import org.apache.spark.sgx.SgxSettings

import org.apache.spark.internal.Logging

private object MsgDone {}

class SocketHelper(socket: Socket) extends SgxCommunicationInterface with Logging {
	private	val oos = new ObjectOutputStream(socket.getOutputStream())
	private	val ois = new ObjectInputStream(socket.getInputStream())

	def sendOne(obj: Any) = {
		write(obj)
	}

	def recvOne(): AnyRef = {
		read()
	}

	def sendRecv[O](in: Any): O = {
		sendOne(in)
		recvOne.asInstanceOf[O]
	}

	def close() = {
		oos.close()
		ois.close()
		socket.close()
	}

	def read(): AnyRef = {
		ois.readObject()
	}

	def write(o: Any): Unit = {
		oos.reset()
		oos.writeObject(o)
		oos.flush()
	}

	override def toString() = getClass.getSimpleName + "(local=" + socket.getLocalAddress + ":" + socket.getLocalPort + ", remote=" + socket.getRemoteSocketAddress + ":" + socket.getPort + ")"
}

/**
 * Inspired by
 * https://stackoverflow.com/questions/7930814/whats-the-scala-way-to-implement-a-retry-able-call-like-this-one
 */
object Retry {
	def apply[T](n: Int)(fn: => T): T = {
		val r = try { Some(fn) } catch { case e: Exception if n > 1 => None }
		r match {
			case Some(x) => x
			case None => apply(n - 1)(fn)
		}
	}
}

///**
// * All in one go:
// * (1) Open connection
// * (2) Send object
// * (3) Receive answer
// * (4) Close connection
// */
//object SocketOpenSendRecvClose {
//	def apply[O](host: String, port: Int, in: Any): O = {
//		val sh = new SocketHelper(Retry(10)(new Socket(host, port)))
//		val res = sh.sendRecv[O](in)
//		sh.close()
//		res
//	}
//}
