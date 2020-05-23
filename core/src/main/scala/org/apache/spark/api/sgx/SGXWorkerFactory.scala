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

import java.io.{DataInputStream, DataOutputStream, File, InputStream}
import java.net.{InetAddress, ServerSocket, Socket, SocketException, URI}
import java.nio.charset.StandardCharsets
import java.util.Arrays

import scala.collection.JavaConverters._
import org.apache.spark.{SparkEnv, SparkException, SparkFiles}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{RedirectThread, Utils}

import scala.collection.mutable

private[spark] class SGXWorkerFactory(envVars: Map[String, String])
  extends Logging {

  val sgxWorkerModule = "org.apache.spark.deploy.worker.sgx.SGXWorker"
  val sgxWorkerExec = s"${System.getenv("SPARK_HOME")}/sbin/start-sgx-slave.sh"

  val useDaemon = SparkEnv.get.conf.getBoolean("spark.sgx.daemon.enabled", true)
  var daemon: Process = _
  val host = InetAddress.getByAddress(Array(0, 0, 0, 0))

  val simpleWorkers = new mutable.WeakHashMap[Socket, Process]()
  val daemonWorkers = new mutable.WeakHashMap[Socket, Int]()
  val idleWorkers = new mutable.Queue[Socket]()

  /**
   * TODO: start-sgx-slave.sh no longer works as we rely on the daemon
   *       to generate the SGXWorker disk image.
   */
  @deprecated private def createSimpleSGXWorker(): Socket = {
    var serverSocket: ServerSocket = null
    val workerDebug = SparkEnv.get.conf.isSGXDebugEnabled()
    val serverSockerPort = if (workerDebug) 65000 else 0
    try {
      serverSocket = new ServerSocket(serverSockerPort, 1, host)
      serverSocket.setSoTimeout(5 * 60 * 1000)
      // Create and start the worker
      val pb = new ProcessBuilder(Arrays.asList(sgxWorkerExec, sgxWorkerModule))

      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)

      logInfo(s"Unsecure worker port: ${serverSocket.getLocalPort.toString}")
      workerEnv.put("SGX_WORKER_FACTORY_PORT", serverSocket.getLocalPort.toString)
      workerEnv.put("SGX_WORKER_SERIALIZER", SparkEnv.get.conf.getOption("spark.serializer").
        getOrElse("org.apache.spark.serializer.JavaSerializer"))
      workerEnv.put("SGX_WORKER_DEBUG", workerDebug.toString)

      var worker: Process = null
      if (!workerDebug) {
        worker = pb.start()
        // Redirect worker stdout and stderr
        redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)
      }
      // else connect manually

      // Wait for worker to connect to our socket
      serverSocket.setSoTimeout(if (!workerDebug) 100000 else 1000000)

      try {
        val socket = serverSocket.accept()
        log.info(s"SGXWorker successfully connected at Port:${serverSocket.getLocalPort}")
        simpleWorkers.put(socket, worker)
        return socket
      } catch {
        case e: Exception =>
          throw new SparkException("SGXWorker worker failed to connect back.", e)
      }

    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }

  def create(): Socket = {
    if (useDaemon) {
      synchronized {
        if (idleWorkers.nonEmpty) {
          return idleWorkers.dequeue()
        }
      }
      createThroughDaemon()
    } else {
      createSimpleSGXWorker()
    }
  }

  private def createThroughDaemon(): Socket = {
    def createSocket(): Socket = {
      val serverSocketPort = if (SparkEnv.get.conf.isSGXDebugEnabled()) 65000 else 0
      var serverSocket: ServerSocket = null

      try {
        serverSocket = new ServerSocket(serverSocketPort, 1, host)
        serverSocket.setSoTimeout(5 * 60 * 1000)

        val outputStream = new DataOutputStream(daemon.getOutputStream)

        // Write the server socket port to the daemon
        logInfo(s"Unsecure worker port: ${serverSocket.getLocalPort.toString}")
        outputStream.writeInt(serverSocket.getLocalPort)
        outputStream.flush()
        daemon.getOutputStream.flush()

        // Write new jars to daemon
        val env = SparkEnv.get
        val sparkFilesDir = SparkFiles.getRootDirectory()
        val newJars = env.newJars.map { path =>
          val localName = new URI(path).getPath.split("/").last
          new File(sparkFilesDir, localName).toURI.toURL.getPath
        }
        logInfo(newJars.toString())
        newJars.foreach { path =>
          outputStream.writeInt(path.length)
          outputStream.write(path.getBytes(StandardCharsets.UTF_8))
        }
        outputStream.writeInt(SpecialSGXChars.END_OF_DATA_SECTION)
        outputStream.flush()
        daemon.getOutputStream.flush()

        try {
          val socket = serverSocket.accept()
          log.info(s"SGXWorker successfully connected at Port:${serverSocket.getLocalPort}")

          val inputStream = new DataInputStream(daemon.getInputStream)
          val workerPid = inputStream.readInt()
          daemonWorkers.put(socket, workerPid)

          socket
        } catch {
          case e: Exception =>
            throw new SparkException("SGXWorker worker failed to connect back.", e)
        }
      } finally {
        if (serverSocket != null) {
          serverSocket.close()
        }
      }
    }

    synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        createSocket()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to Python daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createSocket()
      }
    }
  }

  private def stopDaemon() {
    synchronized {
      if (useDaemon) {
        cleanupIdleWorkers()

        // Request shutdown of existing daemon by sending SIGTERM
        if (daemon != null) {
          daemon.destroy()
        }

        daemon = null
      } else {
        simpleWorkers.mapValues(_.destroy())
      }
    }
  }

  def stop() {
    stopDaemon()
  }

  def releaseWorker(worker: Socket) {
    if (useDaemon) {
      synchronized {
        // TODO: Monitor idle workers and kill after timeout
        // lastActivity = System.currentTimeMillis()
        idleWorkers.enqueue(worker)
      }
    } else {
      try {
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

  private def startDaemon() {
    synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        val command = Arrays.asList("/bin/bash", "-c", s"sudo -E python3 -u ${System.getenv("SPARK_HOME")}/sgx-worker-daemon.py")
        val pb = new ProcessBuilder(command)
        pb.directory(new File(System.getenv("SPARK_HOME")))
        val daemonEnv = pb.environment()
        daemonEnv.putAll(envVars.asJava)

        daemonEnv.put("SGX_WORKER_SERIALIZER", SparkEnv.get.conf.getOption("spark.serializer")
          .getOrElse("org.apache.spark.serializer.JavaSerializer"))
        daemonEnv.put("SGX_WORKER_DEBUG", SparkEnv.get.conf.isSGXDebugEnabled().toString)
        daemon = pb.start()

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(daemon.getErrorStream)
      } catch {
        case e: Exception =>
          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon)
            .flatMap { d => Utils.getStderr(d, 60 * 1000) }
            .getOrElse("")

          stopDaemon()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage = s"""
                                  |Error from SGXWorker:
                                  |  $formattedStderr
                                  |$e"""

            // Append error message from python daemon, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }
    }
  }

  private def cleanupIdleWorkers() {
    while (idleWorkers.nonEmpty) {
      val worker = idleWorkers.dequeue()
      try {
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

  /**
    * Redirect the given streams to our stderr in separate threads.
    */
  private def redirectStreamsToStderr(streams: InputStream*) {
    try {
      streams.foreach {
        new RedirectThread(_, System.err, "reader for " + sgxWorkerExec).start()
      }
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }
}
