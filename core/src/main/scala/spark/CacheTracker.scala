package spark

import akka.actor._
import akka.actor.Actor
import akka.actor.Actor._
import akka.util.duration._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark.storage.BlockManager
import spark.storage.StorageLevel

sealed trait CacheTrackerMessage
case class AddedToCache(rddId: Int, partition: Int, host: String, size: Long = 0L)
  extends CacheTrackerMessage
case class DroppedFromCache(rddId: Int, partition: Int, host: String, size: Long = 0L)
  extends CacheTrackerMessage
case class MemoryCacheLost(host: String) extends CacheTrackerMessage
case class RegisterRDD(rddId: Int, numPartitions: Int) extends CacheTrackerMessage
case class SlaveCacheStarted(host: String, size: Long) extends CacheTrackerMessage
case object GetCacheStatus extends CacheTrackerMessage
case object GetCacheLocations extends CacheTrackerMessage
case object StopCacheTracker extends CacheTrackerMessage

class CacheTrackerActor extends Actor with Logging {
  // TODO: Should probably store (String, CacheType) tuples
  private val locs = new HashMap[Int, Array[List[String]]]

  /**
   * A map from the slave's host name to its cache size.
   */
  private val slaveCapacity = new HashMap[String, Long]
  private val slaveUsage = new HashMap[String, Long]

  private def getCacheUsage(host: String): Long = slaveUsage.getOrElse(host, 0L)
  private def getCacheCapacity(host: String): Long = slaveCapacity.getOrElse(host, 0L)
  private def getCacheAvailable(host: String): Long = getCacheCapacity(host) - getCacheUsage(host)
  
  def receive = {
    case SlaveCacheStarted(host: String, size: Long) =>
      logInfo("Started slave cache (size %s) on %s".format(
        Utils.memoryBytesToString(size), host))
      slaveCapacity.put(host, size)
      slaveUsage.put(host, 0)
      self.reply(true)

    case RegisterRDD(rddId: Int, numPartitions: Int) =>
      logInfo("Registering RDD " + rddId + " with " + numPartitions + " partitions")
      locs(rddId) = Array.fill[List[String]](numPartitions)(Nil)
      self.reply(true)

    case AddedToCache(rddId, partition, host, size) =>
      slaveUsage.put(host, getCacheUsage(host) + size)
      logInfo("Cache entry added: (%s, %s) on %s (size added: %s, available: %s)".format(
        rddId, partition, host, Utils.memoryBytesToString(size),
        Utils.memoryBytesToString(getCacheAvailable(host))))
      locs(rddId)(partition) = host :: locs(rddId)(partition)
      self.reply(true)

    case DroppedFromCache(rddId, partition, host, size) =>
      logInfo("Cache entry removed: (%s, %s) on %s (size dropped: %s, available: %s)".format(
        rddId, partition, host, Utils.memoryBytesToString(size),
        Utils.memoryBytesToString(getCacheAvailable(host))))
      slaveUsage.put(host, getCacheUsage(host) - size)
      // Do a sanity check to make sure usage is greater than 0.
      val usage = getCacheUsage(host)
      if (usage < 0) {
        logError("Cache usage on %s is negative (%d)".format(host, usage))
      }
      locs(rddId)(partition) = locs(rddId)(partition).filterNot(_ == host)
      self.reply(true)

    case MemoryCacheLost(host) =>
      logInfo("Memory cache lost on " + host)
      for ((id, locations) <- locs) {
        for (i <- 0 until locations.length) {
          locations(i) = locations(i).filterNot(_ == host)
        }
      }
      self.reply(true)

    case GetCacheLocations =>
      logInfo("Asked for current cache locations")
      self.reply(locs.map{case (rrdId, array) => (rrdId -> array.clone())})

    case GetCacheStatus =>
      val status = slaveCapacity.map { case (host, capacity) =>
        (host, capacity, getCacheUsage(host))
      }.toSeq
      self.reply(status)

    case StopCacheTracker =>
      logInfo("CacheTrackerActor Server stopped!")
      self.reply(true)
      self.exit()
  }
}

class CacheTracker(isMaster: Boolean, blockManager: BlockManager) extends Logging {
  // Tracker actor on the master, or remote reference to it on workers
  val ip: String = System.getProperty("spark.master.host", "localhost")
  val port: Int = System.getProperty("spark.master.port", "7077").toInt
  val aName: String = "CacheTracker"
  
  if (isMaster) {
  }
  
  var trackerActor: ActorRef = if (isMaster) {
    val actor = actorOf(new CacheTrackerActor)
    remote.register(aName, actor)
    actor.start()
    logInfo("Registered CacheTrackerActor actor @ " + ip + ":" + port)
    actor
  } else {
    remote.actorFor(aName, ip, port)
  }

  val registeredRddIds = new HashSet[Int]

  // Remembers which splits are currently being loaded (on worker nodes)
  val loading = new HashSet[String]
  
  // Registers an RDD (on master only)
  def registerRDD(rddId: Int, numPartitions: Int) {
    registeredRddIds.synchronized {
      if (!registeredRddIds.contains(rddId)) {
        logInfo("Registering RDD ID " + rddId + " with cache")
        registeredRddIds += rddId
        (trackerActor ? RegisterRDD(rddId, numPartitions)).as[Any] match {
          case Some(true) =>
            logInfo("CacheTracker registerRDD " + RegisterRDD(rddId, numPartitions) + " successfully.")
          case Some(oops) =>
            logError("CacheTracker registerRDD" + RegisterRDD(rddId, numPartitions) + " failed: " + oops)
          case None => 
            logError("CacheTracker registerRDD None. " + RegisterRDD(rddId, numPartitions))
            throw new SparkException("Internal error: CacheTracker registerRDD None.")
        }
      }
    }
  }
  
  // For BlockManager.scala only
  def cacheLost(host: String) {
    (trackerActor ? MemoryCacheLost(host)).as[Any] match {
       case Some(true) =>
         logInfo("CacheTracker successfully removed entries on " + host)
       case _ =>
         logError("CacheTracker did not reply to MemoryCacheLost")
    }
  }

  // Get the usage status of slave caches. Each tuple in the returned sequence
  // is in the form of (host name, capacity, usage).
  def getCacheStatus(): Seq[(String, Long, Long)] = {
    (trackerActor ? GetCacheStatus) match {
      case h: Seq[(String, Long, Long)] => h.asInstanceOf[Seq[(String, Long, Long)]]

      case _ =>
        throw new SparkException(
          "Internal error: CacheTrackerActor did not reply with a Seq[Tuple3[String, Long, Long]")
    }
  }
  
  // For BlockManager.scala only
  def notifyTheCacheTrackerFromBlockManager(t: AddedToCache) {
    (trackerActor ? t).as[Any] match {
      case Some(true) =>
        logInfo("CacheTracker notifyTheCacheTrackerFromBlockManager successfully.")
      case Some(oops) =>
        logError("CacheTracker notifyTheCacheTrackerFromBlockManager failed: " + oops)
      case None => 
        logError("CacheTracker notifyTheCacheTrackerFromBlockManager None.")
    }
  }
  
  // Get a snapshot of the currently known locations
  def getLocationsSnapshot(): HashMap[Int, Array[List[String]]] = {
    (trackerActor ? GetCacheLocations).as[Any] match {
      case Some(h: HashMap[_, _]) =>
        h.asInstanceOf[HashMap[Int, Array[List[String]]]]
        
      case _ => 
        throw new SparkException("Internal error: CacheTrackerActor did not reply with a HashMap")
    }
  }
  
  // Gets or computes an RDD split
  def getOrCompute[T](rdd: RDD[T], split: Split, storageLevel: StorageLevel): Iterator[T] = {
    val key = "rdd:%d:%d".format(rdd.id, split.index)
    logInfo("Cache key is " + key)
    blockManager.get(key) match {
      case Some(cachedValues) =>
        // Split is in cache, so just return its values
        logInfo("Found partition in cache!")
        return cachedValues.asInstanceOf[Iterator[T]]

      case None =>
        // Mark the split as loading (unless someone else marks it first)
        loading.synchronized {
          if (loading.contains(key)) {
            logInfo("Loading contains " + key + ", waiting...")
            while (loading.contains(key)) {
              try {loading.wait()} catch {case _ =>}
            }
            logInfo("Loading no longer contains " + key + ", so returning cached result")
            // See whether someone else has successfully loaded it. The main way this would fail
            // is for the RDD-level cache eviction policy if someone else has loaded the same RDD
            // partition but we didn't want to make space for it. However, that case is unlikely
            // because it's unlikely that two threads would work on the same RDD partition. One
            // downside of the current code is that threads wait serially if this does happen.
            blockManager.get(key) match {
              case Some(values) =>
                return values.asInstanceOf[Iterator[T]]
              case None =>
                logInfo("Whoever was loading " + key + " failed; we'll try it ourselves")
                loading.add(key)
            }
          } else {
            loading.add(key)
          }
        }
        // If we got here, we have to load the split
        // Tell the master that we're doing so
        //val host = System.getProperty("spark.hostname", Utils.localHostName)
        //val future = trackerActor !! AddedToCache(rdd.id, split.index, host)
        // TODO: fetch any remote copy of the split that may be available
        // TODO: also register a listener for when it unloads
        logInfo("Computing partition " + split)
        try {
          val values = new ArrayBuffer[Any]
          values ++= rdd.compute(split)
          blockManager.put(key, values.iterator, storageLevel, false)
          //future.apply() // Wait for the reply from the cache tracker
          return values.iterator.asInstanceOf[Iterator[T]]
        } finally {
          loading.synchronized {
            loading.remove(key)
            loading.notifyAll()
          }
        }
    }
  }

  // Called by the Cache to report that an entry has been dropped from it
  def dropEntry(rddId: Int, partition: Int) {
    //TODO - do we really want to use '!!' when nobody checks returned future? '!' seems to enough here.
    trackerActor !! DroppedFromCache(rddId, partition, Utils.localHostName())
  }

  def stop() {
    trackerActor !! StopCacheTracker
    registeredRddIds.clear()
    trackerActor = null
  }
}
