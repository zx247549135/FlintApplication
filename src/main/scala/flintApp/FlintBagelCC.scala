package flintApp

/**
 * Created by root on 15-10-9.
 */

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util.Arrays

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.WritableComparator
import org.apache.spark.bagel.{Message, Vertex}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.{ShuffledRDD, RDD}

class CCVertex() extends Vertex with Serializable {
  var value: Int = _
  var outEdges: Array[Int] = _
  var active: Boolean = _

  def this(value: Int, outEdges: Array[Int], active: Boolean = true) {
    this()
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }

  override def toString(): String = {
    "PRVertex(value=%d, outEdges.length=%d, active=%s)"
      .format(value, outEdges.length, active.toString)
  }
}

class CCMessage() extends Message[Int] with Serializable {
  var targetId: Int = _
  var value: Int = _

  def this(targetId: Int, value: Int) {
    this()
    this.targetId = targetId
    this.value = value
  }

  override def toString():String={
    "PRMessage(targetId="+targetId+", value="+value+")"
  }
}

class CCChunk(size:Int,threshold:Int,rate:Int) extends ByteArrayOutputStream(size:Int) with Serializable{self =>

  def ensureCapacity(minCapacity: Int) {
    if (minCapacity - buf.length > 0) grow(minCapacity)
  }

  def grow (minCapacity: Int) {
    val oldCapacity: Int = buf.length
    var newCapacity: Int =
      if(oldCapacity < threshold)
        oldCapacity << 1
      else
        oldCapacity + rate
    if (newCapacity - minCapacity < 0) newCapacity = minCapacity
    if (newCapacity < 0) {
      if (minCapacity < 0) throw new OutOfMemoryError
      newCapacity = Integer.MAX_VALUE
    }
    buf = Arrays.copyOf(buf, newCapacity)
  }

  override def write (b: Int) {
    ensureCapacity(count + 1)
    buf(count) = b.toByte
    count += 1
  }

  // format: Input=((Int,PRVertex)), Output=(Iterator(Int,PRMessage))
  def getInitValueIterator() = new Iterator[(Int, CCMessage)] {
    var offset = 0

    override def hasNext = offset < self.count

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        val srcId = WritableComparator.readInt(buf, offset)
        offset += 4
        val value = WritableComparator.readInt(buf, offset)
        offset += 4
        val size = WritableComparator.readInt(buf, offset)
        offset += 4 + 4 * size
        offset += 1
        (srcId, new CCMessage(srcId, value))
      }
    }
  }

  def getResult() = new Iterator[(Int, Int)] {
    var offset = 0

    override def hasNext() = {
      if (offset >= self.count) false else true
    }

    override def next() = {
      val key = WritableComparator.readInt(buf, offset)
      offset += 4
      val value = WritableComparator.readInt(buf, offset)
      offset += 4
      val numDests = WritableComparator.readInt(buf, offset)
      offset += 4 + 4 * numDests
      offset += 1
      val numCCMessages = WritableComparator.readInt(buf,offset)
      offset += 4
      offset += numCCMessages * ( 4 + 4 )
      (key, value)
    }
  }

  // format: Input=((Int,CCVertex),(Int,CCMessage)), Output=(Iterator(Int,(CCVertext,Array[CCMessage])))
  def getMessageIterator(vertices: Iterator[(Int, CCMessage)]) :Iterator[CCChunk] = {

    var offset = 0
    var currentMessage: (Int, CCMessage) = null
    var changeMessage = true
    val chunk = new CCChunk(self.count+32,self.count+32,1024*1024)
    val dos = new DataOutputStream(chunk)

    while(offset < self.count){
      if (changeMessage && vertices.hasNext) {
        currentMessage = vertices.next()
        changeMessage = false
      }
      val currentKey = WritableComparator.readInt(buf, offset)
      offset += 4
      val currentValue = WritableComparator.readInt(buf, offset)
      offset += 4
      val currentDestm = WritableComparator.readInt(buf, offset)
      offset += 4
      var newValue = 0
      var newMessageValue = 0
      while (currentKey > currentMessage._1 && vertices.hasNext) {
        currentMessage = vertices.next()
      }
      if (currentKey == currentMessage._1) {
        changeMessage = true
        newValue = math.min(currentMessage._2.value,currentValue)
      } else {
        newValue = currentValue
      }
      dos.writeInt(currentKey)
      dos.writeInt(newValue)
      dos.writeInt(currentDestm)
      val outMessgaes = new Array[CCMessage](currentDestm)
      for (i <- 0 until currentDestm) {
        val outEdge = WritableComparator.readInt(buf, offset)
        offset += 4
        dos.writeInt(outEdge)
        newMessageValue = math.min(newValue,outEdge)
        outMessgaes.update(i, new CCMessage(outEdge, newMessageValue))
      }
      offset += 1
      dos.writeBoolean(true)

      dos.writeInt(outMessgaes.length)
      outMessgaes.foreach(pr => {
        dos.writeInt(pr.targetId)
        dos.writeInt(pr.value)
      })
    }
    Iterator(chunk)
  }

  def getMessageIterator2(vertices: Iterator[(Int, CCMessage)]) :Iterator[CCChunk] = {

    var offset = 0
    var currentMessage: (Int, CCMessage) = null
    var changeMessage = true
    val chunk = new CCChunk(self.count+32,self.count+32,1024*1024)
    val dos = new DataOutputStream(chunk)

    while(offset < self.count){
      if (changeMessage && vertices.hasNext) {
        currentMessage = vertices.next()
        changeMessage = false
      }
      val currentKey = WritableComparator.readInt(buf, offset)
      offset += 4
      val currentValue = WritableComparator.readInt(buf, offset)
      offset += 4
      val currentDestm = WritableComparator.readInt(buf, offset)
      offset += 4
      var newValue = 0
      var newMessageValue = 0
      while (currentKey > currentMessage._1 && vertices.hasNext) {
        currentMessage = vertices.next()
      }
      if (currentKey == currentMessage._1) {
        changeMessage = true
        newValue = math.min(currentMessage._2.value,currentValue)
      } else {
        newValue = currentValue
      }
      dos.writeInt(currentKey)
      dos.writeInt(newValue)
      dos.writeInt(currentDestm)
      val outMessgaes = new Array[CCMessage](currentDestm)
      for (i <- 0 until currentDestm) {
        val outEdge = WritableComparator.readInt(buf, offset)
        offset += 4
        dos.writeInt(outEdge)
        newMessageValue = math.min(newValue,outEdge)
        outMessgaes.update(i, new CCMessage(outEdge, newMessageValue))
      }
      offset += 1
      dos.writeBoolean(true)

      dos.writeInt(outMessgaes.length)
      outMessgaes.foreach(pr => {
        dos.writeInt(pr.targetId)
        dos.writeInt(pr.value)
      })

      val numCCMessgaes = WritableComparator.readInt(buf,offset)
      offset += 4
      offset += numCCMessgaes * ( 4 + 4)

    }
    Iterator(chunk)
  }

  def getRanks() = new Iterator[(Int,CCMessage)]{

    var offset = 0
    var currentKey = 0
    var currentDestNum = 0
    var currentIndex = 0
    var changeKey = true


    override def hasNext = offset<self.count
    override def next() = {
      if(changeKey){
        changeKey = false
        currentIndex = 0
        currentKey = WritableComparator.readInt(buf,offset)
        offset += 4
        offset += 4
        val prVetextOutNum = WritableComparator.readInt(buf,offset)
        offset += 4 + 4 * prVetextOutNum
        offset += 1
        currentDestNum = WritableComparator.readInt(buf,offset)
        offset += 4
      }
      currentIndex += 1
      val newMsgId = WritableComparator.readInt(buf,offset)
      offset += 4
      val newMsgValue = WritableComparator.readInt(buf,offset)
      offset += 4

      if(currentIndex == currentDestNum) changeKey=true

      (newMsgId,new CCMessage(newMsgId,newMsgValue))
    }
  }

}

class CCKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[CCVertex])
    kryo.register(classOf[CCMessage])
    kryo.register(classOf[CCChunk])
  }
}

object FlintBagelCC {
  private val ordering = implicitly[Ordering[Int]]

  def testOptimized(groupedEdges: RDD[(Int,CCVertex)],iters:Int,savePath:String,initSize:Int,thershold:Int,rate:Int) {
    var superstep = 0

    val edges = groupedEdges.mapPartitions ( iter => {
      val chunk = new CCChunk(initSize,thershold,rate)
      val dos = new DataOutputStream(chunk)
      for ((src, dests) <- iter) {
        dos.writeInt(src)
        dos.writeInt(dests.value)
        dos.writeInt(dests.outEdges.length)
        dests.outEdges.foreach(dos.writeInt)
        dos.writeBoolean(dests.active)
      }
      Iterator(chunk)
    },true).cache()

    var ranks = edges.mapPartitions(iter => {
      val chunk = iter.next()
      // format: Input=((Int,PRVertex)), Output=(Iterator(Int,PRMessage))
      chunk.getInitValueIterator()
    },true)

    var cacheContribs = edges.zipPartitions(ranks){ (EIter, VIter) =>
      val chunk = EIter.next()
      // format: Input=((Int,PRVertex),(Int,PRMessage)), Output=(Iterator(VertexChunk))
      chunk.getMessageIterator(VIter)
    }.cache()
    var lastRDD:RDD[CCChunk] = cacheContribs

    do{

      superstep += 1

      ranks = cacheContribs.mapPartitions ( iters => {
        val chunk = iters.next()
        chunk.getRanks()
      }).mapValues(_.value).reduceByKey((v1,v2) => math.min(v1,v2))
        .asInstanceOf[ShuffledRDD[Int,_,_]].setKeyOrdering(ordering).asInstanceOf[RDD[(Int,Int)]]
        .map(t => {
        (t._1,new CCMessage(t._1,t._2))
      })
      //.sortByKey()

      val processCacheContribs = cacheContribs.zipPartitions(ranks){ (EIter, VIter) =>
        val chunk = EIter.next()
        // format: Input=((Int,CCVertex),(Int,CCMessage)), Output=(Iterator(VertexChunk))
        chunk.getMessageIterator2(VIter)
      }.cache()
      processCacheContribs.foreach(_ => Unit)
      if(lastRDD!=null)
        lastRDD.unpersist(false)
      lastRDD = processCacheContribs

      cacheContribs = processCacheContribs

    } while(superstep < iters)

    val result = cacheContribs.mapPartitions(iter => {
      val chunk = iter.next()
      chunk.getResult()
    })

    result.saveAsTextFile(savePath)
    val count = result.map(_._2).distinct().count()
    println("the count of connected components is " + count)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(args(4))
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator",classOf[CCKryoRegistrator].getName)
    val spark = new SparkContext(conf)

    //Logger.getRootLogger.setLevel(Level.FATAL)

    val numPartitions = args(1).toInt
    val lines = spark.textFile(args(0),numPartitions)

    val links = lines.map( s => {
      val fields = s.split("\\s+")
      val title = fields(0).toInt
      val target = fields(1).toInt
      (title,target)
    }).groupByKey().asInstanceOf[ShuffledRDD[Int,_,_]]
      .setKeyOrdering(ordering)
      .asInstanceOf[RDD[(Int,Iterable[Int])]]
      .map(lines => {
      val id = lines._1
      val list = lines._2.toArray
      (id,new CCVertex(id, list))
    })

    val iters = args(2).toInt
    val initChunkSize = args(5).toInt
    val thresholdSize = args(6).toInt
    val rateSize = args(7).toInt
    testOptimized(links,iters,args(3),initChunkSize,thresholdSize,rateSize)
  }
}

