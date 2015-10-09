package flintApp

/**
 * Created by root on 15-10-8.
 */

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.util.Arrays

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.io.WritableComparator
import org.apache.spark.bagel.{Message, Vertex}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.{ShuffledRDD, RDD}

class PRVertex() extends Vertex with Serializable {
  var value: Double = _
  var outEdges: Array[Int] = _
  var active: Boolean = _

  def this(value: Double, outEdges: Array[Int], active: Boolean = true) {
    this()
    this.value = value
    this.outEdges = outEdges
    this.active = active
  }

  override def toString(): String = {
    "PRVertex(value=%f, outEdges.length=%d, active=%s)"
      .format(value, outEdges.length, active.toString)
  }
}

class PRMessage() extends Message[Int] with Serializable {
  var targetId: Int = _
  var value: Double = _

  def this(targetId: Int, value: Double) {
    this()
    this.targetId = targetId
    this.value = value
  }

  override def toString():String={
    "PRMessage(targetId="+targetId+", value="+value+")"
  }
}

class VertexChunk(size:Int,threshold:Int,rate:Int) extends ByteArrayOutputStream(size:Int) with Serializable{self =>

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
  def getInitValueIterator() = new Iterator[(Int, PRMessage)] {
    var offset = 0

    override def hasNext = offset < self.count

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        val srcId = WritableComparator.readInt(buf, offset)
        offset += 4
        val value = WritableComparator.readDouble(buf, offset)
        offset += 8
        val size = WritableComparator.readInt(buf, offset)
        offset += 4 + 4 * size
        offset += 1
        (srcId, new PRMessage(srcId, value))
      }
    }
  }

  def getResult() = new Iterator[(Int, Double)] {
    var offset = 0

    override def hasNext() = {
      if (offset >= self.count) false else true
    }

    override def next() = {
      val key = WritableComparator.readInt(buf, offset)
      offset += 4
      val value = WritableComparator.readDouble(buf, offset)
      offset += 8
      val numDests = WritableComparator.readInt(buf, offset)
      offset += 4 + 4 * numDests
      offset += 1
      val numPRMessages = WritableComparator.readInt(buf,offset)
      offset += 4
      offset += numPRMessages * ( 4 + 8 )
      (key, value)
    }
  }

  // format: Input=((Int,PRVertex),(Int,PRMessage)), Output=(Iterator(Int,(PRVertext,Array[PRMessage])))
  def getMessageIterator(vertices: Iterator[(Int, PRMessage)]) :Iterator[VertexChunk] = {

    var offset = 0
    var currentMessage: (Int, PRMessage) = null
    var changeMessage = true
    val chunk = new VertexChunk(self.count+32,self.count+32,1024*1024)
    val dos = new DataOutputStream(chunk)

    while(offset < self.count){
      if (changeMessage && vertices.hasNext) {
        currentMessage = vertices.next()
        changeMessage = false
      }
      val currentKey = WritableComparator.readInt(buf, offset)
      offset += 4
      val currentValue = WritableComparator.readDouble(buf, offset)
      offset += 8
      val currentDestm = WritableComparator.readInt(buf, offset)
      offset += 4
      var newValue = 0.0
      var newMessageValue = 0.0
      while (currentKey > currentMessage._1 && vertices.hasNext) {
        currentMessage = vertices.next()
      }
      if (currentKey == currentMessage._1) {
        changeMessage = true
        newValue = 0.15 + 0.85 * currentMessage._2.value
        newMessageValue = newValue / currentDestm
      } else {
        newValue = currentValue
        newMessageValue = newValue / currentDestm
      }
      dos.writeInt(currentKey)
      dos.writeDouble(newValue)
      dos.writeInt(currentDestm)
      val outMessgaes = new Array[PRMessage](currentDestm)
      for (i <- 0 until currentDestm) {
        val outEdge = WritableComparator.readInt(buf, offset)
        offset += 4
        dos.writeInt(outEdge)
        outMessgaes.update(i, new PRMessage(outEdge, newMessageValue))
      }
      offset += 1
      dos.writeBoolean(true)

      dos.writeInt(outMessgaes.length)
      outMessgaes.foreach(pr => {
        dos.writeInt(pr.targetId)
        dos.writeDouble(pr.value)
      })
    }
    Iterator(chunk)
  }

  def getMessageIterator2(vertices: Iterator[(Int, PRMessage)]) :Iterator[VertexChunk] = {

    var offset = 0
    var currentMessage: (Int, PRMessage) = null
    var changeMessage = true
    val chunk = new VertexChunk(self.count+32,self.count+32,1024*1024)
    val dos = new DataOutputStream(chunk)

    while(offset < self.count){
      if (changeMessage && vertices.hasNext) {
        currentMessage = vertices.next()
        changeMessage = false
      }
      val currentKey = WritableComparator.readInt(buf, offset)
      offset += 4
      val currentValue = WritableComparator.readDouble(buf, offset)
      offset += 8
      val currentDestm = WritableComparator.readInt(buf, offset)
      offset += 4
      var newValue = 0.0
      var newMessageValue = 0.0
      while (currentKey > currentMessage._1 && vertices.hasNext) {
        currentMessage = vertices.next()
      }
      if (currentKey == currentMessage._1) {
        changeMessage = true
        newValue = 0.15 + 0.85 * currentMessage._2.value
        newMessageValue = newValue / currentDestm
      } else {
        newValue = currentValue
        newMessageValue = newValue / currentDestm
      }
      dos.writeInt(currentKey)
      dos.writeDouble(newValue)
      dos.writeInt(currentDestm)
      val outMessgaes = new Array[PRMessage](currentDestm)
      for (i <- 0 until currentDestm) {
        val outEdge = WritableComparator.readInt(buf, offset)
        offset += 4
        dos.writeInt(outEdge)
        outMessgaes.update(i, new PRMessage(outEdge, newMessageValue))
      }
      offset += 1
      dos.writeBoolean(true)

      dos.writeInt(outMessgaes.length)
      outMessgaes.foreach(pr => {
        dos.writeInt(pr.targetId)
        dos.writeDouble(pr.value)
      })

      val numPRMessgaes = WritableComparator.readInt(buf,offset)
      offset += 4
      offset += numPRMessgaes * ( 4 + 8)

    }
    Iterator(chunk)
  }

  def getEdges():Iterator[VertexChunk]={
    var offset = 0
    val chunk = new VertexChunk(self.count/2,self.count/2,1024*1024)
    val dos = new DataOutputStream(chunk)

    while(offset<self.count){
      val key = WritableComparator.readInt(buf, offset)
      offset += 4
      dos.writeInt(key)

      val prValue = WritableComparator.readDouble(buf, offset)
      offset += 8
      dos.writeDouble(prValue)
      val outEdgesNum = WritableComparator.readInt(buf, offset)
      offset += 4
      dos.writeInt(outEdgesNum)
      for (i <- 0 until outEdgesNum) {
        val outEdge = WritableComparator.readInt(buf, offset)
        offset += 4
        dos.writeInt(outEdge)
      }
      offset += 1
      dos.writeBoolean(true)
      val numMsg = WritableComparator.readInt(buf,offset)
      offset += 4
      offset += (4+8)*numMsg
    }
    Iterator(chunk)

  }

  def getRanks() = new Iterator[(Int,PRMessage)]{

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
        offset += 8
        val prVetextOutNum = WritableComparator.readInt(buf,offset)
        offset += 4 + 4 * prVetextOutNum
        offset += 1
        currentDestNum = WritableComparator.readInt(buf,offset)
        offset += 4
      }
      currentIndex += 1
      val newMsgId = WritableComparator.readInt(buf,offset)
      offset += 4
      val newMsgValue = WritableComparator.readDouble(buf,offset)
      offset += 8

      if(currentIndex == currentDestNum) changeKey=true

      (newMsgId,new PRMessage(newMsgId,newMsgValue))
    }
  }

}

class PRKryoRegistrator extends KryoRegistrator {
  def registerClasses(kryo: Kryo) {
    kryo.register(classOf[PRVertex])
    kryo.register(classOf[PRMessage])
    kryo.register(classOf[VertexChunk])
  }
}

object FlintBagelPR {
  private val ordering = implicitly[Ordering[Int]]

  def testOptimized(groupedEdges: RDD[(Int,PRVertex)],iters:Int,savePath:String,initSize:Int,thershold:Int,rate:Int) {
    var superstep = 0

    val edges = groupedEdges.mapPartitions ( iter => {
      val chunk = new VertexChunk(initSize,thershold,rate)
      val dos = new DataOutputStream(chunk)
      for ((src, dests) <- iter) {
        dos.writeInt(src)
        dos.writeDouble(dests.value)
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
    var lastRDD:RDD[VertexChunk] = cacheContribs

    do{

      superstep += 1

      ranks = cacheContribs.mapPartitions ( iters => {
        val chunk = iters.next()
        chunk.getRanks()
      }).mapValues(_.value).reduceByKey(_+_)
        .asInstanceOf[ShuffledRDD[Int,_,_]].setKeyOrdering(ordering).asInstanceOf[RDD[(Int,Double)]]
        .map(t => {
        (t._1,new PRMessage(t._1,t._2))
      })
      //.sortByKey()

      val processCacheContribs = cacheContribs.zipPartitions(ranks){ (EIter, VIter) =>
        val chunk = EIter.next()
        // format: Input=((Int,PRVertex),(Int,PRMessage)), Output=(Iterator(VertexChunk))
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
    //result.foreach(t => println(t))
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(args(4))
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      set("spark.kryo.registrator",classOf[PRKryoRegistrator].getName)
    val spark = new SparkContext(conf)

    //Logger.getRootLogger.setLevel(Level.FATAL)

    val numPartitions = args(1).toInt
    val lines = spark.textFile(args(0),numPartitions)

    //    val links = lines.map{ s =>
    //      val fields = s.split("\\s+")
    //      val title = fields(0)
    //      val length = fields.length-1
    //      val outEdges = new Array[Int](length)
    //      for(i <- 0 until length)
    //        outEdges.update(i, fields(i+1).toInt)
    //      val id = title.toInt
    //      (id, new PRVertex(1.0 , outEdges))
    //    }.partitionBy(new HashPartitioner(numPartitions))
    //      .asInstanceOf[ShuffledRDD[Int,_,_]]
    //      .setKeyOrdering(ordering)
    //      .asInstanceOf[RDD[(Int,PRVertex)]]

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
      (id,new PRVertex(1.0, list))
    })

    val iters = args(2).toInt
    val initChunkSize = args(5).toInt
    val thresholdSize = args(6).toInt
    val rateSize = args(7).toInt
    testOptimized(links,iters,args(3),initChunkSize,thresholdSize,rateSize)
  }
}

