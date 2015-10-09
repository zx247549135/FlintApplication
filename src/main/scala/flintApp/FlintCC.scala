package flintApp

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

/**
 * Created by root on 15-10-8.
 */

class VerticesChunk() extends ByteArrayOutputStream with Serializable { self =>

  def getInitMessage() = new Iterator[(Int,Int)]{
    var offset = 0

    override def hasNext = offset<self.count

    override def next()={
      if(!hasNext) Iterator.empty.next()
      else{
        val vertexId = WritableComparator.readInt(buf,offset)
        offset += 4
        val numOutEdges = WritableComparator.readInt(buf,offset)
        offset += 4 + 4 * numOutEdges
        (vertexId,vertexId)
      }
    }

  }

  def getG(message:Iterator[(Int,Int)]) ={
    val chunk = new VerticesChunk()
    val dos = new DataOutputStream(chunk)
    var offset = 0
    var changeMsg = true
    var msg:(Int,Int) = (Int.MaxValue,Int.MaxValue)

    while(offset < self.count) {
      if (changeMsg && message.hasNext) {
        msg = message.next()
        changeMsg = false
      }
      val currentKey = WritableComparator.readInt(buf, offset)
      offset += 4
      dos.writeInt(currentKey)
      if (currentKey == msg._1) {
        dos.writeInt(math.min(WritableComparator.readInt(buf, offset), msg._2))
        offset += 4
        changeMsg = true
      }else{
        dos.writeInt(WritableComparator.readInt(buf,offset))
        offset += 4
      }
    }
    Iterator(chunk)
  }

  def getMessages(vIter:Iterator[(Int,Int)]) = new Iterator[(Int,Int)]{
    var changeVertex = true
    var offset = 0
    var currentDestIndex = 0
    var currentDestNum = 0
    var currentContrib = 0

    private def matchVertices(): Boolean = {
      assert(changeVertex)

      if (offset >= self.count) return false

      var matched = false
      while (!matched && vIter.hasNext) {
        val currentVertex = vIter.next()
        while (currentVertex._1 > WritableComparator.readInt(buf, offset)) {
          offset += 4
          val numDests = WritableComparator.readInt(buf, offset)
          offset += 4 + 4 * numDests

          if (offset >= self.count) return false
        }
        if (currentVertex._1 == WritableComparator.readInt(buf, offset)) {
          matched = true
          offset += 4
          currentDestNum = WritableComparator.readInt(buf, offset)
          offset += 4
          currentDestIndex = 0
          currentContrib = currentVertex._2
          changeVertex = false
        }
      }
      matched
    }

    override def hasNext = !changeVertex || matchVertices()

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        currentDestIndex += 1
        if (currentDestIndex == currentDestNum) changeVertex = true

        val destId = WritableComparator.readInt(buf, offset)
        offset += 4

        (destId, math.min(currentContrib,destId))
      }
    }

  }

}

object FlintCC {

  private val ordering = implicitly[Ordering[Int]]

  def main(args:Array[String]){

    val sparkConf = new SparkConf().setAppName(args(4))
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0),args(1).toInt)
    val iterations = args(2).toInt

    val edges1 = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }
    val edges2 = edges1.map(eMsg => (eMsg._2,eMsg._1))
    val edges = (edges1 ++ edges2).distinct()

    val g = edges.groupByKey().asInstanceOf[ShuffledRDD[Int,_,_]]
      .setKeyOrdering(ordering).asInstanceOf[RDD[(Int,Iterable[Int])]]
      .mapPartitions( eMsg => {
      val chunk = new VerticesChunk()
      val dos = new DataOutputStream(chunk)
      eMsg.foreach(vtx => {
        dos.writeInt(vtx._1)
        dos.writeInt(vtx._2.toList.length)
        vtx._2.foreach(dos.writeInt)
      })
      Iterator(chunk)
    }).cache()

    g.foreach(_ => Unit)

    var messages = g.mapPartitions( EIter => {
      val chunk = EIter.next()
      chunk.getInitMessage()
    },true)

    for( i <- 1 to iterations){

      val newVertices = g.zipPartitions(messages){(GIter,MIter) => {
        val chunk = GIter.next()
        chunk.getMessages(MIter)
      }}

      messages = newVertices.reduceByKey((v1,v2) => math.min(v1,v2))
        .asInstanceOf[ShuffledRDD[Int,_,_]].setKeyOrdering(ordering).asInstanceOf[RDD[(Int,Int)]]
    }

    messages.saveAsTextFile(args(3))
    val result = messages.map(_._2).distinct.count()
    println("the count of connected components is "+result)

  }

}
