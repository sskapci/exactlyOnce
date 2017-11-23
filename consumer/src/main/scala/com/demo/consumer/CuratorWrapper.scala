package com.demo.consumer

import com.demo.consumer.Application.TopicAndPartitionParser
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange

import scala.collection.mutable

/**
  * @author sskapci
  */

object CuratorWrapper {

  //this is for checking and creating zNodes for the topics
  def checkAndPrepareZnodes(connectionString: String, topics: List[TopicAndPartitionParser]): Unit = {
    val cm = new CuratorManager
    val cl = cm.createSimple(connectionString)
    cl.start()
    topics.foreach(f => {
      if (!cm.checkExists(cl, "/" + f.topic)) {
        cm.create(cl, "/" + f.topic, "0")
      }

      for (a <- 0 to f.partition.toInt) {
        if (!cm.checkExists(cl, "/" + f.topic + "/" + a.toString)) {
          cm.create(cl, "/" + f.topic + "/" + a.toString, "0")
        }
      }
    })
    cl.close()
  }

  def readTopicValues(connectionString: String, topics: List[String]): Map[TopicAndPartition, Long] = {
    var topicsMutableMap: mutable.Map[TopicAndPartition, Long] = mutable.Map()

    val cm = new CuratorManager
    val cl = cm.createSimple(connectionString)
    cl.start()
    topics.foreach(f => {

      var counterPartitions = 0

      val dataPaths = cm.getListChildren(cl, "/" + f)
      dataPaths.foreach(v => {
        val data = cm.readData(cl, "/" + f + "/" + v)
        topicsMutableMap += TopicAndPartition(f, counterPartitions) -> data.toLong
        counterPartitions += 1
      })

    })
    cl.close()

    topicsMutableMap.toMap
  }


  def saveOffsets(connectionString: String, offsetArray: Array[OffsetRange]): Unit = {
    val cm = new CuratorManager
    val cl = cm.createSimple(connectionString)
    cl.start()

    offsetArray.foreach(x => {
      cm.setData(cl, "/" + x.topic + "/" + x.partition, x.untilOffset.toString)
    })
    cl.close()
  }

}
