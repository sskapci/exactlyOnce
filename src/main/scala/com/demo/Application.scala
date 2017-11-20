package com.demo

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Application {

  private val logger = Logger.getLogger(getClass)

  case class KafkaMessageAndMetadata[K, V](key: K, value: V, topic: String, partition: Int, offset: Long) extends Serializable

  def main(args: Array[String]) {
    val conf = new SparkConf(true)
    conf.setAppName(getClass().getSimpleName().dropRight(1))


    val ssc = new StreamingContext(conf, Seconds(2))

    //TODO Change to parameter
    val topicsList: List[String] = List[String]("test1", "test2", "test3")

    var topicsMutableMap: mutable.Map[TopicAndPartition, Long] = mutable.Map()
    topicsMutableMap += TopicAndPartition("test1", 0) -> 0
    val fromOffsets: Map[TopicAndPartition, Long] = topicsMutableMap.toMap

    //TODO Change to parameter
    val kafkaSettingsMap = Map[String, String]("bootstrap.servers" -> "localhost:9092",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val messageHandler = (mmd: MessageAndMetadata[String, String]) => KafkaMessageAndMetadata(mmd.key, mmd.message, mmd.topic, mmd.partition, mmd.offset)

    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, KafkaMessageAndMetadata[String, String]](
      ssc, kafkaSettingsMap, fromOffsets, messageHandler
    )

    stream.foreachRDD { (rdd, time) =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach(z => logger.info("**** OffsetRanges =>  Topic: " + z.topic +
        " Partition: " + z.partition +
        " FromOffset: " + z.fromOffset +
        " UntilOffset: " + z.untilOffset))

      rdd.collect.foreach(z => logger.info(
        "Topic : " + z.topic +
          " Partition : " + z.partition +
          " Offset : " + z.offset +
          " Value : " + z.value
      ))

      if (!rdd.isEmpty()) {
        try {

          val itemsArray = rdd.collect()

          //TODO save offsets in here

        } catch {
          case e: Exception =>
            logger.error("**************Error in Consumer")
            logger.error(e.getMessage)
        }

      }
    }

    ssc.start()
    ssc.awaitTermination()

    sys.exit(0)
  }

}
