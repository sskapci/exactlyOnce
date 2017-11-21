package com.demo.consumer

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Application {

  private val logger = Logger.getLogger(getClass)

  case class KafkaMessageAndMetadata[K, V](key: K, value: V, topic: String, partition: Int, offset: Long) extends Serializable

  case class StartArgs(broker: String = null, topics: String = null, zookeeper: String = null)

  val parser = new scopt.OptionParser[StartArgs]("startArgs") {
    head("exactly once approach", "1.0")
    opt[String]('b', "broker")
      .required()
      .action((f, c) => c.copy(broker = f))
      .valueName("<broker>")
      .text("Broker Address")
    opt[String]('t', "topics")
      .action((f, c) => c.copy(topics = f))
      .valueName("<topics>")
      .text("Topics List")
    opt[String]('z', "zookeeper")
      .action((f, c) => c.copy(topics = f))
      .valueName("<topics>")
      .text("Zookeeper Topic")
  }

  def main(args: Array[String]) = parser.parse(args, StartArgs()) match {
    case None => parser.failure("Failed parsing arguments")

    case Some(confForArgs) => {

      val configuration = new SparkConf(true)
      configuration.setAppName(getClass().getSimpleName())

      val ssc = new StreamingContext(configuration, Seconds(2))

      val topicsList: List[String] = confForArgs.topics.split(",").toList

      var topicsMutableMap: mutable.Map[TopicAndPartition, Long] = mutable.Map()
      topicsMutableMap += TopicAndPartition("test1", 0) -> 0
      val fromOffsets: Map[TopicAndPartition, Long] = topicsMutableMap.toMap

      val kafkaSettingsMap = Map[String, String]("bootstrap.servers" -> confForArgs.broker,
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

  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) instance = new SQLContext(sparkContext)
      instance
    }
  }

}
