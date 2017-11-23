package com.demo.consumer

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scopt.OptionParser

import scala.collection.mutable

/**
  * @author sskapci
  */

object Application {

  private val logger = Logger.getLogger(getClass)

  case class KafkaMessageAndMetadata[K, V](key: K, value: V, topic: String, partition: Int, offset: Long) extends Serializable

  case class TopicAndPartitionParser(topic: String, partition: String) extends Serializable

  case class StartArgs(broker: String = null, topics: String = null, zookeeper: String = null) extends Serializable

  val parser: OptionParser[StartArgs] = new scopt.OptionParser[StartArgs]("startArgs") {
    head("exactly once approach", "1.0")
    opt[String]('b', "broker")
      .required()
      .action((f, c) => c.copy(broker = f))
      .valueName("<broker>")
      .text("Broker Address")
    opt[String]('t', "topics")
      .action((f, c) => c.copy(topics = f))
      .valueName("<topics>")
      .text("Topics List separated with comma and number of partitions with semicolon ex: test:2")
    opt[String]('z', "zookeeper")
      .action((f, c) => c.copy(zookeeper = f))
      .valueName("<zookeeper>")
      .text("Zookeeper connection")
  }

  def main(args: Array[String]): Unit = parser.parse(args, StartArgs()) match {
    case None => parser.failure("Failed parsing arguments")

    case Some(confForArgs) => {

      val configuration = new SparkConf(true)
      configuration.setAppName(getClass.getSimpleName)

      val ssc = new StreamingContext(configuration, Seconds(5))

      val topicsList: List[String] = confForArgs.topics.split(",").toList
      val topicsWithPartitions = topicsList.map(x => TopicAndPartitionParser(x.split(":")(0), x.split(":")(1)))
      CuratorWrapper.checkAndPrepareZnodes(confForArgs.zookeeper, topicsWithPartitions)

      val fromOffsets: Map[TopicAndPartition, Long] = CuratorWrapper.readTopicValues(confForArgs.zookeeper, topicsWithPartitions.map(_.topic))

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

            rdd.foreach(x => {
              logger.info("------Result TOPIC: " + x.topic + "  PARTITION: " + x.partition + "  OFFSET: " + x.offset + "  KEY: " + x.key + "  VALUE:" + x.value)
            })

            
            CuratorWrapper.saveOffsets(confForArgs.zookeeper, offsetRanges)
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
