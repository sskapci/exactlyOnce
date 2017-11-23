package com.demo.consumer

import com.demo.consumer.Application.TopicAndPartitionParser
import kafka.common.TopicAndPartition
import org.apache.log4j.Logger
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.apache.curator.test.TestingServer
import org.apache.spark.streaming.kafka.OffsetRange

import scala.collection.mutable.ArrayBuffer

/**
  * @author Safak Kapci
  */

class CuratorTest extends FunSpec with BeforeAndAfter with Matchers {

  private val logger = Logger.getLogger(getClass)

  var zkServer: TestingServer = _

  before {
    zkServer = new TestingServer(2180, true)
  }

  after {
    zkServer.stop()
  }

  it("connect zookeeper, create a node, list nodes,read value and delete the znode") {
    val curatorManager = new CuratorManager()
    val curatorClient = curatorManager.createSimple(zkServer.getConnectString)
    curatorClient.start()

    curatorManager.create(curatorClient, "/test", "test123")
    val listOfZnode = curatorManager.getListChildren(curatorClient, "/")
    for (x <- listOfZnode) {
      logger.info(x)
    }

    val data = curatorManager.readData(curatorClient, "/test")
    logger.info(data)

    val check1 = curatorManager.checkExists(curatorClient, "/test")
    logger.info(check1)

    val check2 = curatorManager.checkExists(curatorClient, "/test1")
    logger.info(check2)

    curatorManager.delete(curatorClient, "/test")
    curatorClient.close()
  }

  it("should Prepare Znodes ") {
    val topicsList: List[String] = "testTopic1:5,testTopic2:3".split(",").toList
    val topicsWithPartitions = topicsList.map(x => TopicAndPartitionParser(x.split(":")(0), x.split(":")(1)))
    CuratorWrapper.checkAndPrepareZnodes(zkServer.getConnectString, topicsWithPartitions)

    val fromOffsets: Map[TopicAndPartition, Long] = CuratorWrapper.readTopicValues(zkServer.getConnectString, topicsWithPartitions.map(_.topic))

    for (x <- fromOffsets) {
      logger.info(x._1.topic + " " + x._1.partition + " " + x._2)
    }

    val offsetRanges: ArrayBuffer[OffsetRange] = new ArrayBuffer[OffsetRange]()
    offsetRanges.append(OffsetRange.create("testTopic1", 0, 0, 100))
    offsetRanges.append(OffsetRange.create("testTopic1", 1, 0, 100))
    offsetRanges.append(OffsetRange.create("testTopic1", 2, 0, 100))
    offsetRanges.append(OffsetRange.create("testTopic1", 3, 0, 100))
    offsetRanges.append(OffsetRange.create("testTopic1", 4, 0, 100))

    offsetRanges.append(OffsetRange.create("testTopic2", 0, 0, 100))
    offsetRanges.append(OffsetRange.create("testTopic2", 1, 0, 100))
    offsetRanges.append(OffsetRange.create("testTopic2", 2, 0, 100))


    CuratorWrapper.saveOffsets(zkServer.getConnectString, offsetRanges.toArray)

  }


}
