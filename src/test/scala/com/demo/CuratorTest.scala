package com.demo

import org.apache.log4j.Logger
import org.scalatest.{BeforeAndAfter, FunSpec, Matchers}
import org.apache.curator.test.TestingServer

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
    for(x<- listOfZnode){
      logger.info(x)
    }

    val data = curatorManager.readData(curatorClient, "/test")
    logger.info(data)

    val check=curatorManager.checkExists(curatorClient,"/test")
    logger.info(check)

    curatorManager.delete(curatorClient,"/test")
    curatorClient.close()
  }

}
