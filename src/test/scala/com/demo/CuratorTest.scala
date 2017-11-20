package com.demo

import org.scalatest.{FunSpec, Matchers}

class CuratorTest extends FunSpec with Matchers {

  it("Should create and delete znode") {

    val curatorManager = new CuratorManager()
    val curatorClient = curatorManager.createSimple("localhost:2181")
    curatorManager.transactionTest(curatorClient)

  }

}
