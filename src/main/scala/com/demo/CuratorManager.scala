package com.demo

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.transaction.CuratorTransactionResult

import scala.collection.JavaConversions._
import java.util

import org.apache.log4j.Logger

class CuratorManager {
  private val logger = Logger.getLogger(getClass)

  def createSimple(connectionString: String): CuratorFramework = {
    // these are reasonable arguments for the ExponentialBackoffRetry. The first
    // retry will wait 1 second - the second will wait up to 2 seconds - the
    // third will wait up to 4 seconds.
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    // The simplest way to get a CuratorFramework instance. This will use default values.
    // The only required arguments are the connection string and the retry policy
    CuratorFrameworkFactory.newClient(connectionString, retryPolicy)
  }

  @throws[Exception]
  def transactionTest(client: CuratorFramework): util.Collection[CuratorTransactionResult] = { // this example shows how to use ZooKeeper's transactions
    val createOp = client.transactionOp.create.forPath("/test/path1", "some data".getBytes)
    val setDataOp = client.transactionOp.setData.forPath("/test/path2", "other data".getBytes)
    val deleteOp1 = client.transactionOp.delete.forPath("/test/path1")
    val deleteOp2 = client.transactionOp.delete.forPath("/test/path2")
    val results = client.transaction.forOperations(createOp, setDataOp, deleteOp1,deleteOp2)
    for (result <- results) {
      logger.info(result.getForPath + " - " + result.getType)
    }
    results
  }

}
