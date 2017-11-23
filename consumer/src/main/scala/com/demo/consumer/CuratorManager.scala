package com.demo.consumer

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.Logger

import scala.collection.JavaConversions._

/**
  * @author sskapci
  */

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

  def create(client: CuratorFramework, path: String, payload: String): Unit = {
    client.create.forPath(path, payload.getBytes("UTF-8"))
  }

  def setData(client: CuratorFramework, path: String, payload: String): Unit = {
    client.setData().forPath(path, payload.getBytes("UTF-8"))
  }

  def readData(client: CuratorFramework, path: String): String = {
    new String(client.getData.forPath(path), "UTF-8")
  }

  def getListChildren(client: CuratorFramework, path: String): List[String] = {
    client.getChildren.forPath(path).toList
  }

  def delete(client: CuratorFramework, path: String): Unit = {
    client.delete.forPath(path)
  }

  def checkExists(client: CuratorFramework, path: String): Boolean = {
    try {
      if (client.checkExists().forPath(path).equals(null))
        false
      else
        true
    }
    catch {
      case e: Exception => false
    }
  }

}
