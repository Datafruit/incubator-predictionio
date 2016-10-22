
package org.apache.predictionio.data.storage.druid

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import org.apache.hadoop.conf.Configuration
import spray.http._
import spray.client.pipelining._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}


/**
  */
object Test {
  def main(args: Array[String]): Unit = {
    val format = new DruidInputFormat
    val conf = new Configuration()
    conf.set("druid.datasource", "rujia1")
    conf.set("druid.coordinator.host", "192.168.0.219:8081")
    conf.set("druid.interval", "2014-01-01/2014-12-01")
    format.setConf(conf)
    format.getSplits(null, 10)

  }
}
