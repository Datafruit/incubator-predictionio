
package org.apache.predictionio.data.storage.druid

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapred._
import org.joda.time.Interval
import spray.client.pipelining._
import spray.http.{HttpRequest, HttpResponse}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class Result {

}

/**
  */
class DruidInputFormat
  extends InputFormat[Long, Result] with Configurable {

  val DATASOURCE = "druid.datasource"
  val COORDINATOR_HOST = "druid.coordinator.host"
  val INTERVAL = "druid.interval"

  private var conf: Configuration = null
  private var ds: String = null
  private var host: String = null
  private var interval: Interval = null
  private implicit val system = ActorSystem()
  import system.dispatcher

  override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    val response: Future[HttpResponse] = pipeline(Get(url()))
    val r = Await.result(response, Duration(10, TimeUnit.SECONDS))
    null
  }

  override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter)
  : RecordReader[Long, Result] = {
    val druidSplit = split.asInstanceOf[DruidInputSplit]
    DruidRecordReader(druidSplit.paths)
  }

  private def url(): String = {
    "http://%s/druid/coordinator/v1/datasources/%s/intervals/%s/serverview?partial=true"
      .format(
        host,
        ds,
        interval.toString.replace("/", "_")
      )
  }


  override def getConf: Configuration = conf

  override def setConf(conf: Configuration): Unit = {
    this.conf = conf

    ds = conf.get(DATASOURCE)
    host = conf.get(COORDINATOR_HOST)
    val interval = conf.get(INTERVAL)
    this.interval = new Interval(interval)
  }
}
