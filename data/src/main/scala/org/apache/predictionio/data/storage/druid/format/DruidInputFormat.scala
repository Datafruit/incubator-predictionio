
package org.apache.predictionio.data.storage.druid.format

import java.util.concurrent.TimeUnit
import java.util.{Collections, List => jList}

import akka.actor.ActorSystem
import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapreduce._
import org.apache.predictionio.data.storage.druid.Result
import org.joda.time.{DateTime, Interval}
import spray.client.pipelining._
import spray.http.{HttpRequest, HttpResponse}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.parsing.json.JSON

/**
  */
class DruidInputFormat
  extends InputFormat[DateTime, Result] with Configurable {
  private var conf: Configuration = null
  private var ds: String = null
  private var host: String = null
  private var interval: Interval = null
  private implicit val system = ActorSystem()
  import system.dispatcher

  override def getSplits(context: JobContext): jList[InputSplit] = {
    val pipeline: HttpRequest => Future[HttpResponse] = sendReceive
    val response: Future[HttpResponse] = pipeline(Get(url()))
    val r = Await.result(response, Duration(10, TimeUnit.SECONDS))
    JSON.parseFull(r.entity.asString) match {
      case Some(list: List[Map[String, Map[String, Any]]]) =>
        list.map({
          map =>
            new DruidInputSplit(
              Array(map("segment")("loadSpec").asInstanceOf[Map[String, String]]("path")))
        }).asJava.asInstanceOf[jList[InputSplit]]
      case other =>
        Collections.emptyList()
    }
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
  : RecordReader[DateTime, Result] = {
    val druidSplit = split.asInstanceOf[DruidInputSplit]
    val conf = new Configuration()
    DruidRecordReader(FileSystem.get(conf), druidSplit.paths)
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

    ds = conf.get(DruidInputFormat.DATASOURCE)
    host = conf.get(DruidInputFormat.COORDINATOR_URL)
    val interval = conf.get(DruidInputFormat.INTERVAL)
    this.interval = new Interval(interval)
  }

}

object DruidInputFormat {
  val DATASOURCE = "druid.datasource"
  val COORDINATOR_URL = "druid.coordinator.url"
  val INTERVAL = "druid.interval"

}