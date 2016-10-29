
package org.apache.predictionio.data.storage.druid

import org.apache.hadoop.conf.Configuration
import org.apache.predictionio.data.storage.StorageClientConfig
import org.apache.predictionio.data.storage.druid.format.DruidInputFormat
import org.apache.predictionio.data.storage.outter.{Event, OEvents}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTime, Interval}

/**
  */
class DRUIDOEvents(client: String, config: StorageClientConfig, namespace: String) extends OEvents {
  def find(dataSource: String,
          startTime: DateTime,
          untilTime: DateTime)(sc: SparkContext): RDD[Event] = {
    val conf = new Configuration()
    conf.set(DruidInputFormat.DATASOURCE, dataSource)
    conf.set(DruidInputFormat.COORDINATOR_URL, client)
    conf.set(DruidInputFormat.INTERVAL, new Interval(startTime, untilTime).toString)

    val rdd = sc
      .newAPIHadoopRDD(conf, classOf[DruidInputFormat], classOf[DateTime], classOf[Result])
      .map {
        case (key, row) => DRUIDEventsUtil.resultToEvent(row, key)
      }
    rdd
  }
}
