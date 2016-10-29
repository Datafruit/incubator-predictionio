
package org.apache.predictionio.data.store

import org.apache.predictionio.data.storage.Storage
import org.apache.predictionio.data.storage.outter.Event
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  */
object OEventStore {
  @transient lazy private val eventsDb = Storage.getOEvents()

  def find(datasource: String,
           startTime: DateTime,
           untilTime: DateTime)(sc: SparkContext): RDD[Event] = {
    eventsDb.find(datasource, startTime, untilTime)(sc)
  }
}
