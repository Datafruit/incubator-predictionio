
package org.apache.predictionio.data.storage.outter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  */
trait OEvents extends Serializable {
  def find(dataSource: String,
           startTime: DateTime,
           untilTime: DateTime)(sc: SparkContext): RDD[Event]
}
