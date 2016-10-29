
package org.apache.predictionio.data.storage.outter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
  */
trait OProperties extends Serializable {
  def find(datasource: String)(sc: SparkContext): RDD[Event]
}
