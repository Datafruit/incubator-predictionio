
package org.apache.predictionio.data.storage.outter

import org.joda.time.DateTime
import scala.collection.mutable.Map

/**
  */
case class Event(
  val properties: Map[String, Any],
  val createTime: DateTime = DateTime.now
) {

}
