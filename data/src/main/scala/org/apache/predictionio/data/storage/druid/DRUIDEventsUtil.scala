
package org.apache.predictionio.data.storage.druid

import org.apache.predictionio.data.storage.outter.Event
import org.joda.time.DateTime

import scala.collection.mutable.Map

case class Result(var event: Map[String, Any]) {
}

/**
  */
object DRUIDEventsUtil {
  def resultToEvent(result: Result, createTime: DateTime): Event = {
    new Event(
      result.event,
      createTime
    )
  }
}
