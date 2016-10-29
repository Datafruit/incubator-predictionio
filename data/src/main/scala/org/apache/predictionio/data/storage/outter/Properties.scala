
package org.apache.predictionio.data.storage.outter

import scala.collection.mutable.Map

/**
  */
case class Properties(
   content: Map[String, Any]
) {
  override def toString(): String = {
    s"Event(content=$content)"
  }
}
