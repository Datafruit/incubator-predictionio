
package org.apache.predictionio.data.storage.druid.reader

import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.store.Directory
import org.joda.time.DateTime

import scala.collection.mutable.Map

/**
  */
class DirectoryReader(fieldMappings: FieldMappings,
                      directory: Directory) {
  val leafReader = DirectoryReader.open(directory).getContext.leaves.get(0).reader
  val cursor = new LuceneCursor(fieldMappings, leafReader)
  val timeDimensionSelector = cursor.makeLongDimensionSelector("__time", false)
  val dimensionSelectors = fieldMappings.fieldMaps.keySet
    .map(d=> (d, cursor.makeDimensionSelector(d)))

  def read(): (DateTime, Map[String, Any]) = {
    cursor.isDone match {
      case true =>
        (new DateTime(0), null)
      case other =>
        val event = Map[String, Any]()
        val time = timeDimensionSelector.getValues()(0)
        for((d, ds)<-dimensionSelectors) {
          ds.accumalate(event)((event, v) => event += ((d, v)))
        }

        cursor.advance()
        (new DateTime(time), event)
    }
  }

  def close(): Unit = {
    leafReader.close()
  }
}
