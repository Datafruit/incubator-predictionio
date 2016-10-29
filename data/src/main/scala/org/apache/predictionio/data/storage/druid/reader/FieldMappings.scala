
package org.apache.predictionio.data.storage.druid.reader

import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, StreamInput}

import scala.collection.mutable.Map

/**
  */
case class Field(name: String, `type`: String, hasMultipleValues: Boolean)

case class DimensionSchema()

class FieldMappings(val fieldMaps: Map[String, Field]) {
  def getField(key: String): Option[Field] = {
    fieldMaps.get(key)
  }
}

object FieldMappings {
  val FILE_NAME: String = ".mapping"

  def apply(fs: FileSystem, path: Path): FieldMappings = {
    val stream = new ZipInputStream(fs.open(path))
    var tmpEntry:ZipEntry = null
    while (null != (tmpEntry = stream.getNextEntry) && tmpEntry.getName != FILE_NAME){

    }

    val j = parse(new StreamInput(stream))
    implicit val formats = DefaultFormats
    val fields = j.extract[Array[Field]]

    val fieldMaps = Map[String, Field]()
    for (field <- fields) {
      fieldMaps.put(field.name, field)
    }
    new FieldMappings(fieldMaps)
  }
}

