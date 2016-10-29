
package org.apache.predictionio.data.storage.druid.format

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, Writable}
import org.apache.hadoop.mapreduce.InputSplit

/**
  */
class DruidInputSplit(var paths: Array[String]) extends InputSplit with Writable {
  var length = paths.length

  def this() = this(Array())

  override def getLength: Long = 0

  override def getLocations: Array[String] = paths.map(_.toString)

  override def write(out: DataOutput): Unit = {
    out.writeInt(length)
    for(i <- 0 to length) {
      Text.writeString(out, paths(0))
    }
  }

  override def readFields(in: DataInput): Unit = {
    length = in.readInt()
    paths = new Array(length)
    for(i <- 0 until length) {
      paths(i) = Text.readString(in)
    }
  }
}
