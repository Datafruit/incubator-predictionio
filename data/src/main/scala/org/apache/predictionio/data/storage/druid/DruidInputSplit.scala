
package org.apache.predictionio.data.storage.druid

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.InputSplit

/**
  */
class DruidInputSplit(private[druid] val paths: Array[Path]) extends InputSplit {

  override def getLength: Long = 0

  override def getLocations: Array[String] = paths.map(_.toString)

  override def write(out: DataOutput): Unit = {
  }

  override def readFields(in: DataInput): Unit = {

  }
}
