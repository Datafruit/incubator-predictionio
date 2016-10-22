
package org.apache.predictionio.data.storage.druid

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.RecordReader

/**
  */
class DruidRecordReader(paths: Array[Path]) extends RecordReader[Long, Result] {

  override def next(key: Long, value: Result): Boolean = {
    false
  }

  override def getProgress: Float = 0

  override def getPos: Long = 0

  override def createKey(): Long = {
    0
  }

  override def close(): Unit = {

  }

  override def createValue(): Result = {
    new Result
  }
}

object DruidRecordReader {
  def apply(paths: Array[Path]) : DruidRecordReader = {
    new DruidRecordReader(paths)
  }
}