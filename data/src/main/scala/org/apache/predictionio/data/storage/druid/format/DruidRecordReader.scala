
package org.apache.predictionio.data.storage.druid.format

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.lucene.store.Directory
import org.apache.predictionio.data.storage.druid.Result
import org.apache.predictionio.data.storage.druid.directory.bytebuffer.ByteBufferDirectory
import org.apache.predictionio.data.storage.druid.directory.hdfs.ZipHdfsDirectory
import org.apache.predictionio.data.storage.druid.reader.{DirectoryReader, FieldMappings}
import org.joda.time.DateTime

/**
  */
class DruidRecordReader(paths: Array[String], fs: FileSystem)
  extends RecordReader[DateTime, Result] {
  var directories: Array[Directory] = null
  var fieldMappingses: Array[FieldMappings] = null
  var reader: DirectoryReader = null

  var key: DateTime = null
  var value: Result = null

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    directories = paths
      .map(path => new ZipHdfsDirectory(fs, new Path(path)))
      .map(new ByteBufferDirectory(_, null))
    fieldMappingses = paths.map(path => FieldMappings(fs, new Path(path)))
    reader = new DirectoryReader(fieldMappingses(0), directories(0))
  }

  override def getProgress: Float = 0

  override def nextKeyValue(): Boolean = {
    val (time, event) = reader.read()
    if(null == event) {
      value = null
      false
    } else {
      key = time
      value = new Result(event)
      true
    }
  }

  override def getCurrentValue: Result = value


  override def getCurrentKey: DateTime = key

  override def close(): Unit = {
    reader.close()
  }
}

object DruidRecordReader {
  def apply(fs: FileSystem, paths: Array[String]): DruidRecordReader = {
    new DruidRecordReader(paths, fs)
  }
}