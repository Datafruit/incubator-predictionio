
package org.apache.predictionio.data.storage.druid.reader

import org.apache.lucene.index.LeafReader
import org.apache.lucene.search.{IndexSearcher, MatchAllDocsQuery}
import org.apache.predictionio.data.storage.druid.directory.hdfs.DocListCollector

/**
  */
class LuceneCursor(fieldMappings: FieldMappings, leafReader: LeafReader) {
  private val searcher = new IndexSearcher(leafReader)
  private val docListCollector = new DocListCollector

  searcher.search(new MatchAllDocsQuery, docListCollector)

  private val docList = docListCollector.getDocList
  private val maxDocOffset = docList.size - 1

  private var done = docList.isEmpty
  private var offset = 0

  def makeDimensionSelector(dimension: String): DimensionSelector[_ <: Any] = {
    val field = fieldMappings.getField(dimension)
    field match {
      case Some(Field(name, ftype, hasMultipleValues)) =>
        ftype match {
          case "string" =>
            makeStringDimensionSelector(name, hasMultipleValues)
          case "float" =>
            makeFloatDimensionSelector(name, hasMultipleValues)
          case "int" =>
            makeIntDimensionSelector(name, hasMultipleValues)
          case "long" =>
            makeLongDimensionSelector(name, hasMultipleValues)
          case other =>
            null
        }
      case other =>
        null
    }
  }

  def makeStringDimensionSelector(dimension: String, hasMultipleValues: Boolean)
  : StringDimensionSelector = {
    hasMultipleValues match {
      case true =>
        new StringMultiDimensionSelector(this, leafReader, dimension)
      case false =>
        new StringSingleDimensionSelector(this, leafReader, dimension)
    }
  }

  def makeIntDimensionSelector(dimension: String, hasMultipleValues: Boolean)
  : IntDimensionSelector = {
    hasMultipleValues match {
      case true =>
        new IntMultiDimensionSelector(this, leafReader, dimension)
      case false =>
        new IntSingleDimensionSelector(this, leafReader, dimension)
    }
  }

  def makeLongDimensionSelector(dimension: String, hasMultipleValues: Boolean)
  : LongDimensionSelector = {
    hasMultipleValues match {
      case true =>
        new LongMultiDimensionSelector(this, leafReader, dimension)
      case false =>
        new LongSingleDimensionSelector(this, leafReader, dimension)
    }
  }

  def makeFloatDimensionSelector(dimension: String, hasMultipleValues: Boolean)
  : FloatDimensionSelector = {
    hasMultipleValues match {
      case true =>
        new FloatMultiDimensionSelector(this, leafReader, dimension)
      case false =>
        new FloatSingleDimensionSelector(this, leafReader, dimension)
    }
  }

  def isDone:Boolean = done

  def advance(): Unit = {
    if (maxDocOffset > offset) offset += 1
    else done = true
  }

  def getCurrentDoc: Int = docList.get(offset)
}
