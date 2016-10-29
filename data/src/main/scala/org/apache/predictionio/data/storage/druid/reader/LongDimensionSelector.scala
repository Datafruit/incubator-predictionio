
package org.apache.predictionio.data.storage.druid.reader

import org.apache.lucene.index.LeafReader

/**
  */
trait LongDimensionSelector extends DimensionSelector[Long] {
}

class LongSingleDimensionSelector(cursor:LuceneCursor,
                                  leafReader: LeafReader,
                                  dimension: String) extends LongDimensionSelector {
  val docValues = leafReader.getNumericDocValues(dimension)

  override def getValues(): List[Long] = {
    if (null != docValues) {
      val value = docValues.get(cursor.getCurrentDoc)
      return List(value)
    }

    return List()
  }

  override def accumalate[AccumalateType](accumalated: AccumalateType)
                                         (f: (AccumalateType, Long) => AccumalateType)
  : AccumalateType = {
    if (null != docValues) {
      val value = docValues.get(cursor.getCurrentDoc)
      f(accumalated, value)
    } else {
      accumalated
    }
  }}


class LongMultiDimensionSelector(cursor:LuceneCursor,
                                 leafReader: LeafReader,
                                 dimension: String) extends LongDimensionSelector {
  val docValues = leafReader.getSortedNumericDocValues(dimension)

  override def getValues(): List[Long] = {
    if (null != docValues) {
      docValues.setDocument(cursor.getCurrentDoc)
    }
    return List()
  }

  override def accumalate[AccumalateType](accumalated: AccumalateType)
                                         (f: (AccumalateType, Long) => AccumalateType)
  : AccumalateType = {
    if (null != docValues) {
      docValues.setDocument(cursor.getCurrentDoc)
    }
    accumalated
  }}
