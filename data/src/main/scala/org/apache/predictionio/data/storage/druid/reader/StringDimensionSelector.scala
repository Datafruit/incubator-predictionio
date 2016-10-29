
package org.apache.predictionio.data.storage.druid.reader

import org.apache.lucene.index.LeafReader

/**
  */
trait StringDimensionSelector extends DimensionSelector[String] {
}

class StringSingleDimensionSelector(cursor: LuceneCursor,
                                    leafReader: LeafReader,
                                    dimension: String) extends StringDimensionSelector {
  val docValues = leafReader.getSortedDocValues(dimension)

  override def getValues(): List[String] = {
    if (null != docValues) {
      val v = docValues.get(cursor.getCurrentDoc).utf8ToString
      return List(v)
    }
    return List()
  }

  override def accumalate[AccumalateType](accumalated: AccumalateType)
                                         (f: (AccumalateType, String) => AccumalateType)
  : AccumalateType = {
    if (null != docValues) {
      val v = docValues.get(cursor.getCurrentDoc).utf8ToString
      f(accumalated, v)
    } else {
      accumalated
    }
  }
}

class StringMultiDimensionSelector(cursor: LuceneCursor,
                                   leafReader: LeafReader,
                                   dimension: String) extends StringDimensionSelector {
  val docValues = leafReader.getSortedSetDocValues(dimension)

  override def getValues(): List[String] = {
    if (null != docValues) {
      docValues.setDocument(cursor.getCurrentDoc)
    }
    return List()
  }

  override def accumalate[AccumalateType](accumalated: AccumalateType)
                                         (f: (AccumalateType, String) => AccumalateType)
  : AccumalateType = {
    if (null != docValues) {
      docValues.setDocument(cursor.getCurrentDoc)
    }
    accumalated
  }
}