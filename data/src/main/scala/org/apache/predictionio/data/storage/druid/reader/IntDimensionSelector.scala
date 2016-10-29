
package org.apache.predictionio.data.storage.druid.reader

import com.google.common.primitives.Ints
import org.apache.lucene.index.LeafReader

/**
  */
trait IntDimensionSelector extends DimensionSelector[Int] {
}

class IntSingleDimensionSelector(cursor:LuceneCursor,
                                 leafReader: LeafReader,
                                 dimension: String) extends IntDimensionSelector {
  val docValues = leafReader.getNumericDocValues(dimension)

  override def getValues(): List[Int] = {
    if (null != docValues) {
      val value = docValues.get(cursor.getCurrentDoc)
      return List(Ints.checkedCast(value))
    }

    return List()
  }

  override def accumalate[AccumalateType](accumalated: AccumalateType)
                                         (f: (AccumalateType, Int) => AccumalateType)
  : AccumalateType = {
    if (null != docValues) {
      val value = docValues.get(cursor.getCurrentDoc)
      f(accumalated, Ints.checkedCast(value))
    } else {
      accumalated
    }
  }
}

class IntMultiDimensionSelector(cursor:LuceneCursor,
                                leafReader: LeafReader,
                                dimension: String)extends IntDimensionSelector {
  val docValues = leafReader.getSortedNumericDocValues(dimension)

  override def getValues():  List[Int] = {
    if (null != docValues) {
      docValues.setDocument(cursor.getCurrentDoc)
    }
    return List()
  }

  override def accumalate[AccumalateType](accumalated: AccumalateType)
                                         (f: (AccumalateType, Int) => AccumalateType)
  : AccumalateType = {
    if (null != docValues) {
      docValues.setDocument(cursor.getCurrentDoc)
    }
    accumalated
  }
}
