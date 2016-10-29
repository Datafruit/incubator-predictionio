
package org.apache.predictionio.data.storage.druid.reader

import java.lang.{Float => jFloat}

import com.google.common.primitives.Ints
import org.apache.lucene.index.LeafReader

/**
  */
trait FloatDimensionSelector extends DimensionSelector[Float] {
}

class FloatSingleDimensionSelector(cursor:LuceneCursor,
                                   leafReader: LeafReader,
                                   dimension: String)  extends FloatDimensionSelector {
  val docValues = leafReader.getNumericDocValues(dimension)

  override def getValues(): List[Float] = {
    if (null != docValues) {
      val value = docValues.get(cursor.getCurrentDoc)
      val fValue = jFloat.intBitsToFloat(Ints.checkedCast(value))
      return List(fValue)
    }
    return List()
  }

  override def accumalate[AccumalateType](accumalated: AccumalateType)
                                         (f: (AccumalateType, Float) => AccumalateType)
  : AccumalateType = {
    if (null != docValues) {
      val value = docValues.get(cursor.getCurrentDoc)
      val fValue = jFloat.intBitsToFloat(Ints.checkedCast(value))
      f(accumalated, fValue)
    } else {
      accumalated
    }
  }
}

class FloatMultiDimensionSelector(cursor:LuceneCursor,
                                  leafReader: LeafReader,
                                  dimension: String)  extends FloatDimensionSelector {
  val docValues = leafReader.getSortedNumericDocValues(dimension)

  override def getValues():  List[Float] = {
    if (null != docValues) {
      docValues.setDocument(cursor.getCurrentDoc)
    }
    return List()
  }

  override def accumalate[AccumalateType](accumalated: AccumalateType)
                                         (f: (AccumalateType, Float) => AccumalateType)
  : AccumalateType = {
    if (null != docValues) {
      docValues.setDocument(cursor.getCurrentDoc)
    }
    accumalated
  }}
