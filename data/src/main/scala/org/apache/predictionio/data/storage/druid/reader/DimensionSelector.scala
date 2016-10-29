
package org.apache.predictionio.data.storage.druid.reader

/**
  */
trait DimensionSelector[T] {
  def getValues(): List[T]

  def accumalate[AccumalateType](accumalated: AccumalateType)
                                (f: (AccumalateType, T) => AccumalateType): AccumalateType
}
