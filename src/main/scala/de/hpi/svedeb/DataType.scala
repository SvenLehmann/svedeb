package de.hpi.svedeb


/*
 * TODO: Unused for now as we have to find out how to use TypedActors correctly.
 */
sealed class DataType[T]
object DataType {
  implicit object StringWitness extends DataType[String]
  implicit object IntWitness extends DataType[Int]
}
