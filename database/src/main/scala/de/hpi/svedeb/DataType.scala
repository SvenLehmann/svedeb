package de.hpi.svedeb

/*
 * TODO: Implement and use if you want to support DataTypes other than String.
 * Hint: Things get ugly when generics are used to extensively across actors.
 */
sealed class DataType[T]
object DataType {
  implicit object StringWitness extends DataType[String]
  implicit object IntWitness extends DataType[Int]
}
