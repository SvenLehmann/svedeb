package de.hpi.svedeb

import scala.language.implicitConversions

/*
 * TODO: Unused for now as we have to find out how to use TypedActors correctly.
 */

trait DataType[T]
object DataTypeImplicits {
  implicit class StringWitness(value: String) extends DataType[String]
  implicit class IntWitness(value: Int) extends DataType[Int]

//  implicit def int2IntWitness(x: Int): DataType = IntWitness(x)
//  implicit def string2StringWitness(x: String): DataType = StringWitness(x)
}



//sealed class CellValueType[T]
//object CellValueType {
//  implicit object StringWitness extends CellValueType[String]
//  implicit object DoubleWitness extends CellValueType[Double]
//  implicit object BigDecimalWitness extends CellValueType[BigDecimal]
//  implicit object IntWitness extends CellValueType[Int]
//  implicit object LongWitness extends CellValueType[Long]
//  implicit object BooleanWitness extends CellValueType[Boolean]
//  implicit object DateWitness extends CellValueType[Date]
//  implicit object DateTimeWitness extends CellValueType[DateTime]
//  implicit object LocalDateWitness extends CellValueType[LocalDate]
//  implicit object CalendarWitness extends CellValueType[Calendar]
//  implicit object JLocalDateWitness extends CellValueType[JLocalDate]
//  implicit object JLocalDateTimeWitness extends CellValueType[JLocalDateTime]
//  implicit object HyperLinkUrlWitness extends CellValueType[HyperLinkUrl]
//}