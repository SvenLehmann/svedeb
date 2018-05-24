package de.hpi.svedeb

/*
 * TODO: Unused for now as we have to find out how to use TypedActors correctly.
 */
sealed class DataType
object DataType {
  implicit object StringWitness extends DataType
  implicit object IntWitness extends DataType
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