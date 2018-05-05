package de.hpi.svedeb.table

case class ColumnType(values: List[String] = List.empty[String]) {
  def append(value: String): ColumnType = {
    ColumnType(this.values :+ value)
  }

  def size(): Int = {
    values.size
  }
}
