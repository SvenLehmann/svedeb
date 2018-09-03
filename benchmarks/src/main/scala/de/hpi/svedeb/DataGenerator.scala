package de.hpi.svedeb

import de.hpi.svedeb.table.ColumnType

object DataGenerator {

  def generateData(columnNames: Seq[String], rowCount: Int, partitionSize: Int, distinctValues: Int): Map[Int, Map[String, ColumnType]] = {
    val partitionCount = (rowCount / partitionSize.toDouble).ceil.toInt
    val r = new scala.util.Random(100)

    def generateColumn(rowCount: Int): Seq[Int] =
      for (_ <- 0 until rowCount) yield r.nextInt(distinctValues)

    def generatePartition(rowsPerPartition: Int): Map[String, ColumnType] = {
      val columnMap = columnNames.map(name => (name, generateColumn(rowsPerPartition))).toMap
      columnMap.mapValues(values => ColumnType(values.toIndexedSeq)).map(identity)
    }

    val rowsPerPartition =
      if (rowCount % partitionSize == 0) List.tabulate(partitionCount)(_ => partitionSize)
      else List.tabulate(partitionCount - 1)(_ => partitionSize) :+ rowCount % partitionSize

    val rowsPerPartitionMap = rowsPerPartition.zipWithIndex.map(_.swap).toMap
    rowsPerPartitionMap.mapValues(generatePartition).map(identity)
  }

}
