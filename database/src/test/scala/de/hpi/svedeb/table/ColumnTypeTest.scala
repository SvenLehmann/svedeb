package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractTest
import org.scalatest.Matchers._

class ColumnTypeTest extends AbstractTest {

  "A column type" should "append values" in {
    val columnType = ColumnType()
    var newColumn = columnType.append(1)
    newColumn.size() shouldEqual 1

    newColumn = newColumn.append(1)
    newColumn.size() shouldEqual 2
  }

  it should "filter by predicate" in {
    val columnType = ColumnType(1, 2, 1, 3)

    val indicesForA = columnType.filterByPredicate(_ == 1)
    indicesForA shouldEqual Seq(0, 2)

    val indicesForC = columnType.filterByPredicate(_ == 3)
    indicesForC shouldEqual Seq(3)
  }

  it should "filter by indices" in {
    val columnType = ColumnType(1, 2, 1, 3)

    val indices = Seq(0, 1)
    val filteredColumn = columnType.filterByIndices(indices)
    filteredColumn shouldEqual ColumnType(1, 2)
  }

  it should "filter by indices adding Nones" in {
    val columnType = ColumnType(1, 2, 1, 3)

    val indices = Seq(0, 1)
    val filteredColumn = columnType.filterByIndicesWithOptional(indices)
    filteredColumn shouldEqual OptionalColumnType(Some(1), Some(2), None, None)
  }

}

