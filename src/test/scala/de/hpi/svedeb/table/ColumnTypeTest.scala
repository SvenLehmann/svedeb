package de.hpi.svedeb.table

import de.hpi.svedeb.AbstractTest
import org.scalatest.Matchers._

class ColumnTypeTest extends AbstractTest {

  "A column type" should "append values" in {
    val columnType = ColumnType()
    var newColumn = columnType.append("SomeValue")
    newColumn.size() shouldEqual 1

    newColumn = newColumn.append("SomeOtherValue")
    newColumn.size() shouldEqual 2
  }

  it should "filter by predicate" in {
    val columnType = ColumnType("a", "b", "a", "c")

    val indicesForA = columnType.filterByPredicate(x => x == "a")
    indicesForA shouldEqual Seq(0, 2)

    val indicesForC = columnType.filterByPredicate(x => x == "c")
    indicesForC shouldEqual Seq(3)
  }

  it should "filter by indices" in {
    val columnType = ColumnType("a", "b", "a", "c")

    val indices = Seq(0, 1)
    val filteredColumn = columnType.filterByIndices(indices)
    filteredColumn shouldEqual ColumnType("a", "b")
  }

}

