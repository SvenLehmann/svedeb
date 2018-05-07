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
    val columnType = ColumnType(IndexedSeq("a", "b", "a", "c"))

    val indizesForA = columnType.filterByPredicate(x => x == "a")
    indizesForA shouldEqual Seq(0, 2)

    val indizesForC = columnType.filterByPredicate(x => x == "c")
    indizesForC shouldEqual Seq(3)
  }

  it should "filter by indizes" in {
    val columnType = ColumnType(IndexedSeq("a", "b", "a", "c"))

    val indizes = Seq(0, 1)
    val filteredColumn = columnType.filterByIndizes(indizes)
    filteredColumn shouldEqual ColumnType(IndexedSeq("a", "b"))
  }

}

