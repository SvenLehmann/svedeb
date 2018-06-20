package de.hpi.svedeb

import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.ColumnType

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object JoinDemo extends App {
  implicit val timeout: Timeout = Timeout(30 seconds)
  val leftTableName = "Table1"
  val rightTableName = "Table2"

  val api = SvedeB.start()

  private def createTable(name: String, data: Map[Int, Map[String, ColumnType]]): Unit = {
    val createTableFuture = api.ask(
      Query(
        QueryPlan(
          CreateTable(
            name,
            data,
            partitionSize = 3
          )
        )
      )
    )
    Await.result(createTableFuture, timeout.duration).asInstanceOf[Result]
    println("Table created")
  }

  try {
    val leftTableData = Map(0 -> Map("leftColumnA" -> ColumnType(3, 4, 1), "leftColumnB" -> ColumnType(4, 9, 6)))
    createTable(leftTableName, leftTableData)

    val rightTableData = Map(0 -> Map("rightColumnA" -> ColumnType(1, 2, 3), "rightColumnB" -> ColumnType(4, 9, 6)))
    createTable(rightTableName, rightTableData)

    // Join tables
    val queryFuture = api.ask(
      Query(
        QueryPlan(
          NestedLoopJoin(
            GetTable(leftTableName),
            GetTable(rightTableName),
            "leftColumnA",
            "rightColumnA",
            _ == _
          )
        )
      )
    )
    val resultMessage = Await.result(queryFuture, timeout.duration).asInstanceOf[Result]

    println("Result received")

    val materializationFuture = api.ask(
      Materialize(
        resultMessage.resultTable
      )
    )
    val materializedResultMessage = Await.result(materializationFuture, timeout.duration).asInstanceOf[MaterializedResult]

    println(materializedResultMessage.result)
  } finally {
    api ! Shutdown()
  }


}
