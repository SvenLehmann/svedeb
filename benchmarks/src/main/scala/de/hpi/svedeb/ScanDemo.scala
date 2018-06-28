package de.hpi.svedeb

import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.ColumnType

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

object ScanDemo extends App {
  implicit val timeout: Timeout = Timeout(30 seconds)
  val tableName = "Table1"

  val api = SvedeB.start().api

  try {
    val data = Map(
      0 -> Map("columnA" -> ColumnType(3, 5, 10), "columnB" -> ColumnType(4, 9, 6)),
      1 -> Map("columnA" -> ColumnType(1, 7, 3), "columnB" -> ColumnType(8, 4, 3)),
      2 -> Map("columnA" -> ColumnType(6, 4, 9), "columnB" -> ColumnType(7, 2, 8))
    )

    val createTableFuture = api.ask(
      Query(
        QueryPlan(
          CreateTable(
            tableName,
            data,
            partitionSize = 3
          )
        )
      )
    )
    Await.result(createTableFuture, timeout.duration).asInstanceOf[Result]
    println("Table created")

    // Scan table
    val queryFuture = api.ask(
      Query(
        QueryPlan(
          Scan(
            GetTable(tableName),
            "columnA",
            _ == 3
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
