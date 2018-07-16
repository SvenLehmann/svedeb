package de.hpi.svedeb

import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.ClusterNode.{ClusterIsUp, FetchAPI, FetchedAPI, IsClusterUp}
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.ColumnType

import scala.concurrent.duration._
import scala.language.postfixOps

object ScanDemo extends App {
  implicit val timeout: Timeout = Timeout(30 seconds)
  val tableName = "Table1"

  val clusterNode = ClusterNode.start()
  val apiFuture = clusterNode.ask(FetchAPI()) (5 seconds)
  import scala.concurrent.Await
  val api = Await.result(apiFuture, 5 seconds).asInstanceOf[FetchedAPI].api

  // Hacky way to wait for cluster start
  while (!Await.result(clusterNode.ask(IsClusterUp()) (5 seconds), 5 seconds).asInstanceOf[ClusterIsUp].bool) {
//    println("Waiting for cluster")
  }

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
    val createTableResult = Await.result(createTableFuture, timeout.duration).asInstanceOf[Result]
    println("Table created")

    val materializationCreateTableFuture = api.ask(
      Materialize(
        createTableResult.resultTable
      )
    )
    val materializedCreateTableResultMessage = Await.result(materializationCreateTableFuture, timeout.duration).asInstanceOf[MaterializedResult]

    println(materializedCreateTableResultMessage.result)

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
