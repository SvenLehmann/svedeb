package de.hpi.svedeb

import akka.pattern.ask
import akka.util.Timeout
import de.hpi.svedeb.ClusterNode.{ClusterIsUp, FetchAPI, FetchedAPI, IsClusterUp}
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

  val clusterNode = ClusterNode.start()

  // Wait for cluster start
  while (!Await.result(clusterNode.ask(IsClusterUp()) (5 seconds), 5 seconds)
    .asInstanceOf[ClusterIsUp].bool) {}

  // Fetch ActorRef for API
  val apiFuture = clusterNode.ask(FetchAPI()) (5 seconds)
  import scala.concurrent.Await
  val api = Await.result(apiFuture, 5 seconds).asInstanceOf[FetchedAPI].api

  // Create Table
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
    val leftTableData = Map(0 -> Map("leftColumnA" -> ColumnType(3), "leftColumnB" -> ColumnType(4)),
                            1 -> Map("leftColumnA" -> ColumnType(4), "leftColumnB" -> ColumnType(0)),
                            2 -> Map("leftColumnA" -> ColumnType(1), "leftColumnB" -> ColumnType(6)))
    createTable(leftTableName, leftTableData)

    val rightTableData = Map(0 -> Map("rightColumnA" -> ColumnType(1), "rightColumnB" -> ColumnType(4)),
                             1 -> Map("rightColumnA" -> ColumnType(2), "rightColumnB" -> ColumnType(0)),
                             2 -> Map("rightColumnA" -> ColumnType(3), "rightColumnB" -> ColumnType(6)))
    createTable(rightTableName, rightTableData)

    // Join tables
    val queryFuture = api.ask(
      Query(
        QueryPlan(
          HashJoin(
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

    val materializationFuture = api.ask(
      Materialize(
        resultMessage.resultTable
      )
    )
    val materializedResultMessage = Await.result(materializationFuture, timeout.duration)
      .asInstanceOf[MaterializedResult]

    println(materializedResultMessage.result)
  } finally {
    api ! Shutdown()
  }
}
