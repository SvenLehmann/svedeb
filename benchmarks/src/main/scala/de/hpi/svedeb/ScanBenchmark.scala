package de.hpi.svedeb

import akka.actor.ActorRef
import akka.pattern.ask
import de.hpi.svedeb.api.API.Query
import de.hpi.svedeb.queryPlan.{GetTable, QueryPlan, Scan}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ScanBenchmark extends AbstractBenchmark {
  val partitionSize = 10000

  override def setup(api: ActorRef, tableSize: Int): Unit = {
    Utils.createTable(api, "table1", Seq("columnA", "columnB"), tableSize, partitionSize)
  }

  override def runBenchmark(api: ActorRef): Unit = {
    // Perform Scan
    val future = api.ask(Query(
      QueryPlan(
        Scan(
          GetTable("table1"),
          "columnA",
          _ < 55)
      )
    ))
    Await.result(future, Duration.Inf)
  }

  override def tearDown(api: ActorRef): Unit = {
    Utils.dropTable(api, "table1")
  }

  override val name: String = "Scan"
}
