package de.hpi.svedeb

import akka.actor.{ActorRef, PoisonPill}
import akka.pattern.ask
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.queryPlan.{GetTable, QueryPlan, Scan}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ScanBenchmark extends AbstractBenchmark {

  override def setup(api: ActorRef, tableSize: Int, numberOfColumns: Int, partitionSize: Int, distinctValues: Int, tableRatio: Double): Unit = {
    Utils.createTable(api, "table1", Seq("columnA", "columnB"), tableSize, partitionSize, distinctValues)
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
    )).mapTo[Result]
    val result = Await.result(future, Duration.Inf)
    result.resultTable ! PoisonPill
  }

  override def tearDown(api: ActorRef): Unit = {
    Utils.dropTable(api, "table1")
  }

  override val name: String = "Scan"
}
