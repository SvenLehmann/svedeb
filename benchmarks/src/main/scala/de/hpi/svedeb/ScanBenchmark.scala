package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.queryPlan.{GetTable, NestedLoopJoin, QueryPlan, Scan}
import akka.pattern.ask

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ScanBenchmark extends AbstractBenchmark() {
  val partitionSize = 10000

  override def setup(api: ActorRef): Unit = {
    loadData(api, "table1", Seq("columnA", "columnB"), 200000, partitionSize)
  }

  override def runBenchmark(api: ActorRef): Result = {
    // Perform Join
    val future = api.ask(Query(
      QueryPlan(
        Scan(
          GetTable("table1"),
          "columnA",
          _ < 55)
      )
    ))
    Await.result(future, Duration.Inf).asInstanceOf[Result]
  }

  override def tearDown(api: ActorRef): Unit = {}

  override val name: String = "Scan"
}
