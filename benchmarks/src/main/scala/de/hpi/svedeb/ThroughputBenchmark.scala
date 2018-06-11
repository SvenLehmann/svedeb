package de.hpi.svedeb
import akka.actor.ActorRef
import akka.pattern.ask
import de.hpi.svedeb.api.API
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.queryPlan.{GetTable, QueryPlan, Scan}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ThroughputBenchmark(numberOfQueries: Int) extends AbstractBenchmark {
  val partitionSize = 1000

  override val name: String = "ThroughputBenchmark"

  override def setup(api: ActorRef, tableSize: Int): Unit = {
    loadData(api, "table1", Seq("columnA", "columnB"), tableSize, partitionSize)
  }

  def singleQuery(api: ActorRef): API.Result = {
    // Perform Scan
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

  override def runBenchmark(api: ActorRef): API.Result = {
    (1 to numberOfQueries).par.foreach(_ => singleQuery(api))
    Result(ActorRef.noSender)
  }

  override def tearDown(api: ActorRef): Unit = {}
}
