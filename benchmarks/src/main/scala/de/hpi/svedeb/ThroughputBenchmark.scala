package de.hpi.svedeb
import akka.actor.ActorRef
import akka.pattern.ask
import de.hpi.svedeb.api.API
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.queryPlan.{GetTable, QueryPlan, Scan}

import scala.concurrent.Future

class ThroughputBenchmark(numberOfQueries: Int) extends AbstractBenchmark {
  val partitionSize = 1000

  override val name: String = "ThroughputBenchmark"

  override def setup(api: ActorRef, tableSize: Int): Unit = {
    loadData(api, "table1", Seq("columnA", "columnB"), tableSize, partitionSize)
  }

  def singleQuery(api: ActorRef): Future[Any] = {
    // Perform Scan
    api.ask(Query(
      QueryPlan(
        Scan(
          GetTable("table1"),
          "columnA",
          _ < 55)
      )
    ))
  }

  override def runBenchmark(api: ActorRef): Unit = {
    val futures = (1 to numberOfQueries).par.map(_ => singleQuery(api))
  }

  override def tearDown(api: ActorRef): Unit = {}
}
