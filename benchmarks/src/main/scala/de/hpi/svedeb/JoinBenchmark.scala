package de.hpi.svedeb

import akka.actor.ActorRef
import akka.pattern.ask
import de.hpi.svedeb.api.API._
import de.hpi.svedeb.queryPlan._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps

object JoinBenchmark extends AbstractBenchmark {

  val partitionSize = 1000

  override def setup(api: ActorRef): Unit = {
    loadData(api, "table1", Seq("a1", "b1"), 20000, partitionSize)
    loadData(api, "table2", Seq("a2", "b2"), 20000, partitionSize)
  }

  override def runBenchmark(api: ActorRef): Result = {
    // Perform Join
    val future = api.ask(Query(
      QueryPlan(
        NestedLoopJoin(
          GetTable("table1"),
          GetTable("table2"),
          "a1",
          "a2",
          _ == _)
      )
    ))
    Await.result(future, Duration.Inf).asInstanceOf[Result]
  }

  override def tearDown(api: ActorRef): Unit = {}

  override val name: String = "NestedLoopJoin"
}
