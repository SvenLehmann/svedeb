package de.hpi.svedeb

import akka.actor.ActorRef
import akka.pattern.ask
import de.hpi.svedeb.api.API.Query
import de.hpi.svedeb.queryPlan.{GetTable, HashJoin, NestedLoopJoin, QueryPlan}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps

object HashJoinBenchmark extends AbstractBenchmark {

  val partitionSize = 10000

  override def setup(api: ActorRef, tableSize: Int): Unit = {
    Utils.createTable(api, "table1", Seq("a1"), tableSize, partitionSize)
    Utils.createTable(api, "table2", Seq("a2"), tableSize / 10, partitionSize)
  }

  override def runBenchmark(api: ActorRef): Unit = {
    // Perform Join
    val future = api.ask(Query(
      QueryPlan(
        HashJoin(
          GetTable("table1"),
          GetTable("table2"),
          "a1",
          "a2",
          _ == _)
      )
    ))
    Await.result(future, Duration.Inf)
  }

  override def tearDown(api: ActorRef): Unit = {
    Utils.dropTable(api, "table1")
    Utils.dropTable(api, "table2")
  }

  override val name: String = "HashJoin"
}
