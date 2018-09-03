package de.hpi.svedeb

import akka.actor.{ActorRef, PoisonPill}
import akka.pattern.ask
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.queryPlan.{GetTable, HashJoin, QueryPlan}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps

object HashJoinBenchmark extends AbstractBenchmark {

  val partitionSize = 10000

  override def setup(api: ActorRef, tableSize: Int): Unit = {
//    val partitionSize = Math.min(10000, tableSize/10)
//    Utils.createTable(api, "table1", Seq("a"), tableSize, partitionSize, tableSize/100)
    Utils.createTable(api, "table1", Seq("a"), tableSize, partitionSize, tableSize)
    Utils.createTable(api, "table2", Seq("b"), tableSize / 10, partitionSize, tableSize)
  }

  override def runBenchmark(api: ActorRef): Unit = {
    // Perform Join
    val future = api.ask(Query(
      QueryPlan(
        HashJoin(
          GetTable("table1"),
          GetTable("table2"),
          "a",
          "b",
          _ == _)
      )
    )).mapTo[Result]
    val result = Await.result(future, Duration.Inf)
    result.resultTable ! PoisonPill
  }

  override def tearDown(api: ActorRef): Unit = {
    Utils.dropTable(api, "table1")
    Utils.dropTable(api, "table2")
  }

  override val name: String = "HashJoin"
}
