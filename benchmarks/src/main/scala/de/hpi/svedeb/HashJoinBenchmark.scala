package de.hpi.svedeb

import akka.actor.{ActorRef, PoisonPill}
import akka.pattern.ask
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.queryPlan.{GetTable, HashJoin, QueryPlan}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.postfixOps

object HashJoinBenchmark extends AbstractBenchmark {

  override def setup(api: ActorRef, tableSize: Int, numberOfColumns: Int, partitionSize: Int, distinctValues: Int, tableRatio: Double): Unit = {
    val potentialColumnNames = Seq("a", "b", "c", "d", "e")

    Utils.createTable(api, "table1", potentialColumnNames.take(numberOfColumns).map(_ + "1"), tableSize, partitionSize, distinctValues)
    Utils.createTable(api, "table2", potentialColumnNames.take(numberOfColumns).map(_ + "2"), (tableSize * tableRatio).toInt, partitionSize, distinctValues)
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
