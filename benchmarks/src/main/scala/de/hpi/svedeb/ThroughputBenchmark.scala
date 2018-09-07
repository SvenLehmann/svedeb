package de.hpi.svedeb
import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import de.hpi.svedeb.api.API.Query
import de.hpi.svedeb.queryPlan.{GetTable, QueryPlan, Scan}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.{Failure, Success}

class ThroughputBenchmark(numberOfQueries: Int) extends AbstractBenchmark {
//  val partitionSize = 1000

  override val name: String = "ThroughputBenchmark"

  override def setup(api: ActorRef, tableSize: Int, numberOfColumns: Int, partitionSize: Int, distinctValues: Int, tableRatio: Double): Unit = {
    Utils.createTable(api, "table1", Seq("columnA", "columnB"), tableSize, partitionSize, distinctValues)
  }

  class HelloActor extends Actor {
    def receive: PartialFunction[Any, Unit] = {
      case "hello" => println("hello back at you")
      case _       => println("huh?")
    }
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

    var counter = 0

    val future = Future {
      while (true) {
        singleQuery(api).onComplete {
          case Success(_) => counter = counter + 1
          case Failure(_) =>
        }
      }
    }

    Thread.sleep(10000)

    future.failed
    println(counter)
  }

  override def tearDown(api: ActorRef): Unit = {}
}
