package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.queryPlan.{CreateTable, DropTable, QueryPlan}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.language.postfixOps

abstract class AbstractBenchmark {
  implicit val timeout: Timeout = Timeout(30 minutes)

  val name: String

  def setup(api: ActorRef, tableSize: Int): Unit

  def runBenchmark(api: ActorRef): Unit

  def tearDown(api: ActorRef): Unit

  protected def loadData(api: ActorRef, tableName: String, columns: Seq[String], rowCount: Int, partitionSize: Int): Unit = {
    val data = DataGenerator.generateData(columns, rowCount, partitionSize)
    val future = api.ask(Query(QueryPlan(CreateTable(tableName, data, partitionSize))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
  }

  protected def dropTable(api: ActorRef, tableName: String): Unit = {
    val future = api.ask(Query(QueryPlan(DropTable(tableName))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
  }
}
