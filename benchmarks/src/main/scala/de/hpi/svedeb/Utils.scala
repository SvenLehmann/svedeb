package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.api.API.{Query, Result}
import de.hpi.svedeb.queryPlan.{CreateTable, DropTable, QueryPlan}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.language.postfixOps

object Utils {
  implicit val timeout: Timeout = Timeout(30 minutes)

  def time[R](description: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"Elapsed time in $description: ${(t1 - t0)/1000000.0}ms")
    result
  }

  def createTable(api: ActorRef, tableName: String, columns: Seq[String], rowCount: Int, partitionSize: Int, distinctValues: Int): Unit = {
    val data = DataGenerator.generateData(columns, rowCount, partitionSize, distinctValues)
    val future = api.ask(Query(QueryPlan(CreateTable(tableName, data, partitionSize))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
  }

  def dropTable(api: ActorRef, tableName: String): Unit = {
    val future = api.ask(Query(QueryPlan(DropTable(tableName))))
    Await.result(future, timeout.duration).asInstanceOf[Result]
  }
}
