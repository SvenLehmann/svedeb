package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging, ActorRef}
import de.hpi.svedeb.table.ColumnType

object AbstractOperatorWorker {
  // Results in column orientation
  case class QueryResult(resultTable: ActorRef)
}

abstract class AbstractOperatorWorker(tableManager: ActorRef) extends Actor with ActorLogging {}
