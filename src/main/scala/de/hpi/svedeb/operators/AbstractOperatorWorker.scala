package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging, ActorRef}

object AbstractOperatorWorker {
  case class QueryResult(resultTable: ActorRef)
}

abstract class AbstractOperatorWorker(tableManager: ActorRef) extends Actor with ActorLogging {}
