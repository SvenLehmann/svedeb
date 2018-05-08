package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging, ActorRef}

object AbstractOperatorWorker {
  case class Execute()

  case class QueryResult(resultTable: ActorRef)
}

abstract class AbstractOperatorWorker extends Actor with ActorLogging {}
