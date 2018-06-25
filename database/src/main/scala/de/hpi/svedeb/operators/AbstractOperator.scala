package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging, ActorRef}

object AbstractOperator {
  case class Execute()
  case class QueryResult(resultTable: ActorRef)
}

trait AbstractOperator extends Actor with ActorLogging
