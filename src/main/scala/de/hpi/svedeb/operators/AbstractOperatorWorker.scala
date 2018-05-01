package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging}

object AbstractOperatorWorker {
  // Results in column orientation
  case class QueryResult(results: List[List[String]])
}

abstract class AbstractOperatorWorker extends Actor with ActorLogging {}
