package de.hpi.svedeb.operators

import akka.actor.Props
import de.hpi.svedeb.operators.AbstractOperatorWorker.QueryResult
import de.hpi.svedeb.operators.ScanWorker.Scan

object ScanWorker {
  case class Scan(table: String)

  def props(): Props = Props(new ScanWorker())
}

class ScanWorker() extends AbstractOperatorWorker {
  override def receive: Receive = {
    case Scan(table) => sender() ! scan(table)
  }

  private def scan(table: String): QueryResult = {
    // Get Table Actor

    // Get Columns

    // Scan

    // Build result

    ???
  }
}
