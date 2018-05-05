package de.hpi.svedeb.operators

import akka.actor.{Actor, ActorLogging, Props}
import de.hpi.svedeb.operators.TableWorker.{CreateTable, DropTable}

object TableWorker {
  case class CreateTable(name: String, columnNames: List[String])
  case class DropTable(name: String)

  case class TableCreated()
  case class TableDropped()

  def props(): Props = Props(new TableWorker())
}

class TableWorker extends Actor with ActorLogging {
  override def receive: Receive = {
    case CreateTable(name, columnNames) => ???
    case DropTable(name) => ???
  }
}