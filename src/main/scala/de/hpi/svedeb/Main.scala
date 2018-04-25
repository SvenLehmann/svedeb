package de.hpi.svedeb

import akka.actor.{Actor, ActorSystem, Props}
import de.hpi.svedeb.tableHierarchy.{AddLineMessage, Table}

// This class is mainly for testing at the moment, just to get something running
object Main {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("tableSystem")

    val table = system.actorOf(Props[Table], "tableA")
    table ! new AddLineMessage("blah")
  }
}
