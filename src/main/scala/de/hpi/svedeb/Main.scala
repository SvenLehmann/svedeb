package de.hpi.svedeb

import akka.actor.{ActorSystem, PoisonPill, Props}
import de.hpi.svedeb.table.Table
import de.hpi.svedeb.table.Table.AddRowMessage

// This class is mainly for testing at the moment, just to get something running
object Main {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem("tableSystem")

    val table = system.actorOf(Props[Table], "tableA")
    table ! AddRowMessage(List("blah"))


  }
}
