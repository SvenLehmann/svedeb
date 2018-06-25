package de.hpi.svedeb

import akka.actor.{ActorRef, ActorSystem}
import de.hpi.svedeb.api.API
import de.hpi.svedeb.management.TableManager

object SvedeB {
  def start(remoteAPIs: Seq[ActorRef], remoteTableManagers: Seq[ActorRef]): ActorRef = {
    val system = ActorSystem("SvedeB")
    val tableManager = system.actorOf(TableManager.props(remoteTableManagers))
    system.actorOf(API.props(tableManager, remoteAPIs))
  }
}
