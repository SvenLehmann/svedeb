package de.hpi.svedeb

import akka.actor.{ActorRef, ActorSystem}
import de.hpi.svedeb.api.API
import de.hpi.svedeb.management.TableManager

object SvedeB {
  case class SvedeBConnection(api: ActorRef, tableManager: ActorRef)

  def start(remoteAPIs: Seq[ActorRef] = Seq.empty, remoteTableManagers: Seq[ActorRef] = Seq.empty): SvedeBConnection = {
    val system = ActorSystem("SvedeB")
    val tableManager = system.actorOf(TableManager.props(remoteTableManagers))
    val api = system.actorOf(API.props(tableManager, remoteAPIs))
    SvedeBConnection(api, tableManager)
  }
}
