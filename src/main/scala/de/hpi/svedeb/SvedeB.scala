package de.hpi.svedeb

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.SvedeB.{GetAPI, ReturnedAPI, Shutdown}
import de.hpi.svedeb.api.API
import de.hpi.svedeb.management.TableManager

object SvedeB {

  case class Shutdown()
  case class GetAPI()

  case class ReturnedAPI(api: ActorRef)

  def props(): Props = Props(new SvedeB())
}

class SvedeB extends Actor with ActorLogging {

  private val tableManager = context.actorOf(TableManager.props())
  private val api = context.actorOf(API.props(tableManager))

  override def receive: Receive = {
    case Shutdown() => ???
    case GetAPI() => sender() ! ReturnedAPI(api)
  }
}
