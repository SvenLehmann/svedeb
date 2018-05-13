package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.API.Query
import de.hpi.svedeb.api.QueryPlanExecutor.Run
import de.hpi.svedeb.queryplan.QueryPlan.QueryPlanNode

object API {
  case class Query(queryPlan: QueryPlanNode)

  def props(tableManager: ActorRef): Props = Props(new API(tableManager))
}

class API(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = {
    case Query(queryPlan) => context.actorOf(QueryPlanExecutor.props(tableManager)) ! Run(queryPlan)
  }
}
