package de.hpi.svedeb

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import de.hpi.svedeb.api.API
import de.hpi.svedeb.management.TableManager

object ClusterNode {
  def start(remoteAPIs: Seq[ActorRef] = Seq.empty, remoteTableManagers: Seq[ActorRef] = Seq.empty): Unit = {
    val system = ActorSystem("SvedeB")
    system.actorOf(ClusterNode.props(), "clusterNode")
  }

  def props(): Props = Props(new ClusterNode())
}

class ClusterNode extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  val tableManager: ActorRef = context.actorOf(TableManager.props(), "tableManager")
  val api: ActorRef = context.actorOf(API.props(tableManager), "api")

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case _: MemberEvent => // ignore
  }

}
