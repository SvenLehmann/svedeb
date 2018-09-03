package de.hpi.svedeb

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import de.hpi.svedeb.ClusterNode.{ClusterIsUp, FetchAPI, FetchedAPI, IsClusterUp}
import de.hpi.svedeb.api.API
import de.hpi.svedeb.management.TableManager

object ClusterNode extends App {
  start()

  def start(): ActorRef = {
    val system = ActorSystem("SvedeB")
    system.actorOf(ClusterNode.props(), "clusterNode")
  }

  case class FetchAPI()
  case class FetchedAPI(api: ActorRef)
  case class IsClusterUp()
  case class ClusterIsUp(bool: Boolean)

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
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case FetchAPI() => sender() ! FetchedAPI(api)
      // Hacky way to find out whether the cluster was correctly initialized... used in Demo
    case IsClusterUp() => if (cluster.state.members.nonEmpty) {
      log.info("Cluster is up")
      sender() ! ClusterIsUp(true)
    } else {
      sender() ! ClusterIsUp(false)
    }
    case m: MemberEvent => log.error(s"$m")
  }

}
