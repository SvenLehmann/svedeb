import akka.remote.testkit.MultiNodeSpec
import akka.testkit.ImplicitSender
import akka.actor.{Actor, Props}
import de.hpi.svedeb.STMultiNodeSpec

object MultiNodeSample {
  class Ponger extends Actor {
    def receive: Receive = {
      case "ping" => sender() ! "pong"
    }
  }
}

class MultiNodeSample extends MultiNodeSpec(MultiNodeSampleConfig)
  with STMultiNodeSpec with ImplicitSender {

  import MultiNodeSampleConfig._
  import MultiNodeSample._

  def initialParticipants: Int = roles.size

  "A MultiNodeSample" should "wait for all nodes to enter a barrier" in {
    enterBarrier("startup")
  }

  it should "send to and receive from a remote node" in {
    runOn(node1) {
      enterBarrier("deployed")
      val ponger = system.actorSelection(node(node2) / "user" / "ponger")
      ponger ! "ping"
      import scala.concurrent.duration._
      expectMsg(10.seconds, "pong")
    }

    runOn(node2) {
      system.actorOf(Props[Ponger], "ponger")
      enterBarrier("deployed")
    }
    enterBarrier("finished")
  }
}