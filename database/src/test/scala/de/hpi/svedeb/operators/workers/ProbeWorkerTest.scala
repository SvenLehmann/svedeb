package de.hpi.svedeb.operators.workers

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.helper.PartitionedHashTableActor.{ListValues, ListedValues}
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.ProbeWorker.{ProbeJob, ProbeResult}

class ProbeWorkerTest extends AbstractActorTest("ProbeWorkerTest") {

  "A probe worker" should "join matching tuples" in {

    val leftHashMap = TestProbe()
    val rightHashMap = TestProbe()

//    leftHashMap.setAutoPilot(new TestActor.AutoPilot {
//      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
//        msg match {
//          case ListValues => testActor.tell(ListedValues(), sender); TestActor.KeepRunning
//        }
//    })

    leftHashMap.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListValues => testActor.tell(
        ListedValues(Seq(PartitionedHashTableEntry(2, 0, 5), PartitionedHashTableEntry(2, 1, 5), PartitionedHashTableEntry(2, 2, 4)))
        , sender)
        TestActor.KeepRunning
    })

    rightHashMap.setAutoPilot((sender: ActorRef, msg: Any) => msg match {
      case ListValues => testActor.tell(
        ListedValues(Seq(PartitionedHashTableEntry(2, 4, 4), PartitionedHashTableEntry(2, 5, 5), PartitionedHashTableEntry(2, 6, 7)))
        , sender)
        TestActor.KeepRunning
    })

    val worker = system.actorOf(ProbeWorker.props(2, leftHashMap.ref, rightHashMap.ref, _ == _))
    worker ! ProbeJob()
    val result = expectMsgType[ProbeResult]

    val expectedResult =
  }

}
