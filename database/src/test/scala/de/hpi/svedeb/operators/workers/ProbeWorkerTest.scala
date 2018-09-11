package de.hpi.svedeb.operators.workers

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.helper.PartitionedHashTableActor.{ListValues, ListedValues}
import de.hpi.svedeb.operators.helper.PartitionedHashTableEntry
import de.hpi.svedeb.operators.workers.ProbeWorker.{FetchIndices, JoinedIndices, ProbeJob, ProbeResult}
import org.scalatest.Matchers._

class ProbeWorkerTest extends AbstractActorTest("ProbeWorkerTest") {

  "A probe worker" should "join matching tuples" in {

    val leftHashMap = TestProbe()
    val rightHashMap = TestProbe()

    leftHashMap.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
        msg match {
          case ListValues() => sender.tell(
            ListedValues(Seq(PartitionedHashTableEntry(2, 0, 5), PartitionedHashTableEntry(2, 1, 5), PartitionedHashTableEntry(2, 2, 4)))
            , leftHashMap.ref)
            TestActor.KeepRunning
        }
    })

    rightHashMap.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
        msg match {
          case ListValues() => sender.tell(
            ListedValues(Seq(PartitionedHashTableEntry(2, 4, 4), PartitionedHashTableEntry(2, 5, 5), PartitionedHashTableEntry(2, 6, 7)))
            , rightHashMap.ref)
            TestActor.KeepRunning
        }
    })

    val worker = system.actorOf(ProbeWorker.props(4, leftHashMap.ref, rightHashMap.ref, _ == _))
    worker ! ProbeJob()
    val result = expectMsgType[ProbeResult]

    val expectedResult = ProbeResult(4)
    result shouldEqual expectedResult

    worker ! FetchIndices()
    val joinedValues = expectMsgType[JoinedIndices]
    joinedValues shouldEqual JoinedIndices(Seq(
      (PartitionedHashTableEntry(2,0,5),PartitionedHashTableEntry(2,5,5)),
      (PartitionedHashTableEntry(2,1,5),PartitionedHashTableEntry(2,5,5)),
      (PartitionedHashTableEntry(2,2,4),PartitionedHashTableEntry(2,4,4))
    ))
  }

}
