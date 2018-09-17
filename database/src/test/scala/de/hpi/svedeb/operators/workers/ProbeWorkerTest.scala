package de.hpi.svedeb.operators.workers

import akka.actor.ActorRef
import akka.testkit.{TestActor, TestProbe}
import de.hpi.svedeb.AbstractActorTest
import de.hpi.svedeb.operators.helper.HashBucket.{ListValues, ListedValues}
import de.hpi.svedeb.operators.helper.HashBucketEntry
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
            ListedValues(Seq(HashBucketEntry(2, 0, 5), HashBucketEntry(2, 1, 5), HashBucketEntry(2, 2, 4)))
            , leftHashMap.ref)
            TestActor.KeepRunning
        }
    })

    rightHashMap.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot =
        msg match {
          case ListValues() => sender.tell(
            ListedValues(Seq(HashBucketEntry(2, 4, 4), HashBucketEntry(2, 5, 5), HashBucketEntry(2, 6, 7)))
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
      (HashBucketEntry(2,0,5),HashBucketEntry(2,5,5)),
      (HashBucketEntry(2,1,5),HashBucketEntry(2,5,5)),
      (HashBucketEntry(2,2,4),HashBucketEntry(2,4,4))
    ))
  }

}
