package de.hpi.svedeb.queryplan

import akka.actor.ActorRef

abstract class AbstractQueryPlanNode(var leftInput: Option[AbstractQueryPlanNode],
                                     var rightInput: Option[AbstractQueryPlanNode],
                                     var assignedWorker: ActorRef = ActorRef.noSender,
                                     var resultTable: ActorRef = ActorRef.noSender) {

  def saveIntermediateResult(intermediateResult: ActorRef): AbstractQueryPlanNode = {
    resultTable = intermediateResult
    this
  }

  def updateAssignedWorker(worker: ActorRef) : AbstractQueryPlanNode = {
    assignedWorker = worker
    this
  }

  def isValidNextStage: Boolean = {
    Seq(leftInput, rightInput).flatten.forall(node => node.isExecuted)
  }

  /**
    * We assume a node has not been executed if there is no result table assigned.
    * @return true if there is a result table else false
    */
  def isExecuted: Boolean = resultTable != ActorRef.noSender
}