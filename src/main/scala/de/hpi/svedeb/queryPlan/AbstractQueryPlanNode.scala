package de.hpi.svedeb.queryPlan

import akka.actor.ActorRef

abstract class AbstractQueryPlanNode(var leftInput: Option[AbstractQueryPlanNode] = None,
                                     var rightInput: Option[AbstractQueryPlanNode] = None,
                                     var assignedWorker: Option[ActorRef] = None,
                                     var resultTable: Option[ActorRef] = None) {

  def saveIntermediateResult(intermediateResult: ActorRef): AbstractQueryPlanNode = {
    resultTable = Some(intermediateResult)
    this
  }

  def updateAssignedWorker(worker: ActorRef) : AbstractQueryPlanNode = {
    assignedWorker = Some(worker)
    this
  }

  def isValidNextStage: Boolean = {
    Seq(leftInput, rightInput).flatten.forall(node => node.isExecuted)
  }

  /**
    * We assume a node has not been executed if there is no result table assigned.
    * @return true if there is a result table else false
    */
  def isExecuted: Boolean = resultTable.isDefined
}