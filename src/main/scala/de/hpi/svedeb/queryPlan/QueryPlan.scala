package de.hpi.svedeb.queryPlan

import akka.actor.{ActorRef, PoisonPill}

import scala.annotation.tailrec

case class QueryPlan(root: AbstractQueryPlanNode) {
  /**
    * Kills operators by sending PoisonPills to each operator.
    * It preserves the root operator to keep the final result of the query.
    */
  def cleanUpOperators(): Unit = {
    @tailrec
    def iter(remainingNodes: Seq[Option[AbstractQueryPlanNode]]): Option[AbstractQueryPlanNode] = {
      remainingNodes match {
        case Nil => None
        case None :: tail => iter(tail)
        case Some(node) :: tail =>
          if (node.assignedWorker.isDefined) {
            node.assignedWorker.get ! PoisonPill
          }
          iter(node.leftInput :: node.rightInput :: tail)
      }
    }

    // Skip root node to not destroy the final result
    iter(Seq(root.leftInput, root.rightInput))
  }

  /**
    * Utility method to save next worker and previous result in one function.
    *
    * @param previousWorker previous worker
    * @param resultTable previous result
    * @param nextWorker next worker
    * @param nextStage next stage to be executed
    * @return
    */
  def advanceToNextStage(previousWorker: ActorRef,
                         resultTable: ActorRef,
                         nextWorker: ActorRef,
                         nextStage: AbstractQueryPlanNode): QueryPlan = {
    updateWorker(nextStage, nextWorker)
    saveIntermediateResult(previousWorker, resultTable)
    this
  }

  /**
    * Updates assigned worker of node.
    * @param node node to be updated
    * @param worker the worker to be assigned
    * @return the updated query plan
    */
  def updateWorker(node: AbstractQueryPlanNode, worker: ActorRef): QueryPlan = {
    // Implementation detail: Assuming that node is mutable and changed in-place
    node.updateAssignedWorker(worker)
    this
  }

  /**
    * Saves a result table which was received from a worker.
    * @param worker the worker
    * @param intermediateResult the result
    * @return the updated query plan
    */
  def saveIntermediateResult(worker: ActorRef, intermediateResult: ActorRef): QueryPlan = {
    val optionalNode = findNodeWithWorker(worker)
    if (optionalNode.isEmpty) {
      throw new Exception("Could not find node for this worker")
    }

    optionalNode.get.saveIntermediateResult(intermediateResult)
    this
  }

  /**
    * Traverses query plan recursively to find a node that can be executed next.
    * This node needs to fulfil two criteria:
    * - it must not be already executed
    * - its child nodes must have been executed
    *
    * @return a node if a matching node is found or else None
    */
  def findNextStage(): Option[AbstractQueryPlanNode] = {
    @tailrec
    def iter(remainingNodes: Seq[Option[AbstractQueryPlanNode]]): Option[AbstractQueryPlanNode] = {
      remainingNodes match {
        case Nil => None
        case None :: tail => iter(tail)
        case Some(node) :: _ if node.isExecuted => None
        case Some(node) :: _ if node.isValidNextStage => Some(node)
        case Some(node) :: tail => iter(node.leftInput :: node.rightInput :: tail)
      }
    }

    iter(Seq(Some(root)))
  }

  /**
    * Traverse query plan recursively to find the node that the worker was assigned to.
    *
    * The inner function builds up a list of nodes that need to be checked.
    * By that the Scala Optimizer can transform the recursive structure into a loop-styled structure,
    * which helps avoiding huge call stacks at runtime.
    * @param workerRef the worker to be looked for
    * @return if successful, a node that holds the worker, else None
    */
  def findNodeWithWorker(workerRef: ActorRef): Option[AbstractQueryPlanNode] = {
    @tailrec
    def iter(remainingNodes: Seq[Option[AbstractQueryPlanNode]]): Option[AbstractQueryPlanNode] = {
      remainingNodes match {
        case Nil => None
        case None :: tail => iter(tail)
        case Some(node) :: _ if node.assignedWorker.isDefined && node.assignedWorker.get == workerRef => Some(node)
        case Some(node) :: tail => iter(node.leftInput :: node.rightInput :: tail)
      }
    }

    iter(Seq(Some(root)))
  }
}
