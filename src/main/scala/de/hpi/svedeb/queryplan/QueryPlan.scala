package de.hpi.svedeb.queryplan

import akka.actor.ActorRef
import de.hpi.svedeb.table.RowType

import scala.annotation.tailrec

object QueryPlan {
  abstract class QueryPlanNode(var leftInput: Option[QueryPlanNode],
                               var rightInput: Option[QueryPlanNode],
                               var assignedWorker: ActorRef = ActorRef.noSender,
                               var resultTable: ActorRef = ActorRef.noSender) {
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
                           nextStage: QueryPlanNode): QueryPlanNode = {
      findNodeAndUpdateWorker(nextStage, nextWorker)
      saveIntermediateResult(previousWorker, resultTable)
      this
    }

    /**
      * Saves a result table which was received from a worker.
      * @param worker the worker
      * @param intermediateResult the result
      * @return the updated query plan
      */
    def saveIntermediateResult(worker: ActorRef, intermediateResult: ActorRef): QueryPlanNode = {
      val optionalNode = findNodeWithWorker(worker)
      if (optionalNode.isEmpty) {
        throw new Exception("Could not find node for this worker")
      }

      optionalNode.get.saveIntermediateResult(intermediateResult)
      this
    }

    private def saveIntermediateResult(intermediateResult: ActorRef): QueryPlanNode = {
      resultTable = intermediateResult
      this
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
    def findNodeWithWorker(workerRef: ActorRef): Option[QueryPlanNode] = {
      @tailrec
      def iter(remainingNodes: Seq[Option[QueryPlanNode]]): Option[QueryPlanNode] = {
        remainingNodes match {
          case Nil => None
          case None :: tail => iter(tail)
          case Some(node) :: _ if node.assignedWorker == workerRef => Some(node)
          case Some(node) :: tail => iter(node.leftInput :: node.rightInput :: tail)
        }
      }

      iter(Seq(Some(this)))
    }

    /**
      * Updates assigned worker of node.
      * @param node node to be updated
      * @param worker the worker to be assigned
      * @return the updated query plan
      */
    def findNodeAndUpdateWorker(node: QueryPlanNode, worker: ActorRef): QueryPlanNode = {
      // Implementation detail: Assuming that node is mutable and changed in-place
      node.updateAssignedWorker(worker)
      this
    }

    private def updateAssignedWorker(worker: ActorRef) : QueryPlanNode = {
      assignedWorker = worker
      this
    }

    /**
      * Finds a node in the query plan by traversing it recursively.
      * @param searchNode the node to be searched
      * @return the
      */
    def findNode(searchNode: QueryPlanNode): Option[QueryPlanNode] = {
      @tailrec
      def iter(remainingNodes: Seq[Option[QueryPlanNode]]): Option[QueryPlanNode] = {
        remainingNodes match {
          case Nil => None
          case None :: tail => iter(tail)
          case Some(node) :: _ if node == searchNode => Some(node)
          case Some(node) :: tail => iter(node.leftInput :: node.rightInput :: tail)
        }
      }

      iter(Seq(Some(this)))
    }

    /**
      * Travers query plan recursively to find a node that can be executed next.
      * This node needs to fulfil two criteria:
      * - it must not be already executed
      * - its child nodes must have been executed
      *
      * @return a node if a matching node is found or else None
      */
    def findNextStage(): Option[QueryPlanNode] = {
      @tailrec
      def iter(remainingNodes: Seq[Option[QueryPlanNode]]): Option[QueryPlanNode] = {
        remainingNodes match {
          case Nil => None
          case None :: tail => iter(tail)
          case Some(node) :: _ if node.isExecuted => None
          case Some(node) :: _ if node.isValidNextStage => Some(node)
          case Some(node) :: tail => iter(node.leftInput :: node.rightInput :: tail)
        }
      }

      iter(Seq(Some(this)))
    }

    private def isValidNextStage: Boolean = {
      Seq(leftInput, rightInput).flatten.forall(node => node.isExecuted)
    }

    /**
      * We assume a node has not been executed if there is no result table assigned.
      * @return true if there is a result table else false
      */
    def isExecuted: Boolean = resultTable != ActorRef.noSender
  }

  case class GetTable(tableName: String) extends QueryPlanNode(None, None)
  case class Scan(input: QueryPlanNode, columnName: String, predicate: String => Boolean) extends QueryPlanNode(Some(input), None)
  case class CreateTable(tableName: String, columnNames: Seq[String], partitionSize: Int) extends QueryPlanNode(None, None)
  case class DropTable(tableName: String) extends QueryPlanNode(None, None)
  case class InsertRow(table: QueryPlanNode, row: RowType) extends QueryPlanNode(Some(table), None)
  case class Join(left: QueryPlanNode, right: QueryPlanNode) extends QueryPlanNode(Some(left), Some(right))
  // case class Projection(input: QueryPlanNode, columns: Seq[String]) extends QueryPlanNode
}
