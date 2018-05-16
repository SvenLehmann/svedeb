package de.hpi.svedeb.queryplan

import akka.actor.ActorRef
import de.hpi.svedeb.table.RowType

import scala.annotation.tailrec

object QueryPlan {
  abstract class QueryPlanNode(var leftInput: Option[QueryPlanNode],
                               var rightInput: Option[QueryPlanNode],
                               var assignedWorker: ActorRef = ActorRef.noSender,
                               var resultTable: ActorRef = ActorRef.noSender) {
    def saveIntermediateResult(intermediateResult: ActorRef): QueryPlanNode = {
      resultTable = intermediateResult
      this
    }

    def updateAssignedWorker(nextStep: QueryPlanNode, nextWorker: ActorRef): QueryPlanNode = {
      val optionalNode = findNode(nextStep)
      if (optionalNode.isEmpty) throw new Exception("Did not find node")

      optionalNode.get.updateAssignedWorker(nextWorker)
      this
    }

    def isExecuted: Boolean = {
      resultTable match {
        case ActorRef.noSender => false
        case _ => true
      }
    }

    // TODO: tailrec
    def findNodeWithWorker(workerRef: ActorRef): Option[QueryPlanNode] = {
      @tailrec
      def iter(l: Seq[QueryPlanNode]): Option[QueryPlanNode] = {
        l match {
          case Nil => None
          case node :: ls if node.assignedWorker == workerRef => Some(node)
          case node :: ls if node.leftInput.isDefined && node.rightInput.isDefined =>
            iter(node.leftInput.get :: node.rightInput.get :: ls)
          case node :: ls if node.leftInput.isDefined => iter(node.leftInput.get :: ls)
          case node :: ls if node.rightInput.isDefined => iter(node.rightInput.get :: ls)
          case _ => None
        }
      }

      iter(Seq(this))
    }

    def advanceToNextStep(lastWorker: ActorRef,
                          resultTable: ActorRef,
                          nextWorker: ActorRef,
                          nextStep: QueryPlanNode): QueryPlanNode = {
      findNodeAndUpdateWorker(nextWorker, nextStep)
      saveIntermediateResult(lastWorker, resultTable)
      this
    }

    def saveIntermediateResult(worker: ActorRef, intermediateResult: ActorRef): QueryPlanNode = {
      val optionalNode = findNodeWithWorker(worker)
      if (optionalNode.isEmpty) {
        throw new Exception("Could not find node for this worker")
      }

      optionalNode.get.saveIntermediateResult(intermediateResult)
      this
    }

    def findNodeAndUpdateWorker(worker: ActorRef, node: QueryPlanNode): QueryPlanNode = {
      val foundNode = findNode(node)
      if (foundNode.isEmpty) {
        throw new Exception("Cannot update non-existing node")
      }
      foundNode.get.updateAssignedWorker(worker)
      this
    }

    def updateAssignedWorker(worker: ActorRef) : QueryPlanNode = {
      assignedWorker = worker
      this
    }

    def findNextStep(): Option[QueryPlanNode] = {
      @tailrec
      def iter(l: Seq[QueryPlanNode]): Option[QueryPlanNode] = {
        l match {
          case Nil => None
          case node :: _ if node.isExecuted => None
          case node :: ls if node.leftInput.isDefined && node.rightInput.isDefined =>
            if (node.leftInput.get.isExecuted && node.rightInput.get.isExecuted) Some(node)
            else if (!node.leftInput.get.isExecuted && !node.rightInput.get.isExecuted) {
              iter(node.leftInput.get :: node.rightInput.get :: ls)
            } else if (!node.leftInput.get.isExecuted) {
              iter(node.leftInput.get :: ls)
            } else {
              iter(node.rightInput.get :: ls)
            }
          case node :: ls if node.leftInput.isDefined =>
            if (node.leftInput.get.isExecuted) Some(node)
            else iter(node.leftInput.get :: ls)
          case node :: ls if node.rightInput.isDefined =>
            if (node.rightInput.get.isExecuted) Some(node)
            else iter(node.rightInput.get :: ls)
          case (node: Scan) :: ls =>
            if (node.input.isExecuted) Some(node)
            else iter(node.input :: ls)
          case node :: _ => Some(node)
        }
      }
      iter(Seq(this))
    }


    def findNode(searchNode: QueryPlanNode): Option[QueryPlanNode] = {
      @tailrec
      def iter(l: Seq[QueryPlanNode]): Option[QueryPlanNode] = {
        l match {
          case Nil => None
          case node :: _ if node == searchNode => Some(node)
          case node :: ls if node.leftInput.isDefined && node.rightInput.isDefined =>
            iter(node.leftInput.get :: node.rightInput.get :: ls)
          case node :: ls if node.leftInput.isDefined => iter(node.leftInput.get :: ls)
          case node :: ls if node.rightInput.isDefined => iter(node.rightInput.get :: ls)
          case _ => None
        }
      }
      iter(Seq(this))
    }
  }

  case class GetTable(tableName: String) extends QueryPlanNode(None, None)
  case class Scan(input: QueryPlanNode, columnName: String, predicate: String => Boolean) extends QueryPlanNode(Some(input), None)
  case class CreateTable(tableName: String, columnNames: Seq[String], partitionSize: Int) extends QueryPlanNode(None, None)
  case class DropTable(tableName: String) extends QueryPlanNode(None, None)
  case class InsertRow(table: QueryPlanNode, row: RowType) extends QueryPlanNode(Some(table), None)
  case class Join(left: QueryPlanNode, right: QueryPlanNode) extends QueryPlanNode(Some(left), Some(right))
  // case class Projection(input: QueryPlanNode, columns: Seq[String]) extends QueryPlanNode
}
