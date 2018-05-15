package de.hpi.svedeb.queryplan

import akka.actor.ActorRef
import de.hpi.svedeb.table.RowType

object QueryPlan {
  abstract class QueryPlanNode(var assignedWorker: ActorRef = ActorRef.noSender, var resultTable: ActorRef = ActorRef.noSender) {
    /**
      * Checks whether this node has been executed.
      * @return returns None if it is no potential next stage (e.g. because it has already been executed
      */
    def findNextStage(): Option[QueryPlanNode] = {
      resultTable match {
        case ActorRef.noSender => Some(this)
        case _ => None
      }
    }

    def findNextStepWithException(actor: ActorRef): Option[QueryPlanNode] = {
      if (resultTable == ActorRef.noSender && !assignedWorker.equals(actor)) Some(this)
      else None
    }

    def findNodeWithWorker(workerRef: ActorRef): Option[QueryPlanNode] = {
      if (assignedWorker == workerRef) {
        Some(this)
      } else {
        this match {
          case Scan(input, _, _) => input.findNodeWithWorker(workerRef)
          case InsertRow(table, _) => table.findNodeWithWorker(workerRef)
          case _ => throw new Exception("Something went wrong")
        }
      }
    }

    def prepareNextStage(lastWorker: ActorRef,
                         resultTable: ActorRef,
                         nextWorker: ActorRef,
                         nextStep: QueryPlanNode): QueryPlanNode = {
      val foo = updateAssignedWorker(nextWorker, nextStep)
      val bar = saveIntermediateResult(lastWorker, resultTable)
      this
    }

    def saveIntermediateResult(worker: ActorRef, intermediateResult: ActorRef): Option[QueryPlanNode] = {
      val node = findNodeWithWorker(worker)
      if (node.isDefined && node.get == this) {
        resultTable = intermediateResult
      } else if (node.isDefined) {
        node.get.saveIntermediateResult(worker, intermediateResult)
      }
      Some(this)
    }

    def updateAssignedWorker(worker: ActorRef, node: QueryPlanNode): Option[QueryPlanNode] = {
      val foundNode = findNode(node)
      if (foundNode.isDefined) {
        foundNode.get.updateAssignedWorker(worker)
        Some(this)
      } else {
        None
      }
    }

    def updateAssignedWorker(worker: ActorRef) : Unit = {
      assignedWorker = worker
    }

    def findNode(node: QueryPlanNode): Option[QueryPlanNode] = {
      if (this == node) Some(this)
      else None
    }

    def handleNesting(input: QueryPlanNode): Option[QueryPlanNode] = {
      val nextStep = input.findNextStage()
      nextStep match {
        case None => Some(this)
        case _ => nextStep
      }
    }

    def handleNestingWithException(input: QueryPlanNode, actorRef: ActorRef): Option[QueryPlanNode] = {
      val nextStep = input.findNextStepWithException(actorRef)
      nextStep match {
        case None =>
          if (assignedWorker != actorRef) Some(this)
          else None
        case _ => nextStep
      }
    }
  }

  case class GetTable(tableName: String) extends QueryPlanNode
  case class Scan(input: QueryPlanNode, columnName: String, predicate: String => Boolean) extends QueryPlanNode {
    override def findNextStage(): Option[QueryPlanNode] = if (resultTable == ActorRef.noSender) handleNesting(input) else None
    override def findNextStepWithException(actor: ActorRef): Option[QueryPlanNode] = if (resultTable == ActorRef.noSender) handleNestingWithException(input, actor) else None
    override def findNode(node: QueryPlanNode): Option[QueryPlanNode] = if (this == node) Some(this) else input.findNode(node)
  }
  case class CreateTable(tableName: String, columnNames: Seq[String], partitionSize: Int) extends QueryPlanNode
  case class DropTable(tableName: String) extends QueryPlanNode
  case class InsertRow(table: QueryPlanNode, row: RowType) extends QueryPlanNode {
    override def findNextStage(): Option[QueryPlanNode] = if (resultTable == ActorRef.noSender) handleNesting(table) else None
    override def findNextStepWithException(actor: ActorRef): Option[QueryPlanNode] = if (resultTable == ActorRef.noSender) handleNestingWithException(table, actor) else None
    override def findNode(node: QueryPlanNode): Option[QueryPlanNode] = if (this == node) Some(this) else table.findNode(node)
  }

  // case class Join(left: QueryPlanNode, right: QueryPlanNode, ...) extends QueryPlanNode
  // case class Projection(input: QueryPlanNode, columns: Seq[String]) extends QueryPlanNode
}
