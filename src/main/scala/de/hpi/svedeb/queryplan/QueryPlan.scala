package de.hpi.svedeb.queryplan

import akka.actor.ActorRef
import de.hpi.svedeb.table.RowType

object QueryPlan {
  abstract class QueryPlanNode(var assignedWorker: ActorRef = ActorRef.noSender, var resultTable: ActorRef = ActorRef.noSender) {
    def findNextStep(): Option[QueryPlanNode] = {
      this match {
        case GetTable(_) =>
          if (resultTable == ActorRef.noSender) Some(this)
          else None
        case Scan(input, _, _) =>
          if (resultTable == ActorRef.noSender) handleNesting(input)
          else None
        case CreateTable(_, _, _) =>
          if (resultTable == ActorRef.noSender) Some(this)
          else None
        case DropTable(_) =>
          if (resultTable == ActorRef.noSender) Some(this)
          else None
        case InsertRow(table, _) =>
          if (resultTable == ActorRef.noSender) handleNesting(table)
          else None
        case EmptyNode() => Some(this)
      }
    }

    def findNextStepWithException(actor: ActorRef): Option[QueryPlanNode] = {
      this match {
        case GetTable(_) =>
          if (resultTable == ActorRef.noSender && !assignedWorker.equals(actor)) Some(this)
          else None
        case Scan(input, _, _) =>
          if (resultTable == ActorRef.noSender) handleNestingWithException(input, actor)
          else None
        case DropTable(_) =>
          if (resultTable == ActorRef.noSender && !assignedWorker.equals(actor)) Some(this)
          else None
        case CreateTable(_, _, _) =>
          if (resultTable == ActorRef.noSender && !assignedWorker.equals(actor)) Some(this)
          else None
        case EmptyNode() => None
          if (resultTable == ActorRef.noSender && !assignedWorker.equals(actor)) Some(this)
          else None
        case InsertRow(table, _) =>
          if (resultTable == ActorRef.noSender) handleNestingWithException(table, actor)
          else None
      }
    }

    def findNodeWithWorker(workerRef: ActorRef): Option[QueryPlanNode] = {
      if (assignedWorker == workerRef) {
        return Some(this)
      }

      this match {
        case Scan(input, _, _) => input.findNodeWithWorker(workerRef)
        case InsertRow(table, _) => table.findNodeWithWorker(workerRef)
        case _ => throw new Exception("Smthg went wrong")
      }
    }

    def nextStage(lastWorker: ActorRef, resultTable: ActorRef, nextWorker: ActorRef, nextStep: QueryPlanNode): Option[QueryPlanNode] = {
      updateAssignedWorker(nextWorker, nextStep)
      saveIntermediateResult(lastWorker, resultTable)
      Some(this)
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
      this match {
        case GetTable(_) =>
          if (this == node) Some(this)
          else None
        case Scan(input, _, _) =>
          if (this == node) Some(this)
          else input.findNode(node)
        case CreateTable(_, _, _) =>
          if (this == node) Some(this)
          else None
        case DropTable(_) =>
          if (this == node) Some(this)
          else None
        case InsertRow(table, _) =>
          if (this == node) Some(this)
          else table.findNode(node)
        case EmptyNode() => Some(this)
      }
    }

    def handleNesting(input: QueryPlanNode): Option[QueryPlanNode] = {
      val nextStep = input.findNextStep()
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
  case class Scan(input: QueryPlanNode, columnName: String, predicate: String => Boolean) extends QueryPlanNode
  case class CreateTable(tableName: String, columnNames: Seq[String], partitionSize: Int) extends QueryPlanNode
  case class DropTable(tableName: String) extends QueryPlanNode
  case class InsertRow(table: QueryPlanNode, row: RowType) extends QueryPlanNode
  case class EmptyNode() extends QueryPlanNode

  // case class Join(left: QueryPlanNode, right: QueryPlanNode, ...) extends QueryPlanNode
  // case class Projection(input: QueryPlanNode, columns: Seq[String]) extends QueryPlanNode
}
