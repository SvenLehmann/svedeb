package de.hpi.svedeb.queryplan

import akka.actor.ActorRef
import de.hpi.svedeb.table.RowType

object QueryPlan {
  abstract class QueryPlanNode(var assignedWorker: ActorRef = ActorRef.noSender, var resultTable: ActorRef = ActorRef.noSender) {
    def findNextStep(): QueryPlanNode = {
      this match {
        case GetTable(_) =>
          if (resultTable == ActorRef.noSender) this
          else EmptyNode()
        case Scan(input, _, _) =>
          if (resultTable == ActorRef.noSender) handleNesting(input)
          else EmptyNode()
        case CreateTable(_, _, _) =>
          if (resultTable == ActorRef.noSender) this
          else EmptyNode()
        case DropTable(_) =>
          if (resultTable == ActorRef.noSender) this
          else EmptyNode()
        case InsertRow(table, _) =>
          if (resultTable == ActorRef.noSender) handleNesting(table)
          else EmptyNode()
        case EmptyNode() => this
      }
    }

    def findNextStepWithException(actor: ActorRef): QueryPlanNode = {
//      val excludedNode = findNodeWithWorker(actor)
      this match {
        case GetTable(_) =>
          if (resultTable == ActorRef.noSender && !assignedWorker.equals(actor)) this
          else EmptyNode()
        case Scan(input, _, _) =>
          if (resultTable == ActorRef.noSender) handleNestingWithException(input, actor)
          else EmptyNode()
        case DropTable(_) =>
          if (resultTable == ActorRef.noSender && !assignedWorker.equals(actor)) this
          else EmptyNode()
        case CreateTable(_, _, _) =>
          if (resultTable == ActorRef.noSender && !assignedWorker.equals(actor)) this
          else EmptyNode()
        case InsertRow(table, _) =>
          if (resultTable == ActorRef.noSender) handleNestingWithException(table, actor)
          else EmptyNode()
        case EmptyNode() => EmptyNode()
      }
    }

    def findNodeWithWorker(workerRef: ActorRef): QueryPlanNode = {
      if (assignedWorker == workerRef) {
        return this
      }

      this match {
        case Scan(input, _, _) => input.findNodeWithWorker(workerRef)
        case InsertRow(table, _) => table.findNodeWithWorker(workerRef)
        case _ => throw new Exception("Smthg went wrong")
      }
    }

    def nextStage(lastWorker: ActorRef, resultTable: ActorRef, nextWorker: ActorRef, nextStep: QueryPlanNode): QueryPlanNode = {
      updateAssignedWorker(nextWorker, nextStep)
      saveIntermediateResult(lastWorker, resultTable)
      this
    }

    def saveIntermediateResult(worker: ActorRef, intermediateResult: ActorRef): QueryPlanNode = {
      val node = findNodeWithWorker(worker)
      if (node == this) {
        resultTable = intermediateResult
      } else {
        node.saveIntermediateResult(worker, intermediateResult)
      }
      this
    }

    def updateAssignedWorker(worker: ActorRef, node: QueryPlanNode): QueryPlanNode = {
      val foundNode: QueryPlanNode = findNode(node)
      foundNode.updateAssignedWorker(worker)
      this
    }

    def updateAssignedWorker(worker: ActorRef) : Unit = {
      assignedWorker = worker
    }

    def findNode(node: QueryPlanNode): QueryPlanNode = {
      this match {
        case GetTable(_) =>
          if (this == node) this
          else EmptyNode()
        case Scan(input, _, _) =>
          if (this == node) this
          else input.findNode(node)
        case CreateTable(_, _, _) =>
          if (this == node) this
          else EmptyNode()
        case DropTable(_) =>
          if (this == node) this
          else EmptyNode()
        case InsertRow(table, _) =>
          if (this == node) this
          else table.findNode(node)
        case EmptyNode() => this
      }
    }

    def handleNesting(input: QueryPlanNode): QueryPlanNode = {
      val nextStep = input.findNextStep()
      nextStep match {
        case EmptyNode() => this
        case _ => nextStep
      }
    }

    def handleNestingWithException(input: QueryPlanNode, actorRef: ActorRef): QueryPlanNode = {
      val nextStep = input.findNextStepWithException(actorRef)
      nextStep match {
        case EmptyNode() =>
          if (assignedWorker != actorRef) this
          else EmptyNode()
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
