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
        case CreateTable(_, _) =>
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

    // Not needed at the moment but got the feeling it could be useful
    def findNextStepWithException(actor: ActorRef): QueryPlanNode = {
      val excludedNode = findNodeWithWorker(actor)
      this match {
        case GetTable(_) =>
          if (resultTable == ActorRef.noSender && !this.equals(excludedNode)) this
          else EmptyNode()
        case Scan(input, _, _) =>
          if (resultTable == ActorRef.noSender) input.findNextStep()
          else EmptyNode()
        case DropTable(_) =>
          if (resultTable == ActorRef.noSender && !this.equals(excludedNode)) this
          else EmptyNode()
        case EmptyNode() => EmptyNode()
      }
    }

    def findNodeWithWorker(workerRef: ActorRef): QueryPlanNode = {
      // TODO: I dont think this behaves as I planned it to. Validate!
      if (assignedWorker == workerRef) {
        return this
      }

      this match {
        case GetTable(_) =>
          if (assignedWorker.equals(workerRef)) this
          else EmptyNode()
        case Scan(input, _, _) =>
          if (assignedWorker == workerRef) input.findNodeWithWorker(workerRef)
          else EmptyNode()
        case EmptyNode() => this
      }
    }

    def saveIntermediateResult(worker: ActorRef, intermediateResult: ActorRef): QueryPlanNode = {
      val node = findNodeWithWorker(worker)
      node.resultTable = intermediateResult
      this
    }

    def updateAssignedWorker(worker: ActorRef): QueryPlanNode = {
      assignedWorker = worker
      this
    }

    def handleNesting(input: QueryPlanNode): QueryPlanNode = {
      val nextStep = input.findNextStep()
      nextStep match {
        case EmptyNode() => this
        case _ => nextStep
      }
    }
  }
  case class GetTable(tableName: String) extends QueryPlanNode
  case class Scan(input: QueryPlanNode, columnName: String, predicate: String => Boolean) extends QueryPlanNode
  case class CreateTable(tableName: String, columnNames: Seq[String]) extends QueryPlanNode
  case class DropTable(tableName: String) extends QueryPlanNode
  case class InsertRow(table: QueryPlanNode, row: RowType) extends QueryPlanNode
  case class EmptyNode() extends QueryPlanNode

  // case class Join(left: QueryPlanNode, right: QueryPlanNode, ...) extends QueryPlanNode
  // case class Projection(input: QueryPlanNode, columns: Seq[String]) extends QueryPlanNode
}
