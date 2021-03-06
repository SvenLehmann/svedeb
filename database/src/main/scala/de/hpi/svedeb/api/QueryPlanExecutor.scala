package de.hpi.svedeb.api

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import de.hpi.svedeb.api.QueryPlanExecutor.{QueryFinished, QueryPlanExecutorState, Run}
import de.hpi.svedeb.operators.AbstractOperator.{Execute, QueryResult}
import de.hpi.svedeb.operators._
import de.hpi.svedeb.queryPlan._
import de.hpi.svedeb.table.{ColumnType, RowType}

object QueryPlanExecutor {
  case class Run(queryId: Int, queryPlan: QueryPlan)

  case class QueryFinished(queryId: Int, resultTable: ActorRef)

  private case class QueryPlanExecutorState(sender: ActorRef,
                                            queryPlan: Option[QueryPlan],
                                            queryId: Option[Int]) {
    def storeQueryId(queryId: Int): QueryPlanExecutorState = {
      QueryPlanExecutorState(sender, queryPlan, Some(queryId))
    }
    def storeSender(sender: ActorRef): QueryPlanExecutorState = {
      QueryPlanExecutorState(sender, queryPlan, queryId)
    }

    def assignWorker(queryPlan: QueryPlan,
                     worker: ActorRef,
                     node: AbstractQueryPlanNode): QueryPlanExecutorState = {
      val newQueryPlan = queryPlan.updateWorker(node, worker)
      QueryPlanExecutorState(sender, Some(newQueryPlan), queryId)
    }

    def nextStage(queryPlan: QueryPlan,
                  nextStage: AbstractQueryPlanNode,
                  nextWorker: ActorRef): QueryPlanExecutorState = {
      val newQueryPlan = queryPlan.updateWorker(nextStage, nextWorker)
      QueryPlanExecutorState(sender, Some(newQueryPlan), queryId)
    }
  }

  def props(tableManager: ActorRef): Props = Props(new QueryPlanExecutor(tableManager))
}

class QueryPlanExecutor(tableManager: ActorRef) extends Actor with ActorLogging {
  override def receive: Receive = active(QueryPlanExecutorState(ActorRef.noSender, None, None))

  private def nodeToOperatorActor(node: AbstractQueryPlanNode): ActorRef = {
    node match {
      case GetTable(tableName: String) =>
        context.actorOf(GetTableOperator.props(tableManager, tableName))
      case CreateTable(tableName: String, data: Map[Int, Map[String, ColumnType]], partitionSize: Int) =>
        context.actorOf(CreateTableOperator.props(tableManager, tableName, data, partitionSize))
      case DropTable(tableName: String) =>
        context.actorOf(DropTableOperator.props(tableManager, tableName))
      case Scan(_, columnName: String, predicate) =>
        context.actorOf(ScanOperator.props(node.leftInput.get.resultTable.get, columnName, predicate))
      case NestedLoopJoin(_, _, leftColumn, rightColumn, predicate) =>
        if (node.leftInput.isEmpty || node.rightInput.isEmpty) {
          throw new Exception("Join Input Tables do not exist. Should not happen. Bug in QueryPlan")
        }
        context.actorOf(NestedLoopJoinOperator.props(
          node.leftInput.get.resultTable.get,
          node.rightInput.get.resultTable.get,
          leftColumn,
          rightColumn,
          predicate
        ))
      case HashJoin(_, _, leftColumn, rightColumn, predicate) =>
        if (node.leftInput.isEmpty || node.rightInput.isEmpty) {
          throw new Exception("Join Input Tables do not exist. Should not happen. Bug in QueryPlan")
        }
        context.actorOf(HashJoinOperator.props(
          node.leftInput.get.resultTable.get,
          node.rightInput.get.resultTable.get,
          leftColumn,
          rightColumn,
          predicate
        ), "HashJoinOperator")
      case InsertRow(_, row: RowType) =>
        context.actorOf(InsertRowOperator.props(node.leftInput.get.resultTable.get, row))
      case _ => throw new Exception("Unknown node type, cannot build operator")
    }
  }

  private def handleQuery(state: QueryPlanExecutorState, queryId: Int, queryPlan: QueryPlan): Unit = {
    log.debug("Building initial operator")
    val firstStage = queryPlan.findNextStage().get
    val operator = nodeToOperatorActor(firstStage)

    val newState = state.storeQueryId(queryId).storeSender(sender()).assignWorker(queryPlan, operator, firstStage)
    context.become(active(newState))

    operator ! Execute()
  }

  private def handleQueryResult(state: QueryPlanExecutorState, resultTable: ActorRef): Unit = {
    log.debug("Handling query result")
    if (state.queryPlan.isEmpty) {
      throw new Exception("No query plan to execute")
    }

    val queryPlan = state.queryPlan.get
    queryPlan.saveIntermediateResult(sender(), resultTable)
    val nextStage = queryPlan.findNextStage()

    if (nextStage.isEmpty) {
//      TODO: Add test that verifies deletion of operators
      queryPlan.cleanUpOperators()
      state.sender ! QueryFinished(state.queryId.get, resultTable)
    } else {
      val operator = nodeToOperatorActor(nextStage.get)

      val newState = state.nextStage(state.queryPlan.get, nextStage.get, operator)
      context.become(active(newState))

      operator ! Execute()
    }
  }

  private def active(state: QueryPlanExecutorState): Receive = {
    case Run(queryId, queryPlan) => handleQuery(state, queryId, queryPlan)
    case QueryResult(resultTable) => handleQueryResult(state, resultTable)
    case m => throw new Exception(s"Message not understood: $m")
  }
}
