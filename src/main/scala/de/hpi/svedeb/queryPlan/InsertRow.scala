package de.hpi.svedeb.queryPlan

import de.hpi.svedeb.table.RowType

case class InsertRow(table: AbstractQueryPlanNode, row: RowType) extends AbstractQueryPlanNode(Some(table), None)
