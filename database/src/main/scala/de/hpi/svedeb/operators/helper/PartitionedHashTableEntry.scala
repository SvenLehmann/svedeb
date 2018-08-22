package de.hpi.svedeb.operators.helper

import de.hpi.svedeb.utils.Utils.ValueType

case class PartitionedHashTableEntry(partitionId: Int, rowId: Int, value: ValueType)