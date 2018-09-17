package de.hpi.svedeb.operators.helper

import de.hpi.svedeb.utils.Utils.ValueType

case class HashBucketEntry(partitionId: Int, rowId: Int, value: ValueType)