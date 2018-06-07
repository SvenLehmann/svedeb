package de.hpi.svedeb

object NonActorNestedLoopJoin extends App {

  val columns = Seq("a", "b")
  val rowCount = 20000
  val partitionSize = 1000

  val left = DataGenerator.generateData(columns, rowCount, partitionSize)
  val right = DataGenerator.generateData(columns, rowCount, partitionSize)

  def join(): Seq[(Int, Int)] = {
    val result = left.flatMap {
      case (_, leftPartition) =>
        right.map {
          case (_, rightPartition) =>
            val leftValues = leftPartition("a")
            val rightValues = rightPartition("a")
            for {
              l <- leftValues.values
              r <- rightValues.values
              if l == r
            } yield (l, r)
        }
    }

    val flattened = result.flatten
    flattened.toSeq
  }

  val result = Utils.time("NonActorJoin", join())
}
