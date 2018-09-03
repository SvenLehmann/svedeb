package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.table.ColumnType

object NonActorScan extends AbstractBenchmark {

  val columns = Seq("a")
  val partitionSize = 10000

  private var input: Map[Int, Map[String, ColumnType]] = _

  override def setup(api: ActorRef, tableSize: Int): Unit = {
    input = DataGenerator.generateData(columns, tableSize, partitionSize, 100)
  }

  override def runBenchmark(api: ActorRef): Unit = {
    input.flatMap {
      case (_, partition) =>
        val column = partition("a")
        column.values.filter(_ < 55)
    }
  }

  override def tearDown(api: ActorRef): Unit = {}

  override val name: String = "NonActorScan"
}
