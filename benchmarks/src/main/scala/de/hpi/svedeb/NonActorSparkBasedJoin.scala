package de.hpi.svedeb

import akka.actor.ActorRef
import de.hpi.svedeb.api.API.Result
import de.hpi.svedeb.table.ColumnType
import org.apache.spark.sql.{Dataset, SparkSession}

object NonActorSparkBasedJoin extends AbstractBenchmark {

  case class Scheme(a: Int)
//  val columns = Seq("a", "b")
  val partitionSize = 1000

  private val spark = SparkSession.builder.appName("Spark Join").master("local[4]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  private var left: Dataset[Scheme] = _
  private var right: Dataset[Scheme] = _

  private def createDataset(size: Int): Dataset[Scheme] = {
    val r = new scala.util.Random(100)
    val seq = for (_ <- 0 until size) yield Scheme(r.nextInt(1000))

    import spark.implicits._
    seq.toDS()
  }

  override def setup(api: ActorRef, tableSize: Int): Unit = {
    left = createDataset(tableSize)
    right = createDataset(tableSize/10)
  }

  override def runBenchmark(api: ActorRef): Result = {
    val result = left.join(right, Seq("a", "a"))
    result.collect()

    Result(ActorRef.noSender)
  }

  override def tearDown(api: ActorRef): Unit = {
    spark.stop()
  }

  override val name: String = "NonActorSparkBasedJoin"
}
