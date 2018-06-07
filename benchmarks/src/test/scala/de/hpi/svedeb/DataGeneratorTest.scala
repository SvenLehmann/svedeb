package de.hpi.svedeb

import org.scalatest.Matchers._

class DataGeneratorTest extends org.scalatest.FlatSpec {

  "DataGenerator" should "generate data" in {
    val data = DataGenerator.generateData(Seq("a", "b"), 11, 4)
    println(data)
    data.size shouldEqual 3
    data(0).size shouldEqual 2
    data(0).apply("a").size() shouldEqual 4
    data(0).apply("b").size() shouldEqual 4
    data(1).size shouldEqual 2
    data(1).apply("a").size() shouldEqual 4
    data(1).apply("b").size() shouldEqual 4
    data(2).size shouldEqual 2
    data(2).apply("a").size() shouldEqual 3
    data(2).apply("b").size() shouldEqual 3
  }

  it should "generate simple partition" in {
    val data = DataGenerator.generateData(Seq("a"), 3, 3)
    println(data)
    data.size shouldEqual 1
    data(0).size shouldEqual 1
    data(0).apply("a").size() shouldEqual 3
  }

  it should "generate data (2)" in {
    val data = DataGenerator.generateData(Seq("a"), 20, 5)
    println(data)
    data.size shouldEqual 4
    data(0).size shouldEqual 1
    data(0).apply("a").size() shouldEqual 5
    data(1).size shouldEqual 1
    data(1).apply("a").size() shouldEqual 5
    data(2).size shouldEqual 1
    data(2).apply("a").size() shouldEqual 5
    data(3).size shouldEqual 1
    data(3).apply("a").size() shouldEqual 5
  }

}
