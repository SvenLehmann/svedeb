package de.hpi.svedeb

object Main extends App {

  if (args.length == 0) {
    ClusterNode.start()
  } else {
    args(0) match {
      case "join" =>
        BenchmarkRunner
      case _ =>
        println("Undefined arg, can only understand 'join'")
    }
  }
}
