package nl.gideondk.raiku

object Main extends App {
  if (args.length == 0) {
    println("Run benchmark with 'benchmark:run short' or 'benchmark:run long' depending on the benchmark suite")
  } else {
    args.toList(0) match {
      case "short" =>
        println("Running short performance-suite")
        runShortTests

      case "long" =>
        println("Running long performance-suite")
        runLongTests
      case _ => println("Unknown test type, use 'short' or 'long' depending on the test suite to use.")
    }
  }

  def runShortTests = {
    (new ShortPerformanceTest {}).run
    DB.client disconnect

    DB.system.shutdown()
  }

  def runLongTests = {
    (new LongPerformanceTest {}).run
    DB.client disconnect

    DB.system.shutdown()
  }
}
