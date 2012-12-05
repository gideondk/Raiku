package nl.gideondk.raiku

import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import org.specs2.execute.{ Success, Result }
import org.specs2.specification.{ Fragments, Example }

abstract class BenchmarkSpec extends Specification {
  sequential
  xonly

  def timed(desc: String, n: Int)(benchmark: ⇒ Unit): Result = {
    val t = System.currentTimeMillis
    benchmark
    val d = System.currentTimeMillis - t

    println(desc+":\n*** number of ops/s: "+n / (d / 1000.0)+"\n")
    Success()
  }

}