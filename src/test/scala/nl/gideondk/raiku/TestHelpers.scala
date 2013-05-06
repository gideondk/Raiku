package nl.gideondk.raiku

//import commands.RWObject
import mapreduce._
import scalaz._
import scala.concurrent.Await
import Scalaz._
import spray.json._
import org.specs2.mutable.Specification
import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.util.Random

object DB {
  implicit val system = ActorSystem("perf-bucket-system")
  val client = RaikuClient("localhost", 8087, 4)
}