package nl.gideondk.raiku

//import commands.RWObject

import org.scalatest.time.{ Millis, Seconds, Span }

import scala.concurrent._
import scala.concurrent.duration._
import akka.actor._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Suite, WordSpec }
import org.scalatest.matchers.ShouldMatchers
import spray.json._

object DB {
  implicit val system = ActorSystem("perf-bucket-system")
  val client = RaikuClient("localhost", 8087, 8)
}

case class Y(id: String, name: String, age: Int, groupId: String)

case class Z(id: String, name: String)

object TestModels extends DefaultJsonProtocol {
  implicit val yFormat = jsonFormat4(Y)

  implicit val yConverter = RaikuConverter.newConverter(
    reader = (v: RaikuRWValue) ⇒ yFormat.read(new String(v.data).asJson),
    writer = (o: Y) ⇒ RaikuRWValue(o.id, o.toJson.toString.getBytes, "application/json"),
    binIndexes = (o: Y) ⇒ Map("group_id" -> Set(o.groupId)),
    intIndexes = (o: Y) ⇒ Map("age" -> Set(o.age)))

  implicit val zConverter = RaikuConverter.newConverter(
    reader = (v: RaikuRWValue) ⇒ Z(v.key, new String(v.data)),
    writer = (o: Z) ⇒ RaikuRWValue(o.id, o.name.getBytes(), "application/json"))
}

abstract class RaikuSpec extends WordSpec with Suite with ShouldMatchers with ScalaFutures {
  implicit val timeout = 30 seconds
  implicit val defaultPatience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(200, Millis))

  val client = DB.client

}