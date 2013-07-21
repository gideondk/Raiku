package nl.gideondk.raiku

//import commands.RWObject
import mapreduce._
import scalaz._
import scala.concurrent.Await
import Scalaz._
import spray.json._
import org.specs2.mutable.Specification
import akka.actor.ActorSystem
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Random

object DB {
  implicit val system = ActorSystem("perf-bucket-system")
  val client = RaikuClient("localhost", 8087, 4)
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

trait RaikuSpec extends Specification {
  implicit val timeout = Duration(30, duration.SECONDS)
  val client = DB.client

}