package nl.gideondk.raiku

import commands.RWObject
import mapreduce._

import scalaz._
import Scalaz._
import spray.json._

import org.specs2.mutable.Specification
import akka.actor.ActorSystem

class MapReduceSpec extends Specification with DefaultJsonProtocol {
  import nl.gideondk.raiku.mapreduce.MapReduceJsonProtocol._

  implicit val system = ActorSystem("bucket-system")
  val client = RaikuClient("localhost", 8087, 4)

  implicit val yFormat = jsonFormat4(Y)

  implicit val yConverter = new RaikuConverter[Y] {
    def read(o: RWObject): ReadResult[Y] = try {
      yFormat.read(new String(o.value).asJson).success
    }
    catch {
      case e: Throwable ⇒ e.failure
    }
    def write(bucket: String, o: Y): RWObject = RWObject(bucket, o.id, o.toJson.toString.getBytes,
      binIndexes = Map("group_id" -> List(o.groupId)), intIndexes = Map("age" -> List(o.age)))
  }

  val bucket = RaikuBucket[Y]("raiku_test_z_bucket_"+java.util.UUID.randomUUID.toString, client)

  sequential

  "A map reduce phase" should {
    "be run in correct sequence" in {
      val a = MRBuiltinFunction("Riak.mapValuesJson")
      val b = MRBuiltinFunction("Riak.filterNotFound")
      val c = MRBuiltinFunction("Riak.reduceSum")
      val d = MRBuiltinFunction("Riak.reduceMin")

      val r = MapPhase(a) |* MapPhase(b) |- ReducePhase(c) |- ReducePhase(d)
      val rl = r.phases.list
      rl(0).fn == a && rl(1).fn == b && rl(2).fn == c && rl(3).fn == d
    }
  }

  "A map reduce job" should {
    "be able to be serialized correctly" in {
      val buildInFunction = MRBuiltinFunction("Riak.mapValuesJson")
      val customFunction = MRFunction("""function(values, arg){
       return values.reduce(function(acc, item){
       for(state in item){
         if(acc[state])
          acc[state] += item[state];
         else
          acc[state] = item[state];
       }
       return acc;
     });
    }""")

      val mrJob = MR.items(Set(("persons", "a"), ("persons", "b"))) |>> (MapPhase(buildInFunction) |- ReducePhase(customFunction, k = true))
      val json = MapReduceJobJsonFormat.write(mrJob)
      val comparable = JsObject(
        "inputs" -> JsArray(JsArray(List(JsString("persons"), JsString("a"))), JsArray(List(JsString("persons"), JsString("b")))),
        "query" -> JsArray(
          JsObject("map" -> JsObject(
            "language" -> JsString("javascript"),
            "name" -> JsString(buildInFunction.value),
            "keep" -> JsBoolean(false))),
          JsObject("reduce" -> JsObject(
            "language" -> JsString("javascript"),
            "source" -> JsString(customFunction.value),
            "keep" -> JsBoolean(true)))),
        "timeout" -> JsNumber(60000))

      json == comparable
    }
  }
}