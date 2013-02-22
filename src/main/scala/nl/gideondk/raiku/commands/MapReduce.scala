package nl.gideondk.raiku.commands

import com.basho.riak.protobuf._
import spray.json._

import scalaz._
import Scalaz._

import play.api.libs.iteratee._
import scala.concurrent.Future

import nl.gideondk.raiku.monads._
import nl.gideondk.raiku.mapreduce._
import nl.gideondk.raiku.mapreduce.MapReduceJsonProtocol._

trait MapReduce extends MRRequest {
  def mapReduce(job: MapReduceJob): ValidatedFutureIO[List[JsArray]] = {
    streamMapReduce(job).flatMap {
      ee ⇒
        val fl = ee |>>> Iteratee.fold(List.empty[Future[List[JsValue]]]) {
          (result, chunk) ⇒
            result ++ List(
              chunk |>>> Iteratee.fold(List.empty[JsValue]) {
                (result, chunk) ⇒ result ++ List(chunk)
              })
        }
        ValidatedFutureIO(fl.map(x ⇒ Future.sequence(x)).flatMap(x ⇒ x)).map(_.map(x ⇒ JsArray(x)))
    }
  }

  def streamMapReduce(job: MapReduceJob): ValidatedFutureIO[Enumerator[Enumerator[JsValue]]] = {
    buildMRRequest(request(RiakMessageType.RpbMapRedReq, RpbMapRedReq(job.toJson.compactPrint, "application/json")))
  }
}