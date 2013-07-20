package nl.gideondk.raiku.commands

import nl.gideondk.raiku._
import com.basho.riak.protobuf._
import scalaz._
import Scalaz._
import spray.json._
import nl.gideondk.sentinel.client.SentinelClient

import play.api.libs.iteratee._
import akka.util.ByteString

trait IndexRequests extends Request {
  def siWorker: SentinelClient[RiakCommand, Enumerator[RiakResponse]]

  def buildStreaming2iRequest(messageType: RiakMessageType, message: ByteString): Task[Enumerator[String]] =
    siWorker.sendCommand(RiakCommand(messageType, message)).map {
      _ &> Enumeratee.map { x: RiakResponse ⇒
        riakResponseToValidation(x) match {
          case scala.util.Success(s) ⇒ RpbIndexResp().mergeFrom(s.message.toArray)
          case scala.util.Failure(e) ⇒ throw e
        }
      } &> Enumeratee.map(_.keys.map(byteStringToString(_)).toList) &> Enumeratee.mapFlatten { x ⇒ Enumerator(x: _*) }
    }

  def fetchKeysForIndexRequest(req: Task[RiakMessage]) =
    req.map(x ⇒ RpbIndexResp().mergeFrom(x.message.toArray)).map(x ⇒ x.keys.map(byteStringToString(_)).toList)

  def fetchKeysForMaxedIndexRequest(req: Task[RiakMessage]) = req.map { x ⇒
    val resp = RpbIndexResp().mergeFrom(x.message.toArray)
    val cont = resp.continuation.map(byteStringToString(_))
    val keys = resp.keys.map(byteStringToString(_)).toList
    (cont, keys)
  }

  def fetchKeysForBinIndexByValue(bucket: String, idx: String, idxv: String): Task[List[String]] =
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(0), idxv.some.map(x ⇒ x), None, None)))

  def fetchKeysForIntIndexByValue(bucket: String, idx: String, idxv: Int): Task[List[String]] =
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(0), idxv.some.map(x ⇒ x.toString), None, None)))

  def fetchKeysForBinIndexByValueRange(bucket: String, idx: String, idxr: RaikuStringRange): Task[List[String]] =
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(1), None, idxr.start.some.map(x ⇒ x.toString), idxr.end.some.map(x ⇒ x.toString))))

  def fetchMaxedKeysForBinIndexByValueRange(bucket: String, idx: String, idxr: RaikuStringRange, maxResults: Int, continuation: Option[String]): Task[(Option[String], List[String])] =
    fetchKeysForMaxedIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(1), None, idxr.start.some.map(x ⇒ x.toString), idxr.end.some.map(x ⇒ x.toString), None, None, Some(maxResults), continuation.map(x ⇒ x))))

  def fetchKeysForIntIndexByValueRange(bucket: String, idx: String, idxr: Range): Task[List[String]] =
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(1), None, idxr.start.some.map(x ⇒ x.toString), idxr.end.some.map(x ⇒ x.toString))))

  def fetchMaxedKeysForIntIndexByValueRange(bucket: String, idx: String, idxr: Range, maxResults: Int, continuation: Option[String]): Task[(Option[String], List[String])] =
    fetchKeysForMaxedIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(1), None, idxr.start.some.map(x ⇒ x.toString), idxr.end.some.map(x ⇒ x.toString), None, None, Some(maxResults), continuation.map(x ⇒ x))))

  def streamKeysForBinIndexByValue(bucket: String, idx: String, idxv: String): Task[Enumerator[String]] =
    buildStreaming2iRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(0), idxv.some.map(x ⇒ x), None, None, None, Some(true)))

  def streamKeysForIntIndexByValue(bucket: String, idx: String, idxv: Int): Task[Enumerator[String]] =
    buildStreaming2iRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(0), idxv.some.map(x ⇒ x.toString), None, None, None, Some(true)))

  def streamKeysForIntIndexByValueRange(bucket: String, idx: String, idxr: Range): Task[Enumerator[String]] =
    buildStreaming2iRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(1), None, idxr.start.some.map(x ⇒ x.toString), idxr.end.some.map(x ⇒ x.toString), None, Some(true)))

  def streamKeysForBinIndexByValueRange(bucket: String, idx: String, idxr: RaikuStringRange): Task[Enumerator[String]] =
    buildStreaming2iRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(1), None, idxr.start.some.map(x ⇒ x.toString), idxr.end.some.map(x ⇒ x.toString), None, Some(true)))

}