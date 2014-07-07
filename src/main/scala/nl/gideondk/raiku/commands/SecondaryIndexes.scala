package nl.gideondk.raiku.commands

import nl.gideondk.raiku._
import com.basho.riak.protobuf._
import nl.gideondk.sentinel.processors.Consumer.ConsumerException
import scala.concurrent.Future
import spray.json._

import play.api.libs.iteratee._
import akka.util.ByteString

trait IndexRequests extends Request {

  def buildStreaming2iRequest(messageType: RiakMessageType, message: ByteString): Future[Enumerator[String]] =
    (streamWorker ?->> (RiakCommand(messageType, message))).map {
      _ &> Enumeratee.map { x: RiakResponse ⇒
        riakResponseToValidation(x) match {
          case scala.util.Success(s) ⇒ RpbIndexResp().mergeFrom(s.message.toArray)
          case scala.util.Failure(e) ⇒ throw e
        }
      } &> Enumeratee.map(_.keys.map(byteStringToString(_)).toList) &> Enumeratee.mapFlatten { x ⇒ Enumerator(x: _*) }
    }.recoverWith {
      case e: ConsumerException[RiakMessage] ⇒ Future.failed(new Exception(RpbErrorResp().mergeFrom(e.cause.message.toArray).errmsg))
      case e                                 ⇒ Future.failed(e)
    }

  def fetchKeysForIndexRequest(req: Future[RiakResponse]) =
    req.map(x ⇒ RpbIndexResp().mergeFrom(x.message.toArray)).map(x ⇒ x.keys.map(byteStringToString(_)).toList)

  def fetchKeysForMaxedIndexRequest(req: Future[RiakResponse]) = req.map { x ⇒
    val resp = RpbIndexResp().mergeFrom(x.message.toArray)
    val cont = resp.continuation.map(byteStringToString(_))
    val keys = resp.keys.map(byteStringToString(_)).toList
    (cont, keys)
  }

  /* Basic Index Queries */

  def fetchKeysForBinIndexByValue(bucket: String, idx: String, idxv: String, bucketType: Option[String] = None): Future[List[String]] =
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(0), Some(idxv).map(x ⇒ x), None, None, `type` = bucketType.map(x ⇒ x))))

  def fetchKeysForIntIndexByValue(bucket: String, idx: String, idxv: Int, bucketType: Option[String] = None): Future[List[String]] =
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(0), Some(idxv).map(x ⇒ x.toString), None, None, `type` = bucketType.map(x ⇒ x))))

  /* Ranged Index Queries */

  def fetchKeysForBinIndexByValueRange(bucket: String, idx: String, idxr: RaikuStringRange, regex: Option[String] = None, bucketType: Option[String] = None): Future[List[String]] =
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(1), None, Some(idxr.start).map(x ⇒ x.toString), Some(idxr.end).map(x ⇒ x.toString), `termRegex` = regex.map(x ⇒ x), `type` = bucketType.map(x ⇒ x))))

  def fetchKeysForIntIndexByValueRange(bucket: String, idx: String, idxr: Range, bucketType: Option[String] = None): Future[List[String]] =
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(1), None, Some(idxr.start).map(x ⇒ x.toString), Some(idxr.end).map(x ⇒ x.toString), `type` = bucketType.map(x ⇒ x))))

  /* Paginated Index Queries */
  def fetchMaxedKeysForBinIndexByValue(bucket: String, idx: String, idxv: String, maxResults: Int, continuation: Option[String], bucketType: Option[String] = None): Future[(Option[String], List[String])] =
    fetchKeysForMaxedIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(0), Some(idxv).map(x ⇒ x), None, None, None, None, Some(maxResults), continuation.map(x ⇒ x), `type` = bucketType.map(x ⇒ x))))

  def fetchMaxedKeysForBinIndexByValueRange(bucket: String, idx: String, idxr: RaikuStringRange, maxResults: Int, continuation: Option[String], regex: Option[String] = None, bucketType: Option[String] = None): Future[(Option[String], List[String])] =
    fetchKeysForMaxedIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(1), None, Some(idxr.start).map(x ⇒ x), Some(idxr.end).map(x ⇒ x.toString), None, None, Some(maxResults), continuation.map(x ⇒ x), `termRegex` = regex.map(x ⇒ x), `type` = bucketType.map(x ⇒ x))))

  def fetchMaxedKeysForIntIndexByValue(bucket: String, idx: String, idxv: Int, maxResults: Int, continuation: Option[String], bucketType: Option[String] = None): Future[(Option[String], List[String])] =
    fetchKeysForMaxedIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(0), Some(idxv).map(x ⇒ x.toString), None, None, None, None, Some(maxResults), continuation.map(x ⇒ x), `type` = bucketType.map(x ⇒ x))))

  def fetchMaxedKeysForIntIndexByValueRange(bucket: String, idx: String, idxr: Range, maxResults: Int, continuation: Option[String], bucketType: Option[String] = None): Future[(Option[String], List[String])] =
    fetchKeysForMaxedIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(1), None, Some(idxr.start).map(x ⇒ x.toString), Some(idxr.end).map(x ⇒ x.toString), None, None, Some(maxResults), continuation.map(x ⇒ x), `type` = bucketType.map(x ⇒ x))))

  /* Streaming Index Queries */

  def streamKeysForBinIndexByValue(bucket: String, idx: String, idxv: String, bucketType: Option[String] = None): Future[Enumerator[String]] =
    buildStreaming2iRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(0), Some(idxv).map(x ⇒ x), None, None, None, Some(true), `type` = bucketType.map(x ⇒ x)))

  def streamKeysForIntIndexByValue(bucket: String, idx: String, idxv: Int, bucketType: Option[String] = None): Future[Enumerator[String]] =
    buildStreaming2iRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(0), Some(idxv).map(x ⇒ x.toString), None, None, None, Some(true), `type` = bucketType.map(x ⇒ x)))

  def streamKeysForBinIndexByValueRange(bucket: String, idx: String, idxr: RaikuStringRange, regex: Option[String] = None, bucketType: Option[String] = None): Future[Enumerator[String]] =
    buildStreaming2iRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(1), None, Some(idxr.start).map(x ⇒ x.toString), Some(idxr.end).map(x ⇒ x.toString), None, Some(true), `termRegex` = regex.map(x ⇒ x), `type` = bucketType.map(x ⇒ x)))

  def streamKeysForIntIndexByValueRange(bucket: String, idx: String, idxr: Range, bucketType: Option[String] = None): Future[Enumerator[String]] =
    buildStreaming2iRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(1), None, Some(idxr.start).map(x ⇒ x.toString), Some(idxr.end).map(x ⇒ x.toString), None, Some(true), `type` = bucketType.map(x ⇒ x)))

}
