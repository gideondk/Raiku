package nl.gideondk.raiku.commands

import com.basho.riak.protobuf._
import scalaz._
import Scalaz._
import nl.gideondk.raiku.monads._
import nl.gideondk.raiku.actors._
import spray.json._
import nl.gideondk.sentinel.Task

case class VClock(v: Array[Byte])

case class VTag(v: Array[Byte])

case class RWObject(bucket: String, key: String, value: Array[Byte], contentType: String = "text/plain",
                    vClock: Option[VClock] = None, vTag: Option[VTag] = None, lastModified: Option[Int] = None,
                    lastModifiedMicros: Option[Int] = None, userMeta: Map[String, Option[Array[Byte]]] = Map[String, Option[Array[Byte]]](),
                    intIndexes: Map[String, List[Int]] = Map[String, List[Int]](), binIndexes: Map[String, List[String]] = Map[String, List[String]](),
                    deleted: Option[Boolean] = None)

trait RWRequests extends Request {
  def fetchServerInfo: Task[RpbGetServerInfoResp] = {
    val req = buildRequest(RiakMessageType.RpbGetServerInfoReq)
    req.map(x ⇒ RpbGetServerInfoResp().mergeFrom(x.message.toArray))
  }

  def fetchBucketProperties(bucket: String): Task[RpbBucketProps] = {
    val req = buildRequest(RiakMessageType.RpbGetBucketReq, RpbGetBucketReq(bucket))
    req.map(x ⇒ RpbGetBucketResp().mergeFrom(x.message.toArray)).map(_.props)
  }

  def setBucketProperties(bucket: String, props: RpbBucketProps): Task[Unit] = {
    val req = buildRequest(RiakMessageType.RpbSetBucketReq, RpbSetBucketReq(bucket, props))
    req.map(x ⇒ ())
  }

  def fetch(bucket: String, key: String, r: Option[Int] = None, pr: Option[Int] = None,
            basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None,
            ifModified: Option[VClock] = None, head: Option[Boolean] = None,
            deletedvclock: Option[Boolean] = None): Task[List[RWObject]] = {
    val req = buildRequest(RiakMessageType.RpbGetReq, RpbGetReq(bucket, key, r, pr, basicQuorum, notFoundOk, ifModified.map(x ⇒ x.v), head, deletedvclock))
    req.map(x ⇒ RpbGetResp().mergeFrom(x.message.toArray)).map(x ⇒ pbGetRespToRWObjects(key, bucket, x))
  }

  def store(rwObject: RWObject, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
            pw: Option[Int] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None,
            returnHead: Option[Boolean] = None): Task[List[RWObject]] = {
    val req = buildRequest(RiakMessageType.RpbPutReq, RpbPutReq(rwObject.bucket, stringToByteString(rwObject.key).some,
      rwObject.vClock.map(x ⇒ x.v), rwObjectToRpbContent(rwObject), w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead))
    req.map(x ⇒ RpbPutResp().mergeFrom(x.message.toArray)).map(x ⇒ pbPutRespToRWObjects(rwObject.key, rwObject.bucket, x))
  }

  def deleteByKey(bucket: String, key: String, rw: Option[Int] = None, vClock: Option[VClock] = None, r: Option[Int] = None,
                  w: Option[Int] = None, pr: Option[Int] = None, pw: Option[Int] = None, dw: Option[Int] = None): Task[Unit] = {
    val req = buildRequest(RiakMessageType.RpbDelReq, RpbDelReq(bucket, key, rw, vClock.map(x ⇒ x.v), r, w, pr, pw, dw))
    req.map(x ⇒ ())
  }

  def delete(rwObject: RWObject, rw: Option[Int] = None, r: Option[Int] = None,
             w: Option[Int] = None, pr: Option[Int] = None, pw: Option[Int] = None, dw: Option[Int] = None): Task[Unit] = {
    deleteByKey(rwObject.bucket, rwObject.key, rw, rwObject.vClock, r, w, pr, pw, dw)
  }

  def fetchKeysForIndexRequest(req: Task[RiakMessage]) = {
    req.map(x ⇒ RpbIndexResp().mergeFrom(x.message.toArray)).map(x ⇒ x.keys.map(byteStringToString(_)).toList)
  }

  def fetchKeysForBinIndexByValue(bucket: String, idx: String, idxv: String): Task[List[String]] = {
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_bin", RpbIndexReq.IndexQueryType.valueOf(0), idxv.some.map(x ⇒ x), None, None)))
  }

  def fetchKeysForIntIndexByValue(bucket: String, idx: String, idxv: Int): Task[List[String]] = {
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(0), idxv.some.map(x ⇒ x.toString), None, None)))
  }

  def fetchKeysForIntIndexByValueRange(bucket: String, idx: String, idxr: Range): Task[List[String]] = {
    fetchKeysForIndexRequest(buildRequest(RiakMessageType.RpbIndexReq, RpbIndexReq(bucket, idx+"_int", RpbIndexReq.IndexQueryType.valueOf(1), None, idxr.start.some.map(x ⇒ x.toString), idxr.end.some.map(x ⇒ x.toString))))
  }
}
