package nl.gideondk.raiku.commands

import nl.gideondk.raiku._
import com.basho.riak.protobuf._
import scalaz._
import Scalaz._
import spray.json._
import nl.gideondk.sentinel.client.SentinelClient

import play.api.libs.iteratee._
import akka.util.ByteString

trait GeneralRequests extends Request {
  def fetchServerInfo: Task[RpbGetServerInfoResp] = {
    val req = buildRequest(RiakMessageType.RpbGetServerInfoReq)
    req.map(x ⇒ RpbGetServerInfoResp().mergeFrom(x.message.toArray))
  }
}

trait BucketRequests extends Request {
  def fetchBucketProperties(bucket: String): Task[RpbBucketProps] = {
    val req = buildRequest(RiakMessageType.RpbGetBucketReq, RpbGetBucketReq(bucket))
    req.map(x ⇒ RpbGetBucketResp().mergeFrom(x.message.toArray)).map(_.props)
  }

  def setBucketProperties(bucket: String, props: RpbBucketProps): Task[Unit] = {
    val req = buildRequest(RiakMessageType.RpbSetBucketReq, RpbSetBucketReq(bucket, props))
    req.map(x ⇒ ())
  }
}

trait RWRequests extends Request {
  case class RiakRawFetchResponse(content: Set[RaikuRawValue], vClock: Option[VClock], unchanged: Option[Boolean])
  case class RiakRawPutResponse(content: Set[RaikuRawValue], vClock: Option[VClock])

  def fetch(bucket: String, key: String, r: Option[Int] = None, pr: Option[Int] = None,
            basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None,
            ifModified: Option[VClock] = None, head: Option[Boolean] = None,
            deletedvclock: Option[Boolean] = None): Task[RiakRawFetchResponse] = {
    val req = buildRequest(RiakMessageType.RpbGetReq, RpbGetReq(bucket, key, r, pr, basicQuorum, notFoundOk, ifModified.map(x ⇒ x.v), head, deletedvclock))
    req.map(x ⇒ RpbGetResp().mergeFrom(x.message.toArray)).map { resp ⇒
      RiakRawFetchResponse(pbGetRespToRawValues(key, bucket, resp), resp.vclock.map(x ⇒ VClock(x.toByteArray)), resp.unchanged)
    }
  }

  def store(rv: RaikuRawValue, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
            pw: Option[Int] = None, vClock: Option[VClock] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None,
            returnHead: Option[Boolean] = None): Task[RiakRawPutResponse] = {
    val req = buildRequest(RiakMessageType.RpbPutReq, RpbPutReq(rv.bucket, stringToByteString(rv.key).some,
      vClock.map(_.v), rawValueToRpbContent(rv), w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead))

    req.map(x ⇒ RpbPutResp().mergeFrom(x.message.toArray)).map { resp ⇒
      RiakRawPutResponse(pbPutRespToRawValues(rv.key, rv.bucket, resp), resp.vclock.map(x ⇒ VClock(x.toByteArray)))
    }
  }

  def deleteByKey(bucket: String, key: String, rw: Option[Int] = None, vClock: Option[VClock] = None, r: Option[Int] = None,
                  w: Option[Int] = None, pr: Option[Int] = None, pw: Option[Int] = None, dw: Option[Int] = None): Task[Unit] = {
    val req = buildRequest(RiakMessageType.RpbDelReq, RpbDelReq(bucket, key, rw, vClock.map(x ⇒ x.v), r, w, pr, pw, dw))
    req.map(x ⇒ ())
  }

  def delete(rwObject: RaikuRawValue, rw: Option[Int] = None, vClock: Option[VClock] = None, r: Option[Int] = None,
             w: Option[Int] = None, pr: Option[Int] = None, pw: Option[Int] = None, dw: Option[Int] = None): Task[Unit] = {
    deleteByKey(rwObject.bucket, rwObject.key, rw, vClock, r, w, pr, pw, dw)
  }
}