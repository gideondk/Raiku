package nl.gideondk.raiku.commands

import com.basho.riak.protobuf._

import scala.concurrent.Future

trait CounterRequests extends Request {
  def getCount(bucket: String, key: String, bucketType: Option[String], r: Option[Int] = None, pr: Option[Int] = None,
               basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None): Future[Long] = {
    val req = buildRequest(RiakMessageType.RbpCounterGetReq, RpbCounterGetReq(bucket, key, r, pr, basicQuorum, notFoundOk))
    req.map(x ⇒ RpbCounterGetResp().mergeFrom(x.message.toArray)).map(_.value.getOrElse(0l))
  }

  def incrementCount(bucket: String, key: String, amount: Long, bucketType: Option[String], w: Option[Int] = None,
                     dw: Option[Int] = None, pw: Option[Int] = None, returnValue: Boolean = true): Future[Option[Long]] = {
    val req = buildRequest(RiakMessageType.RbpCounterUpdateReq, RpbCounterUpdateReq(bucket, key, amount, w, dw, pw, Some(returnValue)))
    req.map(x ⇒ RpbCounterUpdateResp().mergeFrom(x.message.toArray)).map(_.value)
  }
}

