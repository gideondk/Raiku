package nl.gideondk.raiku.commands

import nl.gideondk.raiku._
import com.basho.riak.protobuf._
import scala.concurrent.Future

trait GeneralRequests extends Request {
  def fetchServerInfo: Future[RpbGetServerInfoResp] = {
    val req = buildRequest(RiakMessageType.RpbGetServerInfoReq)
    req.map(x ⇒ RpbGetServerInfoResp().mergeFrom(x.message.toArray))
  }
}

trait BucketRequests extends Request {
  def fetchBucketProperties(bucket: String, bucketType: Option[String] = None): Future[RpbBucketProps] = {
    val req = buildRequest(RiakMessageType.RpbGetBucketReq, RpbGetBucketReq(bucket, bucketType.map(x ⇒ x)))
    req.map(x ⇒ RpbGetBucketResp().mergeFrom(x.message.toArray)).map(_.props)
  }

  def setBucketProperties(bucket: String, props: RpbBucketProps, bucketType: Option[String] = None): Future[Unit] = {
    val req = buildRequest(RiakMessageType.RpbSetBucketReq, RpbSetBucketReq(bucket, props, bucketType.map(x ⇒ x)))
    req.map(x ⇒ ())
  }
}

trait RawRequests extends Request {

  case class RiakRawFetchResponse(content: Set[RaikuRawValue], vClock: Option[VClock], unchanged: Option[Boolean])

  case class RiakRawPutResponse(content: Set[RaikuRawValue], vClock: Option[VClock])

  def fetchRaw(bucket: String, key: String, bucketType: Option[String] = None, r: Option[Int] = None, pr: Option[Int] = None,
               basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None,
               ifModified: Option[VClock] = None, head: Option[Boolean] = None,
               deletedvclock: Option[Boolean] = None, timeout: Option[Int] = None,
               sloppyQuorum: Option[Boolean] = None, nVal: Option[Int] = None): Future[RiakRawFetchResponse] = {
    val req = buildRequest(RiakMessageType.RpbGetReq, RpbGetReq(bucket, key, r, pr, basicQuorum, notFoundOk, ifModified.map(x ⇒ x.v), head, deletedvclock, timeout, sloppyQuorum, nVal, bucketType.map(x ⇒ x)))
    req.map(x ⇒ RpbGetResp().mergeFrom(x.message.toArray)).map { resp ⇒
      RiakRawFetchResponse(pbGetRespToRawValues(key, bucket, bucketType, resp), resp.vclock.map(x ⇒ VClock(x.toByteArray)), resp.unchanged)
    }
  }

  def storeRaw(rv: RaikuRawValue, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
               pw: Option[Int] = None, vClock: Option[VClock] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None,
               returnHead: Option[Boolean] = None, timeout: Option[Int] = None,
               asis: Option[Boolean] = None, sloppyQuorum: Option[Boolean] = None, nVal: Option[Int] = None): Future[RiakRawPutResponse] = {
    val req = buildRequest(RiakMessageType.RpbPutReq, RpbPutReq(rv.bucket, Some(stringToByteString(rv.key)),
      vClock.map(_.v), rawValueToRpbContent(rv), w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead, timeout, asis, sloppyQuorum, nVal, rv.bucketType.map(x ⇒ x)))

    req.map(x ⇒ RpbPutResp().mergeFrom(x.message.toArray)).map { resp ⇒
      RiakRawPutResponse(pbPutRespToRawValues(rv.key, rv.bucket, rv.bucketType, resp), resp.vclock.map(x ⇒ VClock(x.toByteArray)))
    }
  }

  def deleteByKey(bucket: String, key: String, bucketType: Option[String] = None, rw: Option[Int] = None, vClock: Option[VClock] = None, r: Option[Int] = None,
                  w: Option[Int] = None, pr: Option[Int] = None, pw: Option[Int] = None, dw: Option[Int] = None, timeout: Option[Int] = None,
                  sloppyQuorum: Option[Boolean] = None, nVal: Option[Int] = None): Future[Unit] = {
    val req = buildRequest(RiakMessageType.RpbDelReq, RpbDelReq(bucket, key, rw, vClock.map(x ⇒ x.v), r, w, pr, pw, dw, timeout, sloppyQuorum, nVal, bucketType.map(x ⇒ x)))
    req.map(x ⇒ ())
  }

  def deleteRaw(rwObject: RaikuRawValue, rw: Option[Int] = None, vClock: Option[VClock] = None, r: Option[Int] = None,
                w: Option[Int] = None, pr: Option[Int] = None, pw: Option[Int] = None, dw: Option[Int] = None, timeout: Option[Int] = None,
                sloppyQuorum: Option[Boolean] = None, nVal: Option[Int] = None): Future[Unit] = {
    deleteByKey(rwObject.bucket, rwObject.key, rwObject.bucketType, rw, vClock, r, w, pr, pw, dw, timeout, sloppyQuorum, nVal)
  }
}

trait RaikuValueRequests extends RawRequests {
  def fetch[T](bucket: String,
               key: String,
               bucketType: Option[String] = None,
               r: RArgument = RArgument(),
               pr: PRArgument = PRArgument(),
               basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
               notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
               ifModified: IfModifiedArgument = IfModifiedArgument(),
               onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
               deletedVClock: DeletedVClockArgument = DeletedVClockArgument(),
               timeout: TimeoutArgument = TimeoutArgument(),
               sloppyQuorum: SloppyQuorumArgument = SloppyQuorumArgument(),
               nVal: NValArgument = NValArgument(),
               resolver: RaikuResolver[T] = RaikuResolver.throwConflicts[T])(implicit converter: RaikuValueConverter[T]): Future[Option[RaikuValue[T]]] = {
    val fetchResp = fetchRaw(bucket, key, bucketType, r.v, pr.v, basicQuorum.v, notFoundOk.v, ifModified.v, onlyHead.v, deletedVClock.v, timeout.v.map(_.toSeconds.toInt), sloppyQuorum.v, nVal.v)
    fetchResp map { x ⇒
      val content = x.content.map(converter.readRaw(_))
      if (content.toList.length < 2) content.headOption else resolver(content)
    }
  }

  /** Stores a RaikuValue[T] to its bucket
   *
   *  @param o the to-be stored object in Riak
   *  @param r the R argument used for retrieving the possible already-stored item: how many replicas need to agree when retrieving the object
   *  @param pr the PR argument used for retrieving the possible already-stored item: how many primary replicas need to be available when retrieving the object
   *  @param basicQuorum whether to return early in some failure cases on the retrieval of the possible already-stored item
   *  @param notFoundOk whether to treat notfounds as successful reads for the purposes of R for the possible already-stored item
   *
   *  @param w (write quorum) how many replicas to write to before returning a successful response
   *  @param dw how many replicas to commit to durable storage before returning a successful response
   *  @param returnBody whether to return the contents of the stored object
   *  @param pw how many primary nodes must be up when the write is attempted
   *  @param ifNotModified update the value only if the vclock of the pre-fetched object matches the one stored to Riak (only makes sense in highly concurrent environment)
   *  @param ifNonMatched store the value only if this bucket/key combination are not already defined
   *  @param onlyReturnHead returnBody except that the value(s) in the object are blank to avoid returning potentially large value(s)
   *  @param timeout Riak based timeout to indicate for how long Riak should wait internally before returning a error
   *
   */

  def store[T](o: RaikuValue[T],
               r: RArgument = RArgument(),
               pr: PRArgument = PRArgument(),
               basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
               notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
               w: WArgument = WArgument(),
               dw: DWArgument = DWArgument(),
               returnBody: ReturnBodyArgument = ReturnBodyArgument(),
               pw: PWArgument = PWArgument(),
               ifNotModified: IfNotModifiedArgument = IfNotModifiedArgument(),
               ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
               onlyReturnHead: ReturnHeadArgument = ReturnHeadArgument(),
               timeout: TimeoutArgument = TimeoutArgument(),
               asis: AsisArgument = AsisArgument(),
               sloppyQuorum: SloppyQuorumArgument = SloppyQuorumArgument(),
               nVal: NValArgument = NValArgument(),
               resolver: RaikuResolver[T] = RaikuResolver.throwConflicts[T],
               mutator: RaikuMutator[T] = RaikuMutator.clobber[T])(implicit converter: RaikuValueConverter[T]): Future[Option[RaikuValue[T]]] = {

    // Should probably expose the "only head" argument during the fetch phase when collisions can be resolved through metadata
    val fetchResp = fetchRaw(o.bucket, o.key, o.bucketType, r.v, pr.v, basicQuorum.v, notFoundOk.v, None, None, None, timeout.v.map(_.toSeconds.toInt), sloppyQuorum.v, nVal.v)

    fetchResp flatMap { x ⇒
      val content = x.content.map(converter.readRaw(_))
      val resolved = if (content.toList.length < 2) content.headOption else resolver(content)
      val storeObj = converter.writeToRaw(mutator(resolved, o))

      storeRaw(storeObj, w.v, dw.v, returnBody.v, pw.v, x.vClock, ifNotModified.v, ifNonMatched.v, onlyReturnHead.v, timeout.v.map(_.toSeconds.toInt), asis.v, sloppyQuorum.v, nVal.v).map(_.content.headOption.map(converter.readRaw(_)))
    }
  }

  def unsafeStoreNew[T](o: RaikuValue[T],
                        pr: PRArgument = PRArgument(),
                        w: WArgument = WArgument(),
                        dw: DWArgument = DWArgument(),
                        returnBody: ReturnBodyArgument = ReturnBodyArgument(),
                        pw: PWArgument = PWArgument(),
                        ifNotModified: IfNotModifiedArgument = IfNotModifiedArgument(),
                        ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
                        onlyReturnHead: ReturnHeadArgument = ReturnHeadArgument(),
                        timeout: TimeoutArgument = TimeoutArgument(),
                        asis: AsisArgument = AsisArgument(),
                        sloppyQuorum: SloppyQuorumArgument = SloppyQuorumArgument(),
                        nVal: NValArgument = NValArgument(),
                        resolver: RaikuResolver[T] = RaikuResolver.throwConflicts[T],
                        mutator: RaikuMutator[T] = RaikuMutator.clobber[T])(implicit converter: RaikuValueConverter[T]): Future[Option[RaikuValue[T]]] = {

    val storeObj = converter.writeToRaw(o)
    storeRaw(storeObj, w.v, dw.v, returnBody.v, pw.v, None, ifNotModified.v, ifNonMatched.v, onlyReturnHead.v, timeout.v.map(_.toSeconds.toInt), asis.v, sloppyQuorum.v, nVal.v).map(_.content.headOption.map(converter.readRaw(_)))
  }

  /** Deletes a RaikuValue[T] from its bucket
   *
   *  @param o the to-be deleted object from Riak
   *  @param rw how many replicas to delete before returning a successful response
   *  @param vClock opaque vector clock, use to prevent deleting of objects that have been modified since the last get request
   *  @param r (read quorum) how many replicas need to agree when retrieving the object
   *  @param w (write quorum) how many replicas to write to before returning a successful response
   *  @param pr (primary read quorum) how many primary replicas need to be available when retrieving the object
   *  @param pw how many primary nodes must be up when the write is attempted
   *  @param dw how many replicas to commit to durable storage before returning a successful response;
   */

  def delete[T](o: RaikuValue[T],
                rw: RWArgument = RWArgument(),
                vClock: VClockArgument = VClockArgument(),
                r: RArgument = RArgument(),
                w: WArgument = WArgument(),
                pr: PRArgument = PRArgument(),
                pw: PWArgument = PWArgument(),
                dw: DWArgument = DWArgument())(implicit converter: RaikuValueConverter[T]) = {
    val deleteObj = converter.writeToRaw(o)
    deleteRaw(deleteObj, rw.v, vClock.v, r.v, w.v, pr.v, pw.v, dw.v)
  }
}