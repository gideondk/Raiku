package nl.gideondk.raiku

import monads.ValidatedFutureIO
import scalaz._
import Scalaz._

import scala.concurrent._
import ExecutionContext.Implicits.global

import com.basho.riak.protobuf._
import nl.gideondk.raiku.commands._
import nl.gideondk.raiku.serialization._
import play.api.libs.iteratee._
import Enumerator._
import spray.json.JsValue

case class RaikuBucketProperties(nVal: Option[Int], allowMulti: Option[Boolean])

case class RaikuBucketConfig(r: RArgument = RArgument(), rw: RWArgument = RWArgument(),
                             w: WArgument = WArgument(), pr: PRArgument = PRArgument(), pw: PWArgument = PWArgument(), dw: DWArgument = DWArgument())

case class RaikuBucket[T](bucketName: String, client: RaikuClient, config: RaikuBucketConfig = RaikuBucketConfig())(implicit converter: RaikuConverter[T]) {

  /** Retrieves the bucket properties from the current bucket
   */

  def fetchBucketProperties =
    client.fetchBucketProperties(bucketName).map(x ⇒ RaikuBucketProperties(x.nVal, x.allowMult))

  /** Sets the bucket properties for the current bucket
   *  @param pr a RaikuBucketProperties class with the to-be-set bucket properties
   */

  def setBucketProperties(pr: RaikuBucketProperties) =
    client.setBucketProperties(bucketName, RpbBucketProps(pr.nVal, pr.allowMulti))

  /** Fetches a T from the current Raiku bucket
   *
   *  @param key the to be retrieved key from Riak
   *  @param r the R argument: how many replicas need to agree when retrieving the object
   *  @param pr the PR argument: how many primary replicas need to be available when retrieving the object
   *  @param basicQuorum whether to return early in some failure cases (eg. when r=1 and you get 2 errors and a success basic_quorum=true would return an error)
   *  @param notFoundOk whether to treat notfounds as successful reads for the purposes of R
   *  @param ifModified when a vclock is supplied as this option only return the object if the vclocks don't match
   *  @param onlyHead only return the head of the object – allows you to get only the meta data for a potentially large value
   *  @param deletedVClock return the tombstone's vclock, if applicable
   */

  def fetch(key: String,
            r: RArgument = RArgument(),
            pr: PRArgument = PRArgument(),
            basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
            notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
            ifModified: IfModifiedArgument = IfModifiedArgument(),
            onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
            deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): ValidatedFutureIO[Option[T]] = {
    val (nR, pR) = (List(r.v, config.r.v).flatten headOption, List(pr.v, config.pr.v).flatten headOption)
    ValidatedFutureIORWListToValidatedFutureIOOptT(client.fetch(bucketName, key, nR, pR, basicQuorum.v, notFoundOk.v, ifModified.v, onlyHead.v, deletedVClock.v))
  }

  /** Fetches a List[T] in parallel from the current Raiku bucket
   *
   *  @param keys the to be retrieved keys from Riak
   *  @param r the R argument: how many replicas need to agree when retrieving the object
   *  @param pr the PR argument: how many primary replicas need to be available when retrieving the object
   *  @param basicQuorum whether to return early in some failure cases (eg. when r=1 and you get 2 errors and a success basic_quorum=true would return an error)
   *  @param notFoundOk whether to treat notfounds as successful reads for the purposes of R
   *  @param onlyHead only return the head of the object – allows you to get only the meta data for a potentially large value
   *  @param deletedVClock return the tombstone's vclock, if applicable
   */

  def fetchMany(keys: List[String],
                r: RArgument = RArgument(),
                pr: PRArgument = PRArgument(),
                basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
                deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): ValidatedFutureIO[List[T]] = {
    ValidatedFutureIO.sequence(keys.map(fetch(_, r, pr, basicQuorum, notFoundOk, IfModifiedArgument(None), onlyHead, deletedVClock))).map(_.flatten)
  }

  /** Stores a T to the current Raiku bucket
   *
   *  @param obj the to-be stored object in Riak
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
   *  @param like returnBody except that the value(s) in the object are blank to avoid returning potentially large value(s)
   */

  def store(obj: T,
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
            returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[Option[T]] = {
    val converted = converter.write(bucketName, obj)
    val (nR, pR) = (List(r.v, config.r.v).flatten headOption, List(pr.v, config.pr.v).flatten headOption)
    ValidatedFutureIORWListToValidatedFutureIOOptRW(client.fetch(bucketName, converted.key, nR, pR, basicQuorum.v, notFoundOk.v, deletedvclock = None))
      .flatMap {
        fetchObj: Option[RWObject] ⇒
          val (nW, nDw, nPw) = (List(w.v, config.w.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption)
          // Clobber converter as default
          val storeObj = converter.write(bucketName, obj).copy(vClock = fetchObj.map(_.vClock).getOrElse(None))
          ValidatedFutureIORWListToValidatedFutureIOOptT(client.store(storeObj, nW, nDw, returnBody.v, nPw, ifNotModified.v, ifNonMatched.v, returnHead.v))
      }
  }

  /*
     * *Should* be faster then the "normal" store, because object won't be fetched first to determine the vclock.
     * Only to be used when you are *absolutely* sure this object doesn't already exists.
     *
     * Not integrated in DSL, unsafe should stay unsafe ;-)
    */

  def unsafeStoreNew(obj: T,
                     basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                     notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                     deletedVClock: DeletedVClockArgument = DeletedVClockArgument(),
                     w: WArgument = WArgument(),
                     dw: DWArgument = DWArgument(),
                     returnBody: ReturnBodyArgument = ReturnBodyArgument(),
                     pw: PWArgument = PWArgument(),
                     ifNotModified: IfNotModifiedArgument = IfNotModifiedArgument(),
                     ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
                     returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[Option[T]] = {
    val (nW, nDw, nPw) = (List(w.v, config.w.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption)
    ValidatedFutureIORWListToValidatedFutureIOOptT(client.store(converter.write(bucketName, obj), nW, nDw, returnBody.v, nPw, ifNotModified.v, ifNonMatched.v, returnHead.v))
  }

  /** Stores a List[T] in parallel to the current Raiku bucket
   *
   *  @param obj the to-be stored object in Riak
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
   *  @param like returnBody except that the value(s) in the object are blank to avoid returning potentially large value(s)
   */

  def storeMany(objs: List[T],
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
                returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[List[T]] = {
    ValidatedFutureIO.sequence(objs.map(store(_, r, pr, basicQuorum, notFoundOk, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead))).map(_.flatten)
  }

  def unsafeStoreManyNew(objs: List[T],
                         basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                         notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                         w: WArgument = WArgument(),
                         dw: DWArgument = DWArgument(),
                         returnBody: ReturnBodyArgument = ReturnBodyArgument(),
                         pw: PWArgument = PWArgument(),
                         ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
                         returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[List[T]] = {
    ValidatedFutureIO.sequence(objs.map(unsafeStoreNew(_, basicQuorum, notFoundOk, DeletedVClockArgument(None), w, dw, returnBody, pw, IfNotModifiedArgument(None), ifNonMatched, returnHead))).map(_.flatten)
  }

  /** Deletes a T from the current Raiku bucket
   *
   *  @param obj the to-be deleted object from Riak
   *  @param rw how many replicas to delete before returning a successful response
   *  @param vClock opaque vector clock, use to prevent deleting of objects that have been modified since the last get request
   *  @param r (read quorum) how many replicas need to agree when retrieving the object
   *  @param w (write quorum) how many replicas to write to before returning a successful response
   *  @param pr (primary read quorum) how many primary replicas need to be available when retrieving the object
   *  @param pw how many primary nodes must be up when the write is attempted
   *  @param dw how many replicas to commit to durable storage before returning a successful response;
   */

  def delete(obj: T,
             rw: RWArgument = RWArgument(),
             vClock: VClockArgument = VClockArgument(),
             r: RArgument = RArgument(),
             w: WArgument = WArgument(),
             pr: PRArgument = PRArgument(),
             pw: PWArgument = PWArgument(),
             dw: DWArgument = DWArgument()): ValidatedFutureIO[Unit] = {
    val (nRw, nR, nW, nPr, nPw, nDw) = (List(rw.v, config.rw.v).flatten headOption, List(r.v, config.r.v).flatten headOption, List(w.v, config.w.v).flatten headOption,
      List(pr.v, config.pr.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption)
    client.delete(converter.write(bucketName, obj).copy(vClock = vClock.v), nRw, nR, nW, nPr, nPw, nDw)
  }

  /** Deletes a List[T] parallel from the current Raiku bucket
   *
   *  @param objs the to-be deleted objects from Riak
   *  @param rw how many replicas to delete before returning a successful response
   *  @param r (read quorum) how many replicas need to agree when retrieving the object
   *  @param w (write quorum) how many replicas to write to before returning a successful response
   *  @param pr (primary read quorum) how many primary replicas need to be available when retrieving the object
   *  @param pw how many primary nodes must be up when the write is attempted
   *  @param dw how many replicas to commit to durable storage before returning a successful response;
   */

  def deleteMany(objs: List[T],
                 rw: RWArgument = RWArgument(),
                 r: RArgument = RArgument(),
                 w: WArgument = WArgument(),
                 pr: PRArgument = PRArgument(),
                 pw: PWArgument = PWArgument(),
                 dw: DWArgument = DWArgument()): ValidatedFutureIO[List[Unit]] = {
    ValidatedFutureIO.sequence(objs.map(delete(_, rw, VClockArgument(None), r, w, pr, pw, dw)))
  }

  /** Fetches keys based on a binary index
   *
   *  @param idxk the binary index key
   *  @param idxv the binary index value
   */

  def fetchKeysForBinIndexByValue(idxk: String, idxv: String): ValidatedFutureIO[List[String]] =
    client.fetchKeysForBinIndexByValue(bucketName, idxk, idxv)

  /** Fetches keys based on a integer index
   *
   *  @param idxk the integer index key
   *  @param idxv the integer index value
   */

  def fetchKeysForIntIndexByValue(idxk: String, idxv: Int): ValidatedFutureIO[List[String]] =
    client.fetchKeysForIntIndexByValue(bucketName, idxk, idxv)

  /** Fetches keys based on a ranged integer index
   *
   *  @param idxk the integer index key
   *  @param idxv the ranged integer index value
   */

  def fetchKeysForIntIndexByValueRange(idxk: String, idxr: Range): ValidatedFutureIO[List[String]] =
    client.fetchKeysForIntIndexByValueRange(bucketName, idxk, idxr)

  /** @see fetch
   */

  def ?(key: String,
        r: RArgument = RArgument(),
        pr: PRArgument = PRArgument(),
        basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
        notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
        ifModified: IfModifiedArgument = IfModifiedArgument(),
        onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
        deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): ValidatedFutureIO[Option[T]] =
    fetch(key, r, pr, basicQuorum, notFoundOk, ifModified, onlyHead, deletedVClock)

  /** @see fetchMany
   */

  def ?*(keys: List[String],
         r: RArgument = RArgument(),
         pr: PRArgument = PRArgument(),
         basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
         notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
         onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
         deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): ValidatedFutureIO[List[T]] =
    fetchMany(keys, r, pr, basicQuorum, notFoundOk, onlyHead, deletedVClock)

  /** @see store
   */

  def <<(obj: T,
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
         returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[Option[T]] =
    store(obj, r, pr, basicQuorum, notFoundOk, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead)

  /** @see storeMany
   */

  def <<*(objs: List[T],
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
          returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[List[T]] =
    storeMany(objs, r, pr, basicQuorum, notFoundOk, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead)

  /** @see delete
   */

  def -(obj: T,
        rw: RWArgument = RWArgument(),
        vClock: VClockArgument = VClockArgument(),
        r: RArgument = RArgument(),
        w: WArgument = WArgument(),
        pr: PRArgument = PRArgument(),
        pw: PWArgument = PWArgument(),
        dw: DWArgument = DWArgument()): ValidatedFutureIO[Unit] =
    delete(obj, rw, vClock, r, w, pr, pw, dw)

  /** @see deleteMany
   */

  def -*(objs: List[T],
         rw: RWArgument = RWArgument(),
         r: RArgument = RArgument(),
         w: WArgument = WArgument(),
         pr: PRArgument = PRArgument(),
         pw: PWArgument = PWArgument(),
         dw: DWArgument = DWArgument()): ValidatedFutureIO[List[Unit]] =
    deleteMany(objs, rw, r, w, pr, pw, dw)

  /** @see fetchKeysForBinIndexByValue
   */

  def idx(idxk: String, idxv: String): ValidatedFutureIO[List[String]] =
    fetchKeysForBinIndexByValue(idxk, idxv)

  /** @see fetchKeysForIntIndexByValue
   */

  def idx(idxk: String, idxv: Int): ValidatedFutureIO[List[String]] =
    fetchKeysForIntIndexByValue(idxk, idxv)

  /** @see fetchKeysForIntIndexByValueRange
   */

  def idx(idxk: String, idxv: Range): ValidatedFutureIO[List[String]] =
    fetchKeysForIntIndexByValueRange(idxk, idxv)
}

