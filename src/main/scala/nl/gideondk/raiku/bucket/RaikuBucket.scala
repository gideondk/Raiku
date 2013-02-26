package nl.gideondk.raiku.bucket

import nl.gideondk.raiku._
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
  def fetchBucketProperties =
    client.fetchBucketProperties(bucketName).map(x ⇒ RaikuBucketProperties(x.nVal, x.allowMult))

  def setBucketProperties(pr: RaikuBucketProperties) =
    client.setBucketProperties(bucketName, RpbBucketProps(pr.nVal, pr.allowMulti))

  def fetch(key: String,
            r: RArgument = RArgument(),
            pr: PRArgument = PRArgument(),
            basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
            notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
            ifModified: IfModifiedArgument = IfModifiedArgument(),
            onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
            deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): ValidatedFutureIO[Option[T]] = {
    val (nR, pR) = (List(r.v, config.r.v).flatten headOption, List(pr.v, config.pr.v).flatten headOption)
    client.fetch(bucketName, key, nR, pR, basicQuorum.v, notFoundOk.v, ifModified.v, onlyHead.v, deletedVClock.v)
  }

  def fetchMany(keys: List[String],
                r: RArgument = RArgument(),
                pr: PRArgument = PRArgument(),
                basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
                deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): ValidatedFutureIO[List[T]] = {
    ValidatedFutureIO.sequence(keys.map(fetch(_, r, pr, basicQuorum, notFoundOk, IfModifiedArgument(None), onlyHead, deletedVClock))).map(_.flatten)
  }

  def store(obj: T,
            r: RArgument = RArgument(),
            pr: PRArgument = PRArgument(),
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
    val converted = converter.write(bucketName, obj)
    val (nR, pR) = (List(r.v, config.r.v).flatten headOption, List(pr.v, config.pr.v).flatten headOption)
    ValidatedFutureIORWListToValidatedFutureIOOptRW(client.fetch(bucketName, converted.key, nR, pR, basicQuorum.v, notFoundOk.v, deletedvclock = deletedVClock.v))
      .flatMap {
        fetchObj: Option[RWObject] ⇒
          val (nW, nDw, nPw) = (List(w.v, config.w.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption)
          // Clobber converter as default
          val storeObj = converter.write(bucketName, obj).copy(vClock = fetchObj.map(_.vClock).getOrElse(None))
          client.store(storeObj, nW, nDw, returnBody.v, nPw, ifNotModified.v, ifNonMatched.v, returnHead.v)
      }
  }

  /* 
     * *Should* be faster then the "normal" store, because object won't be fetched first to determine the vclock.
     * Only to be used when you are *absolutely* sure this object doesn't already exists.
     *
     * Some parameters (like ifNonModified, ifNonMatched) won't make sense for this purpose, 
     * leaving them here if you want to misuse this function for other purposes.
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
    client.store(converter.write(bucketName, obj), nW, nDw, returnBody.v, nPw, ifNotModified.v, ifNonMatched.v, returnHead.v)
  }

  def storeMany(objs: List[T],
                r: RArgument = RArgument(),
                pr: PRArgument = PRArgument(),
                basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                w: WArgument = WArgument(),
                dw: DWArgument = DWArgument(),
                returnBody: ReturnBodyArgument = ReturnBodyArgument(),
                pw: PWArgument = PWArgument(),
                ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
                returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[List[T]] = {
    ValidatedFutureIO.sequence(objs.map(store(_, r, pr, basicQuorum, notFoundOk, DeletedVClockArgument(None), w, dw, returnBody, pw, IfNotModifiedArgument(None), ifNonMatched, returnHead))).map(_.flatten)
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

  def delete(obj: T,
             rw: RWArgument = RWArgument(),
             r: RArgument = RArgument(),
             w: WArgument = WArgument(),
             pr: PRArgument = PRArgument(),
             pw: PWArgument = PWArgument(),
             dw: DWArgument = DWArgument()): ValidatedFutureIO[Unit] = {
    val (nRw, nR, nW, nPr, nPw, nDw) = (List(rw.v, config.rw.v).flatten headOption, List(r.v, config.r.v).flatten headOption, List(w.v, config.w.v).flatten headOption,
      List(pr.v, config.pr.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption)
    client.delete(converter.write(bucketName, obj), nRw, nR, nW, nPr, nPw, nDw)
  }

  def deleteMany(objs: List[T],
                 rw: RWArgument = RWArgument(),
                 r: RArgument = RArgument(),
                 w: WArgument = WArgument(),
                 pr: PRArgument = PRArgument(),
                 pw: PWArgument = PWArgument(),
                 dw: DWArgument = DWArgument()): ValidatedFutureIO[List[Unit]] = {
    ValidatedFutureIO.sequence(objs.map(delete(_, rw, r, w, pr, pw, dw)))
  }

  def fetchKeysForBinIndexByValue(idxk: String, idxv: String): ValidatedFutureIO[List[String]] =
    client.fetchKeysForBinIndexByValue(bucketName, idxk, idxv)

  def fetchKeysForIntIndexByValue(idxk: String, idxv: Int): ValidatedFutureIO[List[String]] =
    client.fetchKeysForIntIndexByValue(bucketName, idxk, idxv)

  def fetchKeysForIntIndexByValueRange(idxk: String, idxr: Range): ValidatedFutureIO[List[String]] =
    client.fetchKeysForIntIndexByValueRange(bucketName, idxk, idxr)

  /*
     * 
     * DSL 
     *
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

  def ?*(keys: List[String],
         r: RArgument = RArgument(),
         pr: PRArgument = PRArgument(),
         basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
         notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
         onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
         deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): ValidatedFutureIO[List[T]] =
    fetchMany(keys, r, pr, basicQuorum, notFoundOk, onlyHead, deletedVClock)

  def <<(obj: T,
         r: RArgument = RArgument(),
         pr: PRArgument = PRArgument(),
         basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
         notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
         deletedVClock: DeletedVClockArgument = DeletedVClockArgument(),
         w: WArgument = WArgument(),
         dw: DWArgument = DWArgument(),
         returnBody: ReturnBodyArgument = ReturnBodyArgument(),
         pw: PWArgument = PWArgument(),
         ifNotModified: IfNotModifiedArgument = IfNotModifiedArgument(),
         ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
         returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[Option[T]] =
    store(obj, r, pr, basicQuorum, notFoundOk, deletedVClock, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead)

  def <<*(objs: List[T],
          r: RArgument = RArgument(),
          pr: PRArgument = PRArgument(),
          basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
          notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
          w: WArgument = WArgument(),
          dw: DWArgument = DWArgument(),
          returnBody: ReturnBodyArgument = ReturnBodyArgument(),
          pw: PWArgument = PWArgument(),
          ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument(),
          returnHead: ReturnHeadArgument = ReturnHeadArgument()): ValidatedFutureIO[List[T]] =
    storeMany(objs, r, pr, basicQuorum, notFoundOk, w, dw, returnBody, pw, ifNonMatched, returnHead)

  def -(obj: T,
        rw: RWArgument = RWArgument(),
        r: RArgument = RArgument(),
        w: WArgument = WArgument(),
        pr: PRArgument = PRArgument(),
        pw: PWArgument = PWArgument(),
        dw: DWArgument = DWArgument()): ValidatedFutureIO[Unit] =
    delete(obj, rw, r, w, pr, pw, dw)

  def -*(objs: List[T],
         rw: RWArgument = RWArgument(),
         r: RArgument = RArgument(),
         w: WArgument = WArgument(),
         pr: PRArgument = PRArgument(),
         pw: PWArgument = PWArgument(),
         dw: DWArgument = DWArgument()): ValidatedFutureIO[List[Unit]] =
    deleteMany(objs, rw, r, w, pr, pw, dw)

  def idx(idxk: String, idxv: String): ValidatedFutureIO[List[String]] =
    fetchKeysForBinIndexByValue(idxk, idxv)

  def idx(idxk: String, idxv: Int): ValidatedFutureIO[List[String]] =
    fetchKeysForIntIndexByValue(idxk, idxv)

  def idx(idxk: String, idxv: Range): ValidatedFutureIO[List[String]] =
    fetchKeysForIntIndexByValueRange(idxk, idxv)
}

