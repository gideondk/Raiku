package nl.gideondk.raiku

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

import shapeless._
import TypeOperators._

import scala.reflect.ClassTag

case class RaikuBucketProperties(nVal: Option[Int], allowMulti: Option[Boolean])

case class RaikuBucketConfig(r: RArgument = RArgument(), rw: RWArgument = RWArgument(),
                             w: WArgument = WArgument(), pr: PRArgument = PRArgument(), pw: PWArgument = PWArgument(), dw: DWArgument = DWArgument())

case class RaikuBucket[T: ClassTag](bucketName: String, client: RaikuClient, config: RaikuBucketConfig = RaikuBucketConfig(),
                                    resolver: RaikuResolver[T] = RaikuResolver.throwConflicts[T], mutator: RaikuMutator[T] = RaikuMutator.clobber[T])(implicit converter: RaikuValueConverter[T]) {

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
            deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): Task[Option[RaikuValue[T]]] = {
    val (nR, pR) = (List(r.v, config.r.v).flatten headOption, List(pr.v, config.pr.v).flatten headOption)
    val fetchResp = client.fetch(bucketName, key, nR, pR, basicQuorum.v, notFoundOk.v, deletedvclock = None)
    fetchResp map { x ⇒
      val content = x.content.map(converter.readRaw(_))
      if (content.length < 2) content.headOption else resolver(content)
    }
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
                deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): Task[List[RaikuValue[T]]] = {
    Task.sequenceSuccesses(keys.map(fetch(_, r, pr, basicQuorum, notFoundOk, IfModifiedArgument(None), onlyHead, deletedVClock))).map(_.flatten)
  }

  /** Stores a RaikuValue[T] to the current Raiku bucket
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
   *  @param returnHead returnBody except that the value(s) in the object are blank to avoid returning potentially large value(s)
   */

  def store[A: (T |∨| RaikuValue[T])#λ](o: A,
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
                                        returnHead: ReturnHeadArgument = ReturnHeadArgument()): Task[Option[RaikuValue[T]]] = {
    val (nR, pR) = (List(r.v, config.r.v).flatten headOption, List(pr.v, config.pr.v).flatten headOption)
    val obj = o match {
      case x: T             ⇒ converter.write(bucketName, x)
      case x: RaikuValue[T] ⇒ x
    }

    val fetchResp = client.fetch(bucketName, obj.key, nR, pR, basicQuorum.v, notFoundOk.v, deletedvclock = None)
    fetchResp flatMap { x ⇒
      val content = x.content.map(converter.readRaw(_))
      val resolved = if (content.length < 2) content.headOption else resolver(content)
      val storeObj = converter.writeToRaw(mutator(resolved, obj))

      val (nW, nDw, nPw) = (List(w.v, config.w.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption)
      client.store(storeObj, nW, nDw, returnBody.v, nPw, x.vClock, ifNotModified.v, ifNonMatched.v, returnHead.v).map(_.content.headOption.map(converter.readRaw(_)))
    }
  }

  def unsafeStoreNew[A: (T |∨| RaikuValue[T])#λ](o: A,
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
                                                 returnHead: ReturnHeadArgument = ReturnHeadArgument()): Task[Option[RaikuValue[T]]] = {
    val (nR, pR) = (List(r.v, config.r.v).flatten headOption, List(pr.v, config.pr.v).flatten headOption)
    val obj = o match {
      case x: T             ⇒ converter.write(bucketName, x)
      case x: RaikuValue[T] ⇒ x
    }

    val (nW, nDw, nPw) = (List(w.v, config.w.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption)
    client.store(converter.writeToRaw(obj), nW, nDw, returnBody.v, nPw, None, ifNotModified.v, ifNonMatched.v, returnHead.v).map(_.content.headOption.map(converter.readRaw(_)))
  }

  /** Stores a List[T] in parallel to the current Raiku bucket
   *
   *  @param objs the to-be stored objects in Riak
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
   *  @param returnHead returnBody except that the value(s) in the object are blank to avoid returning potentially large value(s)
   */

  def storeMany[A: (List[T] |∨| List[RaikuValue[T]])#λ](objs: A,
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
                                                        returnHead: ReturnHeadArgument = ReturnHeadArgument()): Task[List[RaikuValue[T]]] = {
    objs match {
      case List(_: T, _*)             ⇒ Task.sequenceSuccesses(objs.asInstanceOf[List[T]].map(store(_, r, pr, basicQuorum, notFoundOk, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead))).map(_.flatten)
      case List(_: RaikuValue[T], _*) ⇒ Task.sequenceSuccesses(objs.asInstanceOf[List[RaikuValue[T]]].map(store(_, r, pr, basicQuorum, notFoundOk, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead))).map(_.flatten)
    }
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

  def delete[A: (T |∨| RaikuValue[T])#λ](o: A,
                                         rw: RWArgument = RWArgument(),
                                         vClock: VClockArgument = VClockArgument(),
                                         r: RArgument = RArgument(),
                                         w: WArgument = WArgument(),
                                         pr: PRArgument = PRArgument(),
                                         pw: PWArgument = PWArgument(),
                                         dw: DWArgument = DWArgument()): Task[Unit] = {
    val obj = o match {
      case x: T             ⇒ converter.write(bucketName, x)
      case x: RaikuValue[T] ⇒ x
    }

    val (nRw, nR, nW, nPr, nPw, nDw) = (List(rw.v, config.rw.v).flatten headOption, List(r.v, config.r.v).flatten headOption, List(w.v, config.w.v).flatten headOption,
      List(pr.v, config.pr.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption)
    client.delete(converter.writeToRaw(obj), nRw, vClock.v, nR, nW, nPr, nPw, nDw)
  }

  def deleteByKey(key: String,
                  rw: RWArgument = RWArgument(),
                  vClock: VClockArgument = VClockArgument(),
                  r: RArgument = RArgument(),
                  w: WArgument = WArgument(),
                  pr: PRArgument = PRArgument(),
                  pw: PWArgument = PWArgument(),
                  dw: DWArgument = DWArgument()): Task[Unit] = {
    val (nRw, nR, nW, nPr, nPw, nDw) = (List(rw.v, config.rw.v).flatten headOption, List(r.v, config.r.v).flatten headOption, List(w.v, config.w.v).flatten headOption,
      List(pr.v, config.pr.v).flatten headOption, List(pw.v, config.pw.v).flatten headOption, List(dw.v, config.dw.v).flatten headOption)
    client.deleteByKey(bucketName, key, nRw, vClock.v, nR, nW, nPr, nPw, nDw)
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

  def deleteMany[A: (List[T] |∨| List[RaikuValue[T]])#λ](objs: A,
                                                         rw: RWArgument = RWArgument(),
                                                         r: RArgument = RArgument(),
                                                         w: WArgument = WArgument(),
                                                         pr: PRArgument = PRArgument(),
                                                         pw: PWArgument = PWArgument(),
                                                         dw: DWArgument = DWArgument()): Task[List[Unit]] = {

    objs match {
      case List(_: T, _*)             ⇒ Task.sequenceSuccesses(objs.asInstanceOf[List[T]].map(delete(_, rw, VClockArgument(None), r, w, pr, pw, dw)))
      case List(_: RaikuValue[T], _*) ⇒ Task.sequenceSuccesses(objs.asInstanceOf[List[RaikuValue[T]]].map(delete(_, rw, VClockArgument(None), r, w, pr, pw, dw)))
    }
  }

  def deleteManyByKey(keys: Seq[String],
                      rw: RWArgument = RWArgument(),
                      r: RArgument = RArgument(),
                      w: WArgument = WArgument(),
                      pr: PRArgument = PRArgument(),
                      pw: PWArgument = PWArgument(),
                      dw: DWArgument = DWArgument()): Task[Seq[Unit]] = {
    Task.sequenceSuccesses(keys.map(deleteByKey(_, rw, VClockArgument(None), r, w, pr, pw, dw)).toList).map(_.toSeq)
  }

  /** Fetches keys based on a binary index
   *
   *  @param idxk the binary index key
   *  @param idxv the binary index value
   */

  def fetchKeysForBinIndexByValue(idxk: String, idxv: String): Task[List[String]] =
    client.fetchKeysForBinIndexByValue(bucketName, idxk, idxv)

  /** Fetches keys based on a integer index
   *
   *  @param idxk the integer index key
   *  @param idxv the integer index value
   */

  def fetchKeysForIntIndexByValue(idxk: String, idxv: Int): Task[List[String]] =
    client.fetchKeysForIntIndexByValue(bucketName, idxk, idxv)

  /** Fetches keys based on a ranged integer index
   *
   *  @param idxk the integer index key
   *  @param idxr the ranged integer index value
   */

  def fetchKeysForIntIndexByValueRange(idxk: String, idxr: Range): Task[List[String]] =
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
        deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): Task[Option[RaikuValue[T]]] =
    fetch(key, r, pr, basicQuorum, notFoundOk, ifModified, onlyHead, deletedVClock)

  /** @see fetchMany
   */

  def ?*(keys: List[String],
         r: RArgument = RArgument(),
         pr: PRArgument = PRArgument(),
         basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
         notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
         onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
         deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): Task[List[RaikuValue[T]]] =
    fetchMany(keys, r, pr, basicQuorum, notFoundOk, onlyHead, deletedVClock)

  /** @see store
   */

  def <<[A: (T |∨| RaikuValue[T])#λ](obj: A,
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
                                     returnHead: ReturnHeadArgument = ReturnHeadArgument()): Task[Option[RaikuValue[T]]] =
    store(obj, r, pr, basicQuorum, notFoundOk, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead)

  /** @see storeMany
   */

  def <<*[A: (List[T] |∨| List[RaikuValue[T]])#λ](objs: A,
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
                                                  returnHead: ReturnHeadArgument = ReturnHeadArgument()): Task[List[RaikuValue[T]]] =
    storeMany(objs, r, pr, basicQuorum, notFoundOk, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead)

  /** @see delete
   */

  def -[A: (T |∨| RaikuValue[T])#λ](obj: A,
                                    rw: RWArgument = RWArgument(),
                                    vClock: VClockArgument = VClockArgument(),
                                    r: RArgument = RArgument(),
                                    w: WArgument = WArgument(),
                                    pr: PRArgument = PRArgument(),
                                    pw: PWArgument = PWArgument(),
                                    dw: DWArgument = DWArgument()): Task[Unit] =
    delete(obj, rw, vClock, r, w, pr, pw, dw)

  /** @see deleteMany
   */

  def -*[A: (List[T] |∨| List[RaikuValue[T]])#λ](objs: A,
                                                 rw: RWArgument = RWArgument(),
                                                 r: RArgument = RArgument(),
                                                 w: WArgument = WArgument(),
                                                 pr: PRArgument = PRArgument(),
                                                 pw: PWArgument = PWArgument(),
                                                 dw: DWArgument = DWArgument()): Task[List[Unit]] =
    deleteMany(objs, rw, r, w, pr, pw, dw)

  /** @see fetchKeysForBinIndexByValue
   */

  def idx(idxk: String, idxv: String): Task[List[String]] =
    fetchKeysForBinIndexByValue(idxk, idxv)

  /** @see fetchKeysForIntIndexByValue
   */

  def idx(idxk: String, idxv: Int): Task[List[String]] =
    fetchKeysForIntIndexByValue(idxk, idxv)

  /** @see fetchKeysForIntIndexByValueRange
   */

  def idx(idxk: String, idxv: Range): Task[List[String]] =
    fetchKeysForIntIndexByValueRange(idxk, idxv)
}

