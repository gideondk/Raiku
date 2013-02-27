package nl.gideondk.raiku

import monads.ValidatedFutureIO
import scalaz._
import Scalaz._
import effect._

import scala.concurrent._
import ExecutionContext.Implicits.global

import nl.gideondk.raiku.serialization._
import com.basho.riak.protobuf._
import nl.gideondk.raiku.commands._
import play.api.libs.iteratee._
import Enumerator._
import spray.json.JsValue

case class RaikuReactiveBucket[T](bucketName: String, client: RaikuClient, config: RaikuBucketConfig = RaikuBucketConfig(), prefetchSize: Int = 8)(implicit converter: RaikuConverter[T]) {
  private val normalBucket = RaikuBucket(bucketName, client, config)

  /** Creates a Enumeratee[String, Option[T]], consuming single keys, fetching items, and producing Options on T
   */

  def fetchEnumeratee(r: RArgument = RArgument(),
                      pr: PRArgument = PRArgument(),
                      basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                      notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                      onlyHead: OnlyHeadArgument = OnlyHeadArgument()): Enumeratee[String, Option[T]] =
    Enumeratee.mapM[String](x ⇒ unwrapValidation(normalBucket.fetch(x, r, pr, basicQuorum, notFoundOk, IfModifiedArgument(None), onlyHead, DeletedVClockArgument(None)).unsafePerformIO.run))

  /** Creates a Enumeratee[List[String], List[Option[T]]], consuming a list of keys, fetching items in parallel,
   *  and producing a Lists of Options on T
   */

  def fetchManyEnumeratee(r: RArgument = RArgument(),
                          pr: PRArgument = PRArgument(),
                          basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                          notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                          onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
                          deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): Enumeratee[List[String], List[Option[T]]] =
    Enumeratee.mapM[List[String]](x ⇒ unwrapValidation(ValidatedFutureIO.sequence(x.map(normalBucket.fetch(_, r, pr, basicQuorum, notFoundOk,
      IfModifiedArgument(None), onlyHead, DeletedVClockArgument(None)))).unsafePerformIO.run))

  /** Takes a Enumerator[String], pipes it through a fetchEnumeratee and consumes the (found) items into a List[T]
   */

  def fetch(enum: Enumerator[String],
            r: RArgument = RArgument(),
            pr: PRArgument = PRArgument(),
            basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
            notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
            onlyHead: OnlyHeadArgument = OnlyHeadArgument()): Future[List[T]] = {
    enum &> fetchEnumeratee(r, pr, basicQuorum, notFoundOk, onlyHead) |>>> Iteratee.fold(List[T]())((result, chunk) ⇒ result ++ chunk.toList)
  }

  /** Creates a Enumeratee[T, Option[T]], consuming objects, storing the items, and producing Options on T (filled when returnBody = true)
   */

  def storeEnumeratee(r: RArgument = RArgument(),
                      pr: PRArgument = PRArgument(),
                      basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                      notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                      w: WArgument = WArgument(),
                      dw: DWArgument = DWArgument(),
                      pw: PWArgument = PWArgument(),
                      returnBody: ReturnBodyArgument = ReturnBodyArgument(),
                      ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument()): Enumeratee[T, Option[T]] =
    Enumeratee.mapM[T](x ⇒ unwrapValidation(normalBucket.store(x, r, pr, basicQuorum, notFoundOk, w, dw, returnBody,
      pw, IfNotModifiedArgument(None), ifNonMatched, ReturnHeadArgument(None)).unsafePerformIO.run))

  /** Iteratee[T, Int] consuming objects until EOF, returning the count of the stored items afterwards
   */

  def storeIteratee(r: RArgument = RArgument(),
                    pr: PRArgument = PRArgument(),
                    basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                    notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                    w: WArgument = WArgument(),
                    dw: DWArgument = DWArgument(),
                    pw: PWArgument = PWArgument(),
                    ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument()): Iteratee[T, Int] = {
    Iteratee.foldM(0) { (result, chunk) ⇒
      normalBucket.store(chunk, r, pr, basicQuorum, notFoundOk, w, dw, ReturnBodyArgument(None), pw, IfNotModifiedArgument(None), ifNonMatched, ReturnHeadArgument(None)).unsafePerformIO.run.flatMap(z ⇒ z match {
        case Success(s) ⇒ Future(result + 1)
        case Failure(e) ⇒ Future.failed(e)
      })
    }
  }

  /** Takes a Enumerator[T], pipes it through a storeIteratee, returning the count of the stored items afterwards
   */

  def store(enum: Enumerator[T],
            r: RArgument = RArgument(),
            pr: PRArgument = PRArgument(),
            basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
            notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
            w: WArgument = WArgument(),
            dw: DWArgument = DWArgument(),
            pw: PWArgument = PWArgument(),
            ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument()): Future[Int] =
    enum |>>> storeIteratee(r, pr, basicQuorum, notFoundOk, w, dw, pw, ifNonMatched)

  /** Creates a Enumeratee[T, Unit], consuming objects, deleting the items, and producing Units
   */

  def deleteEnumeratee(rw: RWArgument = RWArgument(),
                       r: RArgument = RArgument(),
                       w: WArgument = WArgument(),
                       pr: PRArgument = PRArgument(),
                       pw: PWArgument = PWArgument(),
                       dw: DWArgument = DWArgument()): Enumeratee[T, Unit] =
    Enumeratee.mapM[T](x ⇒ unwrapValidation(normalBucket.delete(x, rw, VClockArgument(None), r, w, pr, pw, dw).unsafePerformIO.run))

  /** Iteratee[T, Int] consuming objects until EOF, returning the count of the deleted items afterwards
   */

  def deleteIteratee(rw: RWArgument = RWArgument(),
                     r: RArgument = RArgument(),
                     w: WArgument = WArgument(),
                     pr: PRArgument = PRArgument(),
                     pw: PWArgument = PWArgument(),
                     dw: DWArgument = DWArgument()): Iteratee[T, Int] = {
    Iteratee.foldM(0)((result, chunk) ⇒ normalBucket.delete(chunk, rw, VClockArgument(None), r, w, pr, pw, dw).unsafePerformIO.run.flatMap(z ⇒ z match {
      case Success(s) ⇒ Future(result + 1)
      case Failure(e) ⇒ Future.failed(e)
    }))
  }

  /** Takes a Enumerator[T], pipes it through a deleteIteratee, returning the count of the deleted items afterwards
   */

  def delete(enum: Enumerator[T],
             rw: RWArgument = RWArgument(),
             r: RArgument = RArgument(),
             w: WArgument = WArgument(),
             pr: PRArgument = PRArgument(),
             pw: PWArgument = PWArgument(),
             dw: DWArgument = DWArgument()): Future[Int] = {
    enum |>>> deleteIteratee(rw, r, w, pr, pw, dw)
  }

  /** Creates a Enumeratee[(String, String), List[String]], consuming tuples on the index key and value, producting a list of matched keys
   */

  def binIdxEnumeratee: Enumeratee[(String, String), List[String]] =
    Enumeratee.mapM[(String, String)](x ⇒ unwrapValidation(normalBucket.fetchKeysForBinIndexByValue(x._1, x._2).unsafePerformIO.run))

  /** Creates a Enumeratee[(String, Int), List[String]], consuming tuples on the index key and value, producting a list of matched keys
   */

  def intIdxEnumeratee: Enumeratee[(String, Int), List[String]] =
    Enumeratee.mapM[(String, Int)](x ⇒ unwrapValidation(normalBucket.fetchKeysForIntIndexByValue(x._1, x._2).unsafePerformIO.run))

  /** Creates a Enumeratee[(String, Range), List[String]], consuming tuples on the index key and value, producting a list of matched keys
   */

  def rangeIdxEnumeratee: Enumeratee[(String, Range), List[String]] =
    Enumeratee.mapM[(String, Range)](x ⇒ unwrapValidation(normalBucket.fetchKeysForIntIndexByValueRange(x._1, x._2).unsafePerformIO.run))

  /** Takes a list of keys and creates a Enumerator generating T's
   */

  def enumerateForKeys(keys: List[String],
                       r: RArgument = RArgument(),
                       pr: PRArgument = PRArgument(),
                       basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                       notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                       onlyHead: OnlyHeadArgument = OnlyHeadArgument()): Enumerator[T] = {
    Enumerator(keys.grouped(prefetchSize).toList: _*) &> fetchManyEnumeratee(r, pr, basicQuorum, notFoundOk, onlyHead) &> Enumeratee.mapConcat[List[Option[T]]](x ⇒ x.flatten)
  }

  def unwrapValidation[A](a: Future[Validation[Throwable, A]]) = a.flatMap { z ⇒
    z match {
      case Success(s) ⇒ Future(s)
      case Failure(e) ⇒ Future.failed(e)
    }
  }
}