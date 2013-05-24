package nl.gideondk.raiku

import scala.util.{ Try, Success, Failure }

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

import scala.reflect.ClassTag

import nl.gideondk.raiku.serialization._
import com.basho.riak.protobuf._
import nl.gideondk.raiku.commands._
import play.api.libs.iteratee._
import Enumerator._
import spray.json.JsValue

case class RaikuReactiveBucket[T: ClassTag](bucketName: String, client: RaikuClient, config: RaikuBucketConfig = RaikuBucketConfig(), prefetchSize: Int = 8,
                                            resolver: RaikuResolver[T] = RaikuResolver.throwConflicts[T], mutator: RaikuMutator[T] = RaikuMutator.clobber[T], waitAtMostForEachRequest: FiniteDuration = 5 seconds)(implicit converter: RaikuValueConverter[T]) {
  private val normalBucket = RaikuBucket(bucketName, client, config)

  implicit val atMost = waitAtMostForEachRequest

  /** Creates a Enumeratee[String, Option[T]], consuming single keys, fetching items, and producing Options on T
   */

  def fetchEnumeratee(r: RArgument = RArgument(),
                      pr: PRArgument = PRArgument(),
                      basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                      notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                      onlyHead: OnlyHeadArgument = OnlyHeadArgument()): Enumeratee[String, Option[T]] =
    Enumeratee.mapM[String](x ⇒ unwrapTry(normalBucket.fetch(x, r, pr, basicQuorum, notFoundOk, IfModifiedArgument(None), onlyHead, DeletedVClockArgument(None)).start).map(_.flatMap(_.value)))

  /** Creates a Enumeratee[List[String], List[Option[T]]], consuming a list of keys, fetching items in parallel,
   *  and producing a Lists of Options on T
   */

  def fetchManyEnumeratee(r: RArgument = RArgument(),
                          pr: PRArgument = PRArgument(),
                          basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                          notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                          onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
                          deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): Enumeratee[List[String], List[Option[T]]] =
    Enumeratee.mapM[List[String]](x ⇒ unwrapTry(Task.sequence(x.map(normalBucket.fetch(_, r, pr, basicQuorum, notFoundOk,
      IfModifiedArgument(None), onlyHead, DeletedVClockArgument(None)))).start).map(_.map(_.flatMap(_.value))))

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
    Enumeratee.mapM[T](x ⇒ unwrapTry(normalBucket.store(x, r, pr, basicQuorum, notFoundOk, w, dw, returnBody,
      pw, IfNotModifiedArgument(None), ifNonMatched, ReturnHeadArgument(None)).start).map(_.flatMap(_.value)))

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
      normalBucket.store(chunk, r, pr, basicQuorum, notFoundOk, w, dw, ReturnBodyArgument(None), pw, IfNotModifiedArgument(None), ifNonMatched, ReturnHeadArgument(None)).start.flatMap(z ⇒ z match {
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
    Enumeratee.mapM[T](x ⇒ unwrapTry(normalBucket.delete(x, rw, VClockArgument(None), r, w, pr, pw, dw).start))

  /** Iteratee[T, Int] consuming objects until EOF, returning the count of the deleted items afterwards
   */

  def deleteIteratee(rw: RWArgument = RWArgument(),
                     r: RArgument = RArgument(),
                     w: WArgument = WArgument(),
                     pr: PRArgument = PRArgument(),
                     pw: PWArgument = PWArgument(),
                     dw: DWArgument = DWArgument()): Iteratee[T, Int] = {
    Iteratee.foldM(0)((result, chunk) ⇒ normalBucket.delete(chunk, rw, VClockArgument(None), r, w, pr, pw, dw).start.flatMap(z ⇒ z match {
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
    Enumeratee.mapM[(String, String)](x ⇒ unwrapTry(normalBucket.fetchKeysForBinIndexByValue(x._1, x._2).start))

  /** Creates a Enumeratee[(String, Int), List[String]], consuming tuples on the index key and value, producting a list of matched keys
   */

  def intIdxEnumeratee: Enumeratee[(String, Int), List[String]] =
    Enumeratee.mapM[(String, Int)](x ⇒ unwrapTry(normalBucket.fetchKeysForIntIndexByValue(x._1, x._2).start))

  /** Creates a Enumeratee[(String, Range), List[String]], consuming tuples on the index key and value, producting a list of matched keys
   */

  def rangeIdxEnumeratee: Enumeratee[(String, Range), List[String]] =
    Enumeratee.mapM[(String, Range)](x ⇒ unwrapTry(normalBucket.fetchKeysForIntIndexByValueRange(x._1, x._2).start))

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

  def unwrapTry[A](a: Future[Try[A]]) = a.flatMap { z ⇒
    z match {
      case Success(s) ⇒ Future(s)
      case Failure(e) ⇒ Future.failed(e)
    }
  }
}
