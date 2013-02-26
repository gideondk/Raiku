package nl.gideondk.raiku.bucket

import nl.gideondk.raiku._
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

  def fetchEnumeratee(r: RArgument = RArgument(),
                      pr: PRArgument = PRArgument(),
                      basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                      notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                      onlyHead: OnlyHeadArgument = OnlyHeadArgument()): Enumeratee[String, Option[T]] = {
    Enumeratee.mapM[String](x ⇒ normalBucket.fetch(x, r, pr, basicQuorum, notFoundOk, IfModifiedArgument(None), onlyHead, DeletedVClockArgument(None)).unsafePerformIO.run.flatMap { z ⇒
      z match {
        case Success(s) ⇒ Future(s)
        case Failure(e) ⇒ Future.failed(e)
      }
    })
  }

  def fetchManyEnumeratee(r: RArgument = RArgument(),
                          pr: PRArgument = PRArgument(),
                          basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                          notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                          onlyHead: OnlyHeadArgument = OnlyHeadArgument(),
                          deletedVClock: DeletedVClockArgument = DeletedVClockArgument()): Enumeratee[List[String], List[Option[T]]] = {
    Enumeratee.mapM[List[String]](x ⇒ ValidatedFutureIO.sequence(x.map(normalBucket.fetch(_, r, pr, basicQuorum, notFoundOk, IfModifiedArgument(None), onlyHead, DeletedVClockArgument(None)))).unsafePerformIO.run.flatMap { z ⇒
      z match {
        case Success(s) ⇒ Future(s)
        case Failure(e) ⇒ Future.failed(e)
      }
    })
  }

  def fetch(enum: Enumerator[String],
            r: RArgument = RArgument(),
            pr: PRArgument = PRArgument(),
            basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
            notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
            onlyHead: OnlyHeadArgument = OnlyHeadArgument()): Future[List[T]] = {
    enum &> fetchEnumeratee(r, pr, basicQuorum, notFoundOk, onlyHead) |>>> Iteratee.fold(List[T]())((result, chunk) ⇒ result ++ chunk.toList)
  }

  def storeIteratee(r: RArgument = RArgument(),
                    pr: PRArgument = PRArgument(),
                    basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                    notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                    w: WArgument = WArgument(),
                    dw: DWArgument = DWArgument(),
                    pw: PWArgument = PWArgument(),
                    ifNonMatched: IfNonMatchedArgument = IfNonMatchedArgument()): Iteratee[T, Int] = {
    Iteratee.foldM(0)((result, chunk) ⇒ normalBucket.store(chunk, r, pr, basicQuorum, notFoundOk, DeletedVClockArgument(None), w, dw, ReturnBodyArgument(None), pw, IfNotModifiedArgument(None), ifNonMatched, ReturnHeadArgument(None)).unsafePerformIO.run.flatMap(z ⇒ z match {
      case Success(s) ⇒ Future(result + 1)
      case Failure(e) ⇒ Future.failed(e)
    }))
  }

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

  def deleteIteratee(rw: RWArgument = RWArgument(),
                     r: RArgument = RArgument(),
                     w: WArgument = WArgument(),
                     pr: PRArgument = PRArgument(),
                     pw: PWArgument = PWArgument(),
                     dw: DWArgument = DWArgument()): Iteratee[T, Int] = {
    Iteratee.foldM(0)((result, chunk) ⇒ normalBucket.delete(chunk, rw, r, w, pr, pw, dw).unsafePerformIO.run.flatMap(z ⇒ z match {
      case Success(s) ⇒ Future(result + 1)
      case Failure(e) ⇒ Future.failed(e)
    }))
  }

  def delete(enum: Enumerator[T],
             rw: RWArgument = RWArgument(),
             r: RArgument = RArgument(),
             w: WArgument = WArgument(),
             pr: PRArgument = PRArgument(),
             pw: PWArgument = PWArgument(),
             dw: DWArgument = DWArgument()): Future[Int] = {
    enum |>>> deleteIteratee(rw, r, w, pr, pw, dw)
  }

  def binIdxEnumeratee: Enumeratee[(String, String), List[String]] =
    Enumeratee.mapM[(String, String)](x ⇒ normalBucket.fetchKeysForBinIndexByValue(x._1, x._2).unsafePerformIO.run.flatMap { z ⇒
      z match {
        case Success(s) ⇒ Future(s)
        case Failure(e) ⇒ Future.failed(e)
      }
    })

  def intIdxEnumeratee: Enumeratee[(String, Int), List[String]] =
    Enumeratee.mapM[(String, Int)](x ⇒ normalBucket.fetchKeysForIntIndexByValue(x._1, x._2).unsafePerformIO.run.flatMap { z ⇒
      z match {
        case Success(s) ⇒ Future(s)
        case Failure(e) ⇒ Future.failed(e)
      }
    })

  def rangeIdxEnumeratee: Enumeratee[(String, Range), List[String]] =
    Enumeratee.mapM[(String, Range)](x ⇒ normalBucket.fetchKeysForIntIndexByValueRange(x._1, x._2).unsafePerformIO.run.flatMap { z ⇒
      z match {
        case Success(s) ⇒ Future(s)
        case Failure(e) ⇒ Future.failed(e)
      }
    })

  def enumerateForKeys(keys: List[String],
                       r: RArgument = RArgument(),
                       pr: PRArgument = PRArgument(),
                       basicQuorum: BasicQuorumArgument = BasicQuorumArgument(),
                       notFoundOk: NotFoundOkArgument = NotFoundOkArgument(),
                       onlyHead: OnlyHeadArgument = OnlyHeadArgument()): Enumerator[T] = {
    Enumerator(keys.grouped(prefetchSize).toList: _*) &> fetchManyEnumeratee(r, pr, basicQuorum, notFoundOk, onlyHead) &> Enumeratee.mapConcat[List[Option[T]]](x ⇒ x.flatten)
  }

}