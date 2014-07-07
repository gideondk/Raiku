package nl.gideondk.raiku

import nl.gideondk.raiku.commands._
import nl.gideondk.raiku.serialization._

import scala.reflect.ClassTag

import com.basho.riak.protobuf._

import scala.concurrent._

import play.api.libs.iteratee._
import Enumerator._
import spray.json.JsValue

case class RaikuBucketProperties(nVal: Option[Int], allowMulti: Option[Boolean])

case class RaikuBucketConfig(r: RArgument = RArgument(), rw: RWArgument = RWArgument(),
                             w: WArgument = WArgument(), pr: PRArgument = PRArgument(),
                             pw: PWArgument = PWArgument(), dw: DWArgument = DWArgument(),
                             basicQuorum: BasicQuorumArgument = BasicQuorumArgument(), notFoundOk: NotFoundOkArgument = NotFoundOkArgument())

object RaikuBucket {
  def typed[T](bucketType: String, bucketName: String, client: RaikuClient, resolver: RaikuResolver[T] = RaikuResolver.throwConflicts[T], mutator: RaikuMutator[T] = RaikuMutator.clobber[T], config: RaikuBucketConfig = RaikuBucketConfig())(implicit converter: RaikuValueConverter[T], executionContext: ExecutionContext) =
    RaikuBucket(bucketName, Some(bucketType), client, resolver, mutator, config)(converter, executionContext)

  def default[T](bucketName: String, client: RaikuClient, resolver: RaikuResolver[T] = RaikuResolver.throwConflicts[T], mutator: RaikuMutator[T] = RaikuMutator.clobber[T], config: RaikuBucketConfig = RaikuBucketConfig())(implicit converter: RaikuValueConverter[T], executionContext: ExecutionContext) =
    RaikuBucket(bucketName, None, client, resolver, mutator, config)(converter, executionContext)
}

case class RaikuBucket[T](bucketName: String, bucketType: Option[String], client: RaikuClient, resolver: RaikuResolver[T], mutator: RaikuMutator[T], config: RaikuBucketConfig)(implicit converter: RaikuValueConverter[T], executionContext: ExecutionContext) {

  /** Retrieves the bucket properties from the current bucket
   */

  def fetchBucketProperties =
    client.fetchBucketProperties(bucketName, bucketType).map(x ⇒ RaikuBucketProperties(x.nVal, x.allowMult))

  /** Sets the bucket properties for the current bucket
   *  @param pr a RaikuBucketProperties class with the to-be-set bucket properties
   */

  def setBucketProperties(pr: RaikuBucketProperties) =
    client.setBucketProperties(bucketName, RpbBucketProps(pr.nVal, pr.allowMulti), bucketType)

  /** Fetches a T from the current Raiku bucket
   *
   *  @param key the to be retrieved key from Riak
   *  @param onlyReturnIfModifiedFrom when a vclock is supplied, only return the object if its vclock is "newer"
   *  @param config override the bucket configuration with a custom request-specific configuration
   */

  def fetch(key: String,
            onlyReturnIfModifiedFrom: IfModifiedArgument = IfModifiedArgument(),
            config: RaikuBucketConfig = config): Future[Option[T]] = {
    client.fetch[T](bucketName, key, bucketType, config.r, config.pr, config.basicQuorum, config.notFoundOk, onlyReturnIfModifiedFrom, resolver = resolver).map(_.map(unwrapRaikuValue))
  }

  /** Fetches a List[T] in parallel from the current Raiku bucket
   *
   *  @param keys the to be retrieved keys from Riak
   *  @param config override the bucket configuration with a custom request-specific configuration
   */

  def fetchMany(keys: List[String],
                config: RaikuBucketConfig = config): Future[List[T]] = {
    Future.sequence(keys.map(fetch(_, config = config))).map(_.flatten)
  }

  /** Stores a T to the current Raiku bucket
   *
   *  @param obj the to-be stored object in Riak
   *  @param returnBody whether to return the contents of the stored object
   *  @param config override the bucket configuration with a custom request-specific configuration
   */

  def store(obj: T,
            returnBody: ReturnBodyArgument = ReturnBodyArgument(),
            config: RaikuBucketConfig = config): Future[Option[T]] = {
    val converted = converter.write(bucketName, bucketType, obj)
    client.store(converted, config.r, config.pr, config.basicQuorum, config.notFoundOk, config.w, config.dw, returnBody, config.pw, resolver = resolver, mutator = mutator).map(_.map(unwrapRaikuValue))
  }

  /** Stores a List[T] in parallel to the current Raiku bucket
   *
   *  @param objs the to-be stored objects in Riak
   *  @param returnBody whether to return the contents of the stored objects
   *  @param config override the bucket configuration with a custom request-specific configuration
   */

  def storeMany(objs: List[T],
                returnBody: ReturnBodyArgument = ReturnBodyArgument(),
                config: RaikuBucketConfig = config): Future[List[T]] = {
    Future.sequence(objs.map(store(_, returnBody, config))).map(_.flatten)
  }

  /** Deletes a T from the current Raiku bucket
   *
   *  @param obj the to-be deleted object from Riak
   *  @param onlyDeleteIfBefore when a vclock is supplied, only delete the object if the vclock equals or is older
   *  @param config override the bucket configuration with a custom request-specific configuration
   */

  def delete(obj: T,
             onlyDeleteIfBefore: VClockArgument = VClockArgument(),
             config: RaikuBucketConfig = config): Future[Unit] = {
    val converted = converter.write(bucketName, bucketType, obj)
    client.delete(converted, config.rw, onlyDeleteIfBefore, config.r, config.w, config.pr, config.pw, config.dw)
  }

  /** Deletes a object from the current Raiku bucket by key
   *
   *  @param key the key of the to-be deleted object
   *  @param onlyDeleteIfBefore when a vclock is supplied, only delete the object if the vclock equals or is older
   *  @param config override the bucket configuration with a custom request-specific configuration
   */

  def deleteByKey(key: String,
                  onlyDeleteIfBefore: VClockArgument = VClockArgument(),
                  config: RaikuBucketConfig = config): Future[Unit] = {
    client.deleteByKey(bucketName, key, bucketType, config.rw.v, onlyDeleteIfBefore.v, config.r.v, config.w.v, config.pr.v, config.pw.v, config.dw.v)
  }

  /** Deletes a List[T] in parallel from the current Raiku bucket
   *
   *  @param objs the to-be deleted objects
   *  @param config override the bucket configuration with a custom request-specific configuration
   */

  def deleteMany(objs: List[T],
                 config: RaikuBucketConfig = config): Future[List[Unit]] = {
    Future.sequence(objs.map(delete(_, config = config)))
  }

  /** Deletes a set of objects in parallel by key from the current Raiku bucket
   *
   *  @param keys the keys of the to-be deleted objects
   *  @param config override the bucket configuration with a custom request-specific configuration
   */

  def deleteManyByKey(keys: List[String],
                      config: RaikuBucketConfig = config): Future[List[Unit]] = {
    Future.sequence(keys.map(deleteByKey(_, config = config)))
  }

  /** Fetches keys based on a binary index
   *
   *  @param idxk the binary index key
   *  @param idxv the binary index value
   */

  def fetchKeysForBinIndexByValue(idxk: String, idxv: String): Future[List[String]] =
    client.fetchKeysForBinIndexByValue(bucketName, idxk, idxv, bucketType)

  /** Fetches keys based on a integer index
   *
   *  @param idxk the integer index key
   *  @param idxv the integer index value
   */

  def fetchKeysForIntIndexByValue(idxk: String, idxv: Int): Future[List[String]] =
    client.fetchKeysForIntIndexByValue(bucketName, idxk, idxv, bucketType)

  /** Fetches keys based on a ranged binary index
   *
   *  @param idxk the integer index key
   *  @param idxr the ranged integer index value
   */

  def fetchKeysForBinIndexByValueRange(idxk: String, idxr: RaikuStringRange): Future[List[String]] =
    client.fetchKeysForBinIndexByValueRange(bucketName, idxk, idxr, bucketType = bucketType)

  /** Fetches keys based on a ranged integer index
   *
   *  @param idxk the integer index key
   *  @param idxr the ranged integer index value
   */

  def fetchKeysForIntIndexByValueRange(idxk: String, idxr: Range): Future[List[String]] =
    client.fetchKeysForIntIndexByValueRange(bucketName, idxk, idxr, bucketType = bucketType)

  /** Fetches keys based on a binary index, maxed on the number of results
   *
   *  @param idxk the integer index key
   *  @param idxv the binary index value
   *  @param maxResults the maximal number of results to return
   *  @param continuation continutation used for pagination
   */

  def fetchMaxedKeysForBinIndexByValue(idxk: String, idxv: String, maxResults: Int, continuation: Option[String] = None): Future[(Option[String], List[String])] =
    client.fetchMaxedKeysForBinIndexByValue(bucketName, idxk, idxv, maxResults, continuation, bucketType = bucketType)

  /** Fetches keys based on a integer index, maxed on the number of results
   *
   *  @param idxk the integer index key
   *  @param idxv the integer index value
   *  @param maxResults the maximal number of results to return
   *  @param continuation continutation used for pagination
   */

  def fetchMaxedKeysForIntIndexByValue(idxk: String, idxv: Int, maxResults: Int, continuation: Option[String] = None): Future[(Option[String], List[String])] =
    client.fetchMaxedKeysForIntIndexByValue(bucketName, idxk, idxv, maxResults, continuation, bucketType = bucketType)

  /** Fetches keys based on a ranged binary index, maxed on the number of results
   *
   *  @param idxk the integer index key
   *  @param idxr the ranged integer index value
   *  @param maxResults the maximal number of results to return
   *  @param continuation continutation used for pagination
   */

  def fetchMaxedKeysForBinIndexByValueRange(idxk: String, idxr: RaikuStringRange, maxResults: Int, continuation: Option[String] = None): Future[(Option[String], List[String])] =
    client.fetchMaxedKeysForBinIndexByValueRange(bucketName, idxk, idxr, maxResults, continuation, bucketType = bucketType)

  /** Fetches keys based on a ranged integer index, maxed on the number of results
   *
   *  @param idxk the integer index key
   *  @param idxr the ranged integer index value
   *  @param maxResults the maximal number of results to return
   *  @param continuation continutation used for pagination
   */

  def fetchMaxedKeysForIntIndexByValueRange(idxk: String, idxr: Range, maxResults: Int, continuation: Option[String] = None): Future[(Option[String], List[String])] =
    client.fetchMaxedKeysForIntIndexByValueRange(bucketName, idxk, idxr, maxResults, continuation, bucketType = bucketType)

  /** Streams keys based on a binary index
   *
   *  @param idxk the binary index key
   *  @param idxv the binary index value
   */

  def streamKeysForBinIndexByValue(idxk: String, idxv: String): Future[Enumerator[String]] =
    client.streamKeysForBinIndexByValue(bucketName, idxk, idxv, bucketType = bucketType)

  /** Streams keys based on a binary index
   *
   *  @param idxk the binary index key
   *  @param idxv the binary index value
   */

  def streamKeysForIntIndexByValue(idxk: String, idxv: Int): Future[Enumerator[String]] =
    client.streamKeysForIntIndexByValue(bucketName, idxk, idxv, bucketType = bucketType)

  /** Streams keys based on a ranged integer index
   *
   *  @param idxk the integer index key
   *  @param idxv the ranged integer index value
   */

  def streamKeysForIntIndexByValueRange(idxk: String, idxv: Range): Future[Enumerator[String]] =
    client.streamKeysForIntIndexByValueRange(bucketName, idxk, idxv, bucketType = bucketType)

  /** Streams keys based on a ranged binary index
   *
   *  @param idxk the binary index key
   *  @param idxv the ranged binary index value
   */

  def streamKeysForBinIndexByValueRange(idxk: String, idxv: RaikuStringRange): Future[Enumerator[String]] =
    client.streamKeysForBinIndexByValueRange(bucketName, idxk, idxv, bucketType = bucketType)

  /** @see fetch
   */

  def ?(key: String): Future[Option[T]] = fetch(key)

  /** @see fetchMany
   */

  def ?*(keys: List[String]): Future[List[T]] = fetchMany(keys)

  /** @see store
   */

  def <<(obj: T): Future[Option[T]] = store(obj)

  /** @see storeMany
   */

  def <<*(objs: List[T]): Future[List[T]] = storeMany(objs)

  /** @see delete
   */

  def -(obj: T): Future[Unit] = delete(obj)

  /** @see deleteByKey
   */

  def -(key: String): Future[Unit] = deleteByKey(key)

  /** @see deleteMany
   */

  def -*(objs: List[T]): Future[List[Unit]] = deleteMany(objs)

  /** @see fetchKeysForBinIndexByValue
   */

  def idx(idxk: String, idxv: String): Future[List[String]] =
    fetchKeysForBinIndexByValue(idxk, idxv)

  /** @see fetchKeysForIntIndexByValue
   */

  def idx(idxk: String, idxv: Int): Future[List[String]] =
    fetchKeysForIntIndexByValue(idxk, idxv)

  /** @see fetchKeysForBinIndexByValueRange
   */

  def idx(idxk: String, idxv: RaikuStringRange): Future[List[String]] =
    fetchKeysForBinIndexByValueRange(idxk, idxv)

  /** @see fetchKeysForIntIndexByValueRange
   */

  def idx(idxk: String, idxv: Range): Future[List[String]] =
    fetchKeysForIntIndexByValueRange(idxk, idxv)

  /** @see fetchMaxedKeysForBinIndexByValue
   */

  def idx(idxk: String, idxv: String, maxResults: Int): Future[(Option[String], List[String])] =
    fetchMaxedKeysForBinIndexByValue(idxk, idxv, maxResults, None)

  def idx(idxk: String, idxv: String, maxResults: Int, continuation: Option[String]): Future[(Option[String], List[String])] =
    fetchMaxedKeysForBinIndexByValue(idxk, idxv, maxResults, continuation)

  /** @see fetchMaxedKeysForIntIndexByValue
   */

  def idx(idxk: String, idxv: Int, maxResults: Int): Future[(Option[String], List[String])] =
    fetchMaxedKeysForIntIndexByValue(idxk, idxv, maxResults, None)

  def idx(idxk: String, idxv: Int, maxResults: Int, continuation: Option[String]): Future[(Option[String], List[String])] =
    fetchMaxedKeysForIntIndexByValue(idxk, idxv, maxResults, continuation)

  /** @see fetchMaxedKeysForBinIndexByValueRange
   */

  def idx(idxk: String, idxr: RaikuStringRange, maxResults: Int): Future[(Option[String], List[String])] =
    fetchMaxedKeysForBinIndexByValueRange(idxk, idxr, maxResults, None)

  def idx(idxk: String, idxr: RaikuStringRange, maxResults: Int, continuation: Option[String]): Future[(Option[String], List[String])] =
    fetchMaxedKeysForBinIndexByValueRange(idxk, idxr, maxResults, continuation)

  /** @see fetchMaxedKeysForIntIndexByValueRange
   */

  def idx(idxk: String, idxr: Range, maxResults: Int): Future[(Option[String], List[String])] =
    fetchMaxedKeysForIntIndexByValueRange(idxk, idxr, maxResults, None)

  def idx(idxk: String, idxr: Range, maxResults: Int, continuation: Option[String]): Future[(Option[String], List[String])] =
    fetchMaxedKeysForIntIndexByValueRange(idxk, idxr, maxResults, continuation)

  /** @see streamKeysForBinIndexByValue
   */

  def streamIdx(idxk: String, idxv: String): Future[Enumerator[String]] =
    streamKeysForBinIndexByValue(idxk, idxv)

  /** @see streamKeysForIntIndexByValue
   */

  def streamIdx(idxk: String, idxv: Int): Future[Enumerator[String]] =
    streamKeysForIntIndexByValue(idxk, idxv)

  /** @see streamKeysForIntIndexByValueRange
   */

  def streamIdx(idxk: String, idxv: Range): Future[Enumerator[String]] =
    streamKeysForIntIndexByValueRange(idxk, idxv)

  /** @see streamKeysForBinIndexByValueRange
   */

  def streamIdx(idxk: String, idxv: RaikuStringRange): Future[Enumerator[String]] =
    streamKeysForBinIndexByValueRange(idxk, idxv)

  /** Created a new counter
   *
   *  @param key the key of the counter
   */
  //def counter(key: String) = new RaikuCounter(this, key)
}

