package nl.gideondk.raiku

import scalaz._
import Scalaz._

import com.basho.riak.protobuf._
import scala.concurrent.ExecutionContext.Implicits.global

trait RaikuConverter[T] {
  type ReadResult[T] = Validation[Throwable, T]
  def read(o: RaikuRWObject): ReadResult[T]
  def write(bucket: String, o: T): RaikuRWObject
}

case class RaikuBucketProperties(nVal: Option[Int], allowMulti: Option[Boolean])

case class RaikuBucketConfig(r: Option[Int] = None, rw: Option[Int] = None, 
	w: Option[Int] = None, pr: Option[Int] = None, pw: Option[Int] = None, dw: Option[Int] = None)

case class RaikuBucket[T](bucketName: String, client: RaikuClient, config: RaikuBucketConfig = RaikuBucketConfig())(implicit converter: RaikuConverter[T]) {
    def fetchBucketProperties =
        client.fetchBucketProperties(bucketName).map(x => RaikuBucketProperties(x.nVal, x.allowMult))
    
    def setBucketProperties(pr: RaikuBucketProperties) = 
        client.setBucketProperties(bucketName, RpbBucketProps(pr.nVal, pr.allowMulti))

	def fetch(key: String, r: Option[Int] = None, pr: Option[Int] = None,
    	basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None,
    	ifModified: Option[String] = None, head: Option[Boolean] = None,
    	deletedVClock: Option[Boolean] = None): ValidatedFutureIO[Option[T]] = {
    	val (nR, pR) = (List(r, config.r).flatten headOption, List(pr, config.pr).flatten headOption)
		client.fetch(bucketName, key, nR, pR, basicQuorum, notFoundOk, ifModified, head, deletedVClock)
    }

    def fetchMany(keys: List[String], r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None,
        ifModified: Option[String] = None, head: Option[Boolean] = None,
        deletedVClock: Option[Boolean] = None): ValidatedFutureIO[List[T]] = {
        ValidatedFutureIO.sequence(keys.map(fetch(_, r, pr, basicQuorum, notFoundOk, ifModified, head, deletedVClock))).map(_.flatten)
    }

    def store(obj: T, r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None, 
        deletedVClock: Option[Boolean] = None, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
    	pw: Option[Int] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None, 
    	returnHead: Option[Boolean] = None): ValidatedFutureIO[Option[T]] = {
        val converted = converter.write(bucketName, obj)
        val (nR, pR) = (List(r, config.r).flatten headOption, List(pr, config.pr).flatten headOption)
        ValidatedFutureIORWListToValidatedFutureIOOptRW(client.fetch(bucketName, converted.key, nR, pR, basicQuorum, notFoundOk, deletedvclock = deletedVClock))
            .flatMap { fetchObj: Option[RaikuRWObject] => 
                val (nW, nDw, nPw) = (List(w, config.w).flatten headOption, List(dw, config.dw).flatten headOption, List(pw, config.pw).flatten headOption)
                // Clobber converter as default
                val storeObj = converter.write(bucketName, obj).copy(vClock = fetchObj.map(_.vClock).getOrElse(None))
                client.store(storeObj, nW, nDw, returnBody, nPw, ifNotModified, ifNonMatched, returnHead)
        }
    }

    /* 
     * Faster then the "normal" store, because object won't be fetched first to determine the vclock.
     * Only to be used when you are *absolutely* sure this object doesn't already exists.
     *
     * Some parameters (like ifNonModified, ifNonMatched) won't make sense for this purpose, 
     * leaving them here if you wan't to misuse this function for other purposes.
     *
     * Not integrated in DSL, unsafe should read unsafe ;-)
    */

    def unsafeStoreNew(obj: T, r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None, 
        deletedVClock: Option[Boolean] = None, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
        pw: Option[Int] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None, 
        returnHead: Option[Boolean] = None): ValidatedFutureIO[Option[T]] = {
        val (nW, nDw, nPw) = (List(w, config.w).flatten headOption, List(dw, config.dw).flatten headOption, List(pw, config.pw).flatten headOption)
        client.store(converter.write(bucketName, obj), nW, nDw, returnBody, nPw, ifNotModified, ifNonMatched, returnHead)
    }

    def storeMany(objs: List[T], r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None, 
        deletedVClock: Option[Boolean] = None, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
        pw: Option[Int] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None, 
        returnHead: Option[Boolean] = None): ValidatedFutureIO[List[T]] = {
        ValidatedFutureIO.sequence(objs.map(store(_, r, pr, basicQuorum, notFoundOk, deletedVClock, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead))).map(_.flatten)
    }

    def unsafeStoreManyNew(objs: List[T], r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None, 
        deletedVClock: Option[Boolean] = None, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
        pw: Option[Int] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None, 
        returnHead: Option[Boolean] = None): ValidatedFutureIO[List[T]] = {
        ValidatedFutureIO.sequence(objs.map(unsafeStoreNew(_, r, pr, basicQuorum, notFoundOk, deletedVClock, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead))).map(_.flatten)
    }

    def delete(obj: T, rw: Option[Int] = None, r: Option[Int] = None, w: Option[Int] = None, pr: Option[Int] = None, 
        pw: Option[Int] = None, dw: Option[Int] = None): ValidatedFutureIO[Unit] = {
        val (nRw, nR, nW, nPr, nPw, nDw) = (List(rw, config.rw).flatten headOption, List(r, config.r).flatten headOption, List(w, config.w).flatten headOption,
            List(pr, config.pr).flatten headOption, List(pw, config.pw).flatten headOption, List(dw, config.dw).flatten headOption)
        client.delete(converter.write(bucketName, obj), nRw, nR, nW, nPr, nPw, nDw)
    }

    def deleteMany(objs: List[T], rw: Option[Int] = None, r: Option[Int] = None, w: Option[Int] = None, pr: Option[Int] = None, 
        pw: Option[Int] = None, dw: Option[Int] = None): ValidatedFutureIO[List[Unit]] = {
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

    def ?(key: String, r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None,
        ifModified: Option[String] = None, head: Option[Boolean] = None,
        deletedVClock: Option[Boolean] = None): ValidatedFutureIO[Option[T]] =  
        fetch(key, r, pr, basicQuorum, notFoundOk, ifModified, head, deletedVClock)

    def ?*(keys: List[String], r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None,
        ifModified: Option[String] = None, head: Option[Boolean] = None,
        deletedVClock: Option[Boolean] = None): ValidatedFutureIO[List[T]] =
        fetchMany(keys, r, pr, basicQuorum, notFoundOk, ifModified, head, deletedVClock)

    def <<(obj: T, r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None, 
        deletedVClock: Option[Boolean] = None, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
        pw: Option[Int] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None, 
        returnHead: Option[Boolean] = None): ValidatedFutureIO[Option[T]] =
        store(obj, r, pr, basicQuorum, notFoundOk, deletedVClock, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead)

    def <<*(objs: List[T], r: Option[Int] = None, pr: Option[Int] = None,
        basicQuorum: Option[Boolean] = None, notFoundOk: Option[Boolean] = None, 
        deletedVClock: Option[Boolean] = None, w: Option[Int] = None, dw: Option[Int] = None, returnBody: Option[Boolean] = None,
        pw: Option[Int] = None, ifNotModified: Option[Boolean] = None, ifNonMatched: Option[Boolean] = None, 
        returnHead: Option[Boolean] = None): ValidatedFutureIO[List[T]] =
        storeMany(objs, r, pr, basicQuorum, notFoundOk, deletedVClock, w, dw, returnBody, pw, ifNotModified, ifNonMatched, returnHead)

    def -(obj: T, rw: Option[Int] = None, r: Option[Int] = None, w: Option[Int] = None, pr: Option[Int] = None, 
        pw: Option[Int] = None, dw: Option[Int] = None): ValidatedFutureIO[Unit] =
        delete(obj, rw, r, w, pr, pw, dw)

    def -*(objs: List[T], rw: Option[Int] = None, r: Option[Int] = None, w: Option[Int] = None, pr: Option[Int] = None, 
        pw: Option[Int] = None, dw: Option[Int] = None): ValidatedFutureIO[List[Unit]] =
        deleteMany(objs, rw, r, w, pr, pw, dw)

    def idx(idxk: String, idxv: String): ValidatedFutureIO[List[String]] = 
        fetchKeysForBinIndexByValue(idxk, idxv)

    def idx(idxk: String, idxv: Int): ValidatedFutureIO[List[String]] = 
        fetchKeysForIntIndexByValue(idxk, idxv)

    def idx(idxk: String, idxv: Range): ValidatedFutureIO[List[String]] = 
        fetchKeysForIntIndexByValueRange(idxk, idxv)
}