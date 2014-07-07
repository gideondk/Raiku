package nl.gideondk.raiku

import scala.concurrent.{ ExecutionContext, Future }

case class RaikuCounter(bucketName: String, key: String, client: RaikuClient, config: RaikuBucketConfig)(implicit executionContext: ExecutionContext) {

  /** Fetches a count from the current Raiku bucket
   *
   */

  def getCount: Future[Long] = {
    client.getCount(bucketName, key, Some("counter"), config.r.v, config.pr.v, config.basicQuorum.v, config.notFoundOk.v)
  }

  /** Increments a count from the current Raiku bucket
   *
   *  @param amount the amount to increment the counter to
   *  @param returnValue if the operation should return the new value or not (true by default)
   */

  def incrementCount(amount: Long,
                     returnValue: Boolean = true): Future[Option[Long]] = {
    client.incrementCount(bucketName, key, amount, Some("counter"), config.w.v, config.dw.v, config.pw.v, returnValue)
  }

  def get: Future[Long] = getCount

  def +=(amount: Long): Future[Long] = incrementCount(amount, true) map (_.getOrElse(0l)) // Always returning the long by default, so not treating it as an option here...

  def -=(amount: Long): Future[Long] = incrementCount(-amount, true) map (_.getOrElse(0l)) // Always returning the long by default, so not treating it as an option here...
}

//case class RaikuMap(bucketName: String, key: String, bucketType: Option[String], client: RaikuClient, config: RaikuBucketConfig) {
//
//  val a = scala.collection.mutable.Map[String, Any]()
//
//  a.
//
//  def +=(key: String, value: Any) = ???
//  def -=(key: String) = ???
//}