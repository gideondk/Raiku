package nl.gideondk.raiku.commands

import java.lang.Exception

import scala.concurrent.Promise

import com.basho.riak.protobuf.RpbErrorResp

import akka.actor.{ ActorRef, ActorSystem, actorRef2Scala }
import akka.util.ByteString

import scalaz._
import scalaz.Scalaz._
import scalaz.effect.IO

import nl.gideondk.raiku.monads.{ ValidatedFuture, ValidatedFutureIO }
import nl.gideondk.raiku.serialization.ProtoBufConversion

private[raiku] case class RiakResponse(length: Int, messageType: Int, message: ByteString)

private[raiku] case class RiakOperation(promise: Promise[RiakResponse], command: ByteString)

trait Connection {
  def system: ActorSystem

  def actor: ActorRef

  implicit val dispatcher = system.dispatcher
}

trait Request extends Connection with ProtoBufConversion {
  def buildRequest(op: ByteString): ValidatedFutureIO[RiakResponse] = {
    val ioAction = {
      val promise = Promise[RiakResponse]()
      actor ! RiakOperation(promise, op)
      promise
    }.point[IO]

    errorCheckedValidatedFutureIORiakResponse(ValidatedFutureIO(ioAction.map(x ⇒ ValidatedFuture(x.future))))
  }

  def errorCheckedValidatedFutureIORiakResponse(vr: ValidatedFutureIO[RiakResponse]) = {
    ValidatedFutureIO(vr.run.map(z ⇒ ValidatedFuture(z.run.map(x ⇒ x.flatMap(riakResponseToValidation)))))
  }

  def riakResponseToValidation(resp: RiakResponse): Validation[Throwable, RiakResponse] = {
    if (resp.messageType == 0)
      new Exception(RpbErrorResp().mergeFrom(resp.message.toArray).errmsg).failure
    else
      resp.success
  }
}