package nl.gideondk.raiku.commands

import akka.util.ByteString

import scala.concurrent.Promise
import com.basho.riak.protobuf._
import akka.actor._

import scalaz._
import Scalaz._
import effect._

import nl.gideondk.raiku.serialization.ProtoBufConversion
import nl.gideondk.raiku.monads.{ ValidatedFuture, ValidatedFutureIO }
import nl.gideondk.raiku.actors.{ RiakOperation, RiakResponse }
import java.lang.Exception

trait Connection {
  def system: ActorSystem

  def actor: ActorRef

  implicit val dispatcher = system.dispatcher
}

trait Request extends Connection with ProtoBufConversion {
  def ?>(op: ByteString): ValidatedFutureIO[RiakResponse] = {
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