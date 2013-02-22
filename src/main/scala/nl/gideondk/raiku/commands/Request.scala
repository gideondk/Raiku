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

import java.lang.Exception
import play.api.libs.iteratee.Enumerator
import spray.json.JsValue

private[raiku] case class RiakResponse(length: Int, messageType: Int, message: ByteString)

private[raiku] case class RiakOperation(promise: Promise[RiakResponse], command: ByteString)

private[raiku] case class RiakMROperation(promise: Promise[Enumerator[Enumerator[JsValue]]], command: ByteString)

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

trait MRRequest extends Connection with ProtoBufConversion {
  def buildMRRequest(op: ByteString): ValidatedFutureIO[Enumerator[Enumerator[JsValue]]] = {
    val ioAction = {
      val promise = Promise[Enumerator[Enumerator[JsValue]]]()
      actor ! RiakMROperation(promise, op)
      promise
    }.point[IO]

    ValidatedFutureIO(ioAction.map(x ⇒ ValidatedFuture(x.future)))
  }
}