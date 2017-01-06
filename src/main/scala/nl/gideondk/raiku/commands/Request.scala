package nl.gideondk.raiku.commands

import java.lang.Exception

import com.basho.riak.protobuf.RpbErrorResp
import akka.actor.{ ActorRef, ActorSystem }
import akka.util.ByteString
import nl.gideondk.raiku.serialization.ProtoBufConversion
import nl.gideondk.sentinel._
import nl.gideondk.sentinel.client.Client

import scala.concurrent.Future
import scala.util.Try
import scala.concurrent._

trait RiakMessage {
  def messageType: RiakMessageType

  def message: ByteString
}

/* Technically the same, but splitting them for readability reasons */
private[raiku] case class RiakCommand(messageType: RiakMessageType, message: ByteString) extends RiakMessage

private[raiku] case class RiakResponse(messageType: RiakMessageType, message: ByteString) extends RiakMessage

trait Connection {
  def system: ActorSystem

  def worker: Client[RiakCommand, RiakResponse]

  def streamWorker: Client[RiakCommand, RiakResponse]

  implicit val dispatcher = system.dispatcher
}

trait Request extends Connection with ProtoBufConversion {
  def buildRequest(messageType: RiakMessageType, message: ByteString): Future[RiakResponse] =
    (worker ask RiakCommand(messageType, message)).flatMap(x ⇒ riakResponseToValidation(x) match {
      case scala.util.Failure(ex) ⇒ Future.failed(ex)
      case scala.util.Success(s)  ⇒ Future(s)
    })

  def buildRequest(messageType: RiakMessageType): Future[RiakResponse] = buildRequest(messageType, ByteString())

  def riakResponseToValidation(resp: RiakResponse): Try[RiakResponse] = {
    if (RiakMessageType.messageTypeToInt(resp.messageType) == 0)
      scala.util.Failure(new Exception(RpbErrorResp().mergeFrom(resp.message.toArray).errmsg))
    else
      scala.util.Success(resp)
  }
}