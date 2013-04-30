package nl.gideondk.raiku.commands

import java.lang.Exception
import scala.concurrent.Promise
import com.basho.riak.protobuf.RpbErrorResp
import akka.actor.{ ActorRef, ActorSystem, actorRef2Scala }
import akka.util.ByteString
import scalaz._
import Scalaz._
import scalaz.effect.IO
import nl.gideondk.raiku.monads.{ ValidatedFuture, ValidatedFutureIO }
import nl.gideondk.raiku.serialization.ProtoBufConversion
import nl.gideondk.sentinel.client._
import nl.gideondk.sentinel.Task
import nl.gideondk.sentinel.Task._
import scala.util.Try

trait RiakMessage {
  def messageType: RiakMessageType
  def message: ByteString
}

/* Technically the same, but splitting them for readability reasons */
private[raiku] case class RiakCommand(messageType: RiakMessageType, message: ByteString) extends RiakMessage
private[raiku] case class RiakResponse(messageType: RiakMessageType, message: ByteString) extends RiakMessage

//private[raiku] case class RiakResponse(messageType: Int, message: ByteString)
//
//private[raiku] case class RiakOperation(messageType: RiakMessageType, message: ByteString)

trait Connection {
  def system: ActorSystem

  def actor: ActorRef

  implicit val dispatcher = system.dispatcher
}

trait Request extends Connection with ProtoBufConversion {
  def buildRequest(messageType: RiakMessageType, message: ByteString): Task[RiakResponse] =
    Task(actor.sendCommand[RiakResponse, RiakCommand](RiakCommand(messageType, message)).get.map(_.map(_.flatMap(riakResponseToValidation))))

  def buildRequest(messageType: RiakMessageType): Task[RiakMessage] = buildRequest(messageType, ByteString())

  def riakResponseToValidation(resp: RiakResponse): Try[RiakResponse] = {
    if (resp.messageType == 0)
      scala.util.Failure(new Exception(RpbErrorResp().mergeFrom(resp.message.toArray).errmsg))
    else
      scala.util.Success(resp)
  }
}