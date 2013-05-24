package nl.gideondk.raiku.commands

import java.lang.Exception
import com.basho.riak.protobuf.RpbErrorResp
import akka.actor.{ ActorRef, ActorSystem }
import akka.util.ByteString
import nl.gideondk.raiku.serialization.ProtoBufConversion
import nl.gideondk.sentinel.client._
import nl.gideondk.sentinel.Task
import scala.util.Try

trait RiakMessage {
  def messageType: RiakMessageType

  def message: ByteString
}

/* Technically the same, but splitting them for readability reasons */
private[raiku] case class RiakCommand(messageType: RiakMessageType, message: ByteString) extends RiakMessage

private[raiku] case class RiakResponse(messageType: RiakMessageType, message: ByteString) extends RiakMessage

trait Connection {
  def system: ActorSystem

  def worker: ActorRef

  implicit val dispatcher = system.dispatcher
}

trait Request extends Connection with ProtoBufConversion {
  def buildRequest(messageType: RiakMessageType, message: ByteString): Task[RiakResponse] =
    Task(worker.sendCommand[RiakResponse, RiakCommand](RiakCommand(messageType, message)).get.map(_.map(_.flatMap(riakResponseToValidation))))

  def buildRequest(messageType: RiakMessageType): Task[RiakMessage] = buildRequest(messageType, ByteString())

  def riakResponseToValidation(resp: RiakResponse): Try[RiakResponse] = {
    if (RiakMessageType.messageTypeToInt(resp.messageType) == 0)
      scala.util.Failure(new Exception(RpbErrorResp().mergeFrom(resp.message.toArray).errmsg))
    else
      scala.util.Success(resp)
  }
}