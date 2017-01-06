package nl.gideondk.raiku.protocol

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ BidiFlow, Framing, Source }
import akka.stream.{ ActorMaterializer, Materializer, OverflowStrategy }
import akka.util.{ ByteString, ByteStringBuilder }
import com.basho.riak.protobuf._
import nl.gideondk.raiku.commands._
import nl.gideondk.sentinel.client.ClientStage.HostUp
import nl.gideondk.sentinel.client.{ Client, Host }
import nl.gideondk.sentinel.pipeline.Resolver
import nl.gideondk.sentinel.protocol.ConsumerAction

import scala.concurrent.ExecutionContext

object RiakMessage {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def deserialize(bs: ByteString): RiakResponse = {
    val bi = bs.iterator
    val messageType = bi.getByte
    val message = bi.toByteString
    RiakResponse(RiakMessageType.intToMessageType(messageType.toInt), message)
  }

  def serialize(m: RiakMessage): ByteString = {
    val bsb = new ByteStringBuilder
    bsb.putByte(RiakMessageType.messageTypeToInt(m.messageType).toByte)
    bsb ++= m.message
    bsb.result
  }

  val flow = BidiFlow.fromFunctions(serialize, deserialize)

  def protocol = flow.atop(Framing.simpleFramingProtocol(1024 * 1024 * 200))
}

object RiakMessageHandler extends Resolver[RiakResponse] {
  def process(implicit mat: Materializer) = {
    case x ⇒
      x.messageType match {
        case _ ⇒ ConsumerAction.AcceptSignal
      }
  }
}

object RaikStreamMessageHandler extends Resolver[RiakResponse] {
  def process(implicit mat: Materializer) = {
    case x ⇒
      x.messageType match {
        case RiakMessageType.RpbIndexResp ⇒
          val idxResp = RpbIndexResp().mergeFrom(x.message.toArray)
          idxResp.done match {
            case None    ⇒ ConsumerAction.ConsumeStreamChunk
            case Some(v) ⇒ ConsumerAction.ConsumeChunkAndEndStream
          }
        case RiakMessageType.RpbErrorResp ⇒
          ConsumerAction.AcceptError

        case _ ⇒
          ConsumerAction.AcceptSignal
      }
  }
}

object RaikuWorker {
  def apply(host: String, port: Int)(implicit system: ActorSystem, mat: ActorMaterializer, ec: ExecutionContext): Client[RiakCommand, RiakResponse] = {
    Client(Source.single(HostUp(Host(host, port))), RiakMessageHandler, false, OverflowStrategy.backpressure, RiakMessage.protocol)
  }
}

object RaikuStreamWorker {
  def apply(host: String, port: Int)(implicit system: ActorSystem, mat: ActorMaterializer, ec: ExecutionContext): Client[RiakCommand, RiakResponse] = {
    Client(Source.single(HostUp(Host(host, port))), RaikStreamMessageHandler, false, OverflowStrategy.backpressure, RiakMessage.protocol)
  }
}