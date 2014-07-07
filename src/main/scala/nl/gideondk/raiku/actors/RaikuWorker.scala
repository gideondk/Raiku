package nl.gideondk.raiku.actors

import akka.actor.ActorSystem
import akka.io._
import akka.util.{ ByteString, ByteStringBuilder }
import com.basho.riak.protobuf._
import nl.gideondk.raiku.commands._
import nl.gideondk.sentinel._

import scala.concurrent.duration._

class RiakMessageStage extends PipelineStage[PipelineContext, RiakCommand, ByteString, RiakResponse, ByteString] {
  def apply(ctx: PipelineContext) = new PipePair[RiakCommand, ByteString, RiakResponse, ByteString] {
    implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

    override val commandPipeline = {
      msg: RiakCommand ⇒
        val bsb = new ByteStringBuilder
        bsb.putByte(RiakMessageType.messageTypeToInt(msg.messageType).toByte)
        bsb ++= msg.message
        ctx.singleCommand(bsb.result)
    }

    override val eventPipeline = {
      bs: ByteString ⇒
        val bi = bs.iterator
        val messageType = bi.getByte
        val message = bi.toByteString
        ctx.singleEvent(RiakResponse(RiakMessageType.intToMessageType(messageType.toInt), message))
    }
  }
}

object RiakMessageHandler extends Resolver[RiakResponse, RiakCommand] {
  def process = {
    case x ⇒
      x.messageType match {
        case _ ⇒ ConsumerAction.AcceptSignal
      }
  }
}

object RaikStreamMessageHandler extends Resolver[RiakResponse, RiakCommand] {
  def process = {
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

trait RaikuStages {
  val stages = new RiakMessageStage >> new LengthFieldFrame(1024 * 1024 * 200, lengthIncludesHeader = false) // 200mb max
}

object RaikuWorker extends RaikuStages {
  def apply(host: String, port: Int, numberOfWorkers: Int)(implicit system: ActorSystem) = {
    Client.randomRouting(host, port, numberOfWorkers, "Raiku", stages, 5 seconds, RiakMessageHandler, true, 1024 * 8, 1024 * 1024 * 5, 1024 * 1024 * 200)(system) // You really should store things larger than 10Mb ;-)
  }
}

object RaikuStreamWorker extends RaikuStages {
  def apply(host: String, port: Int, numberOfWorkers: Int)(implicit system: ActorSystem) = {
    Client.randomRouting(host, port, numberOfWorkers, "Raiku-Stream", stages, 5 seconds, RaikStreamMessageHandler, false, 1024 * 8, 1024 * 1024 * 5, 1024 * 1024 * 200)(system) // You really should store things larger than 10Mb ;-)
  }
}