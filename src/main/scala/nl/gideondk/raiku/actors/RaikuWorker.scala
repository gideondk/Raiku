package nl.gideondk.raiku.actors

import nl.gideondk.raiku.commands._
import nl.gideondk.sentinel.client.SentinelClient

import akka.io._

import akka.actor.ActorSystem
import akka.util.ByteString
import akka.util.ByteStringBuilder

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

object RaikuWorker {
  val stages = new RiakMessageStage >> new LengthFieldFrame(1024 * 1024 * 200, lengthIncludesHeader = false) // 200mb max

  def apply(host: String, port: Int, numberOfWorkers: Int)(implicit system: ActorSystem) = {
    SentinelClient.randomRouting(host, port, numberOfWorkers, "Raiku")(stages)(system)
  }
}