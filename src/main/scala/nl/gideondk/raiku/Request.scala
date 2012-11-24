package nl.gideondk.raiku

import akka.util.ByteString
import java.io.DataOutputStream
import com.basho.riak.protobuf.RiakKvPB._
import com.basho.riak.protobuf.RpbGetReq
import com.google.protobuf.CodedOutputStream
import java.io.ByteArrayOutputStream

import scala.concurrent.Promise
import scala.concurrent.Future
import com.basho.riak.protobuf._
import akka.actor._

import scalaz._
import Scalaz._
import effect._

import com.google.protobuf._

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
    
    errorCheckedValidatedFutureIORiakResponse(ValidatedFutureIO(ioAction.map(x => ValidatedFuture(x.future))))
  }

  def errorCheckedValidatedFutureIORiakResponse(vr: ValidatedFutureIO[RiakResponse]) = {
    ValidatedFutureIO(vr.io.map(z => ValidatedFuture(z.future.map (x => x.flatMap(riakResponseToValidation)))))
  }

  def riakResponseToValidation(resp: RiakResponse): Validation[Throwable, RiakResponse] = {
    if (resp.messageType == 0)
      new Exception(RpbErrorResp().mergeFrom(resp.message.toArray).errmsg).failure
    else
      resp.success
  }
}