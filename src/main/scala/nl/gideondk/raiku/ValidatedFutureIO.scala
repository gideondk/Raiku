package nl.gideondk.raiku

import scalaz._
import Scalaz._
import scalaz.effect._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

final case class ValidatedFuture[A](future: Future[Validation[Throwable, A]]) {
 self =>

 def map[B](f: A => B): ValidatedFuture[B] = ValidatedFuture {
   future map {
     x => x match {
       case Failure(fail) => fail.failure
       case Success(succ) => f(succ).success
     }
   }
 }

 def flatMap[B](f: A => ValidatedFuture[B]): ValidatedFuture[B] = ValidatedFuture {
   future flatMap {
     _ match {
       case Failure(fail) => Future(fail.failure)
       case Success(succ) => f(succ).future
     }
   }
 }
}

object ValidatedFuture {
 def apply[T](a: => Future[T]): ValidatedFuture[T] = ValidatedFuture((a.map(_.success) recover { case t => t.failure }))
}

final case class ValidatedFutureIO[A](io: IO[ValidatedFuture[A]]) {
 self =>

 def map[B](f: A => B): ValidatedFutureIO[B] =
   ValidatedFutureIO(io.map { _ map { f(_) } })

 def flatMap[B](f: A => ValidatedFutureIO[B]): ValidatedFutureIO[B] =
   ValidatedFutureIO { io.flatMap { _.flatMap { f(_).unsafePerformIO }.point[IO] } }

 def fulFill(duration: Duration = 5 seconds): IO[Validation[Throwable, A]] = io.map(x => Await.result(x.future, duration))

 def unsafePerformIO: ValidatedFuture[A] = io.unsafePerformIO

 def unsafeFulFill: Validation[Throwable, A] = unsafeFulFill()

 def unsafeFulFill(duration: Duration = 5 seconds): Validation[Throwable, A] = fulFill(duration).unsafePerformIO

 def !!(duration: Duration = 5 seconds): Validation[Throwable, A] = unsafeFulFill(duration)
}

object ValidatedFutureIO {
 def apply[T](a: => Future[T]): ValidatedFutureIO[T] = ValidatedFutureIO(ValidatedFuture(a).point[IO])

 def sequence[T](z: List[ValidatedFutureIO[T]]): ValidatedFutureIO[List[T]] = 
   ValidatedFutureIO(z.map(_.io).sequence.map(l => Future.sequence(l.map(_.future.map(_.toValidationNEL)))
    .map(_.toList.sequence[({type l[a]=ValidationNEL[Throwable, a]})#l, T])).map(z => ValidatedFuture(z.map(y => (y.bimap(x => x.head, x => x))))))

}