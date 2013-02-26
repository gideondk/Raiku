package nl.gideondk.raiku

import scalaz._
import Scalaz._

import nl.gideondk.raiku.commands.RWObject

trait RaikuConverter[T] {
  type ReadResult[T] = Validation[Throwable, T]

  def read(o: RWObject): ReadResult[T]

  def write(bucket: String, o: T): RWObject
}