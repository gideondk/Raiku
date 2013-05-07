package nl.gideondk.raiku

import scalaz._
import Scalaz._

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher

import spray.json._

import scala.util.{ Success, Failure }

case class Todos(objectId: String, items: List[String], version: Int = 0)
case class ShoppingItem(name: String)
case class ShoppingCart(objectId: String, items: List[ShoppingItem])

trait TodoMutator {
  def mutatorFun(a: Option[RaikuValue[Todos]], b: RaikuValue[Todos]): RaikuValue[Todos] = {
    a match {
      case Some(x) ⇒
        val version = if (b.version > 0) b.version else x.version + 1
        val items = (x.items ++ b.items).distinct
        val nt = Todos(b.objectId, items, version)
        b.copy(value = Some(nt))
      case None ⇒ b
    }
  }

  val todosMutator = RaikuMutator(mutatorFun)
}

trait ShoppingCartResolver {
  def resolverFun(siblings: Set[RaikuValue[ShoppingCart]]): Option[RaikuValue[ShoppingCart]] =
    siblings.headOption map (x ⇒ x.copy(value = Some(x.value.get.copy(items = siblings.map(_.items).flatten.toList.distinct))))

  val cartResolver = RaikuResolver(resolverFun)
}

class MutatorAndResolverSpec extends Specification with DefaultJsonProtocol with TodoMutator with ShoppingCartResolver {
  "A mutator" should {
    "work correctly without collisions" in {
      val todos = Todos("1", List("Do the lawn", "Do the laundry"), 0)
      val rv = RaikuValue("test", "1", Some(todos), None)
      val res = todosMutator(None, rv)
      res.value.get == todos
    }

    "work correctly in case of collisions" in {
      val todosA = Todos("1", List("Do the lawn", "Do the laundry"), 20)
      val todosB = Todos("1", List("Do the dishes", "Do the laundry"), 0)
      val rvA = RaikuValue("test", "1", Some(todosA), None)
      val rvB = RaikuValue("test", "1", Some(todosB), None)

      val res: Todos = todosMutator(Some(rvA), rvB)
      res.version == 21 && res.items.length == 3
    }
  }

  "A resolver" should {
    "handle siblings correctly" in {
      val cartA = ShoppingCart("1", List(
        ShoppingItem("Wrench"),
        ShoppingItem("Haskell Book"),
        ShoppingItem("Duck Tape")))

      val cartB = ShoppingCart("1", List(
        ShoppingItem("Shovel"),
        ShoppingItem("Haskell Book"),
        ShoppingItem("Axe")))

      val rvA = RaikuValue("test", "1", Some(cartA), None)
      val rvB = RaikuValue("test", "1", Some(cartB), None)

      val res: ShoppingCart = cartResolver(Set(rvA, rvB)).get

      res.items.length == 5
    }
  }
}