package com.github.fhackett.azurebench

import upickle.default._

abstract class Quantity[T] {
  protected def valuesImpl: List[T]

  final def values: List[T] = valuesImpl.distinct
}
object Quantity {
  implicit val rBool: Reader[Quantity[Boolean]] = reader[ujson.Value].map {
    case ofBool(q) => q
  }
  implicit val rInt: Reader[Quantity[Int]] = reader[ujson.Value].map {
    case ofInt(q) => q
  }
  implicit val rString: Reader[Quantity[String]] = reader[ujson.Value].map {
    case ofString(q) => q
  }

  private def search[T](value: ujson.Value, fn: PartialFunction[ujson.Value,Quantity[T]]): Option[Quantity[T]] =
    value match {
      case `fn`(result) => Some(result)
      case EnumeratingQuantity(enumeration) =>
        val nested = enumeration.elements.map(fn.lift)
        if(nested.forall(_.nonEmpty)) {
          Some(EnumeratingQuantity(nested.iterator.flatten.flatMap(_.valuesImpl).toList))
        } else {
          None
        }
      case _ => None
    }

  object ofBool {
    def unapply(value: ujson.Value): Option[Quantity[Boolean]] =
      search(value, { case BoolQuantity(q) => q })
  }

  object ofInt {
    def unapply(value: ujson.Value): Option[Quantity[Int]] =
      search(value, {
        case IntQuantity(q) => q
        case IntRange(q) => q
      })
  }

  object ofString {
    def unapply(value: ujson.Value): Option[Quantity[String]] =
      search(value, { case StringQuantity(q) => q })
  }

  object ofAny {
    def unapply(value: ujson.Value): Option[Quantity[String]] =
      search(value, {
        case BoolQuantity(q) => StringQuantity(q.value.toString)
        case IntQuantity(q) => StringQuantity(q.value.toString)
        case IntRange(q) => EnumeratingQuantity(q.values.map(_.toString))
        case StringQuantity(q) => q
      })
  }
}

final case class BoolQuantity(value: Boolean) extends Quantity[Boolean] {
  override def valuesImpl: List[Boolean] = List(value)
}
object BoolQuantity {
  def unapply(value: ujson.Value): Option[BoolQuantity] =
    value match {
      case ujson.Bool(value) => Some(BoolQuantity(value))
      case _ => None
    }
}

final case class StringQuantity(value: String) extends Quantity[String] {
  override def valuesImpl: List[String] = List(value)
}
object StringQuantity {
  def unapply(value: ujson.Value): Option[StringQuantity] =
    value match {
      case ujson.Str(value) => Some(StringQuantity(value))
      case _ => None
    }
}

final case class IntQuantity(value: Int) extends Quantity[Int] {
  override def valuesImpl: List[Int] = List(value)
}
object IntQuantity {
  def unapply(value: ujson.Value): Option[IntQuantity] =
    value match {
      case ujson.Num(value) => Some(IntQuantity(value.toInt))
      case _ => None
    }
}

final case class EnumeratingQuantity[T](elements: List[T]) extends Quantity[T] {
  override def valuesImpl: List[T] = elements
}
object EnumeratingQuantity {
  def unapply(value: ujson.Value): Option[EnumeratingQuantity[ujson.Value]] =
    value match {
      case ujson.Arr(values) => Some(EnumeratingQuantity(values.toList))
      case _ => None
    }
}

final case class IntRange(from: Int, step: Int = 1, to: Int) extends Quantity[Int] {
  override def valuesImpl: List[Int] = Range.inclusive(start = from, end = to, step = step).toList
}
object IntRange {
  implicit val r: Reader[IntRange] = macroR

  def unapply(value: ujson.Value): Option[IntRange] = {
    value match {
      case _: ujson.Obj =>
        Some(read[IntRange](value, trace = true))
      case _ => None
    }
  }
}

final case class AnyQuantity(elements: List[String]) extends Quantity[String] {
  override protected def valuesImpl: List[String] = elements
}
object AnyQuantity {
  implicit val r: Reader[AnyQuantity] = reader[ujson.Value].map {
    case Quantity.ofAny(q) => AnyQuantity(q.values)
  }
}
