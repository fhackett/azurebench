package com.github.fhackett.azurebench

final class Name(prefix: String) {
  def sub(tag: String): Name =
    new Name(s"$prefix-$tag")

  override def toString: String = prefix
}
object Name {
  import scala.language.implicitConversions
  implicit def nameToString(name: Name): String = name.toString
}
