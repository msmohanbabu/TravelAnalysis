package com.quantexo.utils

class InvalidCountryException(message: String) extends Exception(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }
}

object MyException {
  def unapply(e: InvalidCountryException): Option[(String,Throwable)] = Some((e.getMessage, e.getCause))
}