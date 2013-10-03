package edb.catalog

class EdbException(message: String, cause: Throwable)
extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}
