package org.example

import scala.util.hashing.MurmurHash3

case class CDRData(accountId: String, count: Integer, start: Long, end: Long) {

  val windowHash = {
    MurmurHash3.stringHash(accountId.concat(start.toString).concat(end.toString))
  }

  override def toString: String = {
    String.format("Key: %s, Count: %d", accountId, count)
  }
}
