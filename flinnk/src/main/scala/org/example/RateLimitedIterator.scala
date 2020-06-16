package org.example

import java.io.Serializable
import java.util

object RateLimitedIterator {
  def apply(inner: util.Iterator[Transaction]): RateLimitedIterator[Transaction] = new RateLimitedIterator(inner)
}

@SerialVersionUID(1L)
class RateLimitedIterator[T] private(val inner: util.Iterator[T]) extends util.Iterator[T] with Serializable {
  override def hasNext: Boolean = inner.hasNext

  override def next: T = {
    try Thread.sleep(100)
    catch {
      case e: InterruptedException =>
        throw new RuntimeException(e)
    }
    inner.next
  }
}
