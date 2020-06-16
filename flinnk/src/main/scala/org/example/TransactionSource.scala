package org.example

import java.util

import org.apache.flink.annotation.Public
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction


@Public
class TransactionSource(iterator: util.Iterator[Transaction]) extends FromIteratorFunction[Transaction](iterator) {

  def this() = this(RateLimitedIterator.apply(TransactionIterator.unbounded()))

}