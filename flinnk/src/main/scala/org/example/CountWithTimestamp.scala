package org.example

case class CountWithTimestamp(key: String, count: Int, currentProcessingTime: Long) {
  override def toString: String = {
    val text = "Key: %s, Count: %d "
    text.format(this.key, this.count)
  }
}
