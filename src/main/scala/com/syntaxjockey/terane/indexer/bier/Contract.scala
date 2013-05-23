package com.syntaxjockey.terane.indexer.bier

/**
 *
 */
class Contract(val assertions: Assertion*) {

  def validateContract(contract: Contract): Boolean = {
    true
  }

  def validateEventBefore(event: Event): Boolean = {
    true
  }

  def validateEventAfter(event: Event): Boolean = {
    true
  }

  def finalizeEvent(event: Event): Event = {
    event
  }
}

case class Assertion(fieldname: String, expects: Boolean = false, guarantees: Boolean = true, ephemeral: Boolean = false)
