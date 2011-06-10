package cc.refectorie.user.kedarb.dbalign.fields

import com.mongodb.casbah.Imports._
import collection.mutable.HashSet

/**
 * @author kedarb
 * @since 5/10/11
 */

trait CanHashPhrase {
  def hashKeys(phrase: Seq[String]): HashSet[String]
}

/**
 * A generic interface for a field. Maintains the max segment length.
 */
trait Field {
  /**
   * Name of the field
   */
  def name: String

  /**
   * Column index of the field
   */
  def index: Int

  /**
   * Maximum segment length allowed for the field.
   */
  def maxSegmentLength: Int

  /**
   * Add the value to the field (whether an observed field or an extracted field).
   */
  def addValue(recordClusterId: ObjectId, phrase: Seq[String], isObserved: Boolean): Unit

  /**
   * Gets the ids of neighboring values
   */
  def getNeighborValues(phrase: Seq[String]): HashSet[ObjectId]

  /**
   * Number of values
   */
  def numValues: Int

  /**
   * Get value id by index
   */
  def getValueId(index: Int): ObjectId

  /**
   * Get record cluster ids associated with value id
   */
  def getRecordClusterIdsForValueId(valueId: ObjectId): Seq[ObjectId]

  /**
   * Get value ids for record cluster ids
   */
  def getValueIdForRecordClusterId(recordClusterId: ObjectId): Seq[ObjectId]

  /**
   * Remove all the values
   */
  def clearValues: Unit

  /**
   * Returns the value phrase
   */
  def getValuePhrase(id: ObjectId): Seq[String]

  /**
   * Returns the value phrase at index
   */
  def getValuePhrase(index: Int): Seq[String] = getValuePhrase(getValueId(index))

  /**
   * Whether this field is a key
   */
  def isKey: Boolean

  /**
   * Similarity weight
   */
  def simWeight: Double

  /**
   * Phrase similarity score
   */
  def phraseSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]): Double

  /**
   * Similarity threshold. simScore=sim(srcPhrase, dstPhrase)-threshold
   */
  def simThreshold: Double

  /**
   * Scores a phrase with a given value. Returns value between 0 and 1.
   */
  def simScore(src: ObjectId, dstPhrase: Seq[String]): Double

  /**
   * toString
   */
  override lazy val toString = "%s(%d)[maxLen=%d]".format(name, index, maxSegmentLength)
}
