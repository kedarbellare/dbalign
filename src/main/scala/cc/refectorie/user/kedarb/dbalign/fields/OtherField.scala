package cc.refectorie.user.kedarb.dbalign.fields

import com.mongodb.casbah.Imports._
import collection.mutable.HashSet

/**
 * @author kedarb
 * @since 3/26/11
 */

case class OtherField(name: String, index: Int) extends Field {
  var maxSegmentLength = 0
  val defaultIdSet = new HashSet[ObjectId]
  val numValues = 0
  val simThreshold = 0.0
  val isKey = false
  val simWeight = 0.0

  def getValuePhrase(id: ObjectId): Seq[String] = Seq.empty[String]

  def getValueId(index: Int) = throw new RuntimeException("Other field has no values!")

  def getValueIdForRecordClusterId(recordClusterId: ObjectId): Seq[ObjectId] = Seq.empty[ObjectId]

  def getRecordClusterIdsForValueId(valueId: ObjectId): Seq[ObjectId] = Seq.empty[ObjectId]

  def addValue(recordClusterId: ObjectId, phrase: Seq[String], isObserved: Boolean) = {
    if (isObserved && phrase.size > maxSegmentLength) maxSegmentLength = phrase.size
  }

  def phraseSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]): Double = 0.0

  def simScore(src: ObjectId, dstPhrase: Seq[String]) = 0.0

  def clearValues: Unit = {}

  def getNeighborValues(phrase: Seq[String]) = defaultIdSet
}
