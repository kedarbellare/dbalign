package cc.refectorie.user.kedarb.dbalign.fields

import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dynprog.types.Indexer
import collection.mutable.{ArrayBuffer, HashSet, HashMap}

/**
 * @author kedarb
 * @since 3/26/11
 */

trait HashMapPersistentField extends Field with CanHashPhrase {
  // value id -> record cluster ids
  val value2recordClusters = new HashMap[ObjectId, Seq[ObjectId]]
  // record cluster id -> value ids
  val recordClusters2Values = new HashMap[ObjectId, Seq[ObjectId]]
  // ordered ids
  val valueIds = new ArrayBuffer[ObjectId]
  // id -> value store
  val id2phrases = new HashMap[ObjectId, Seq[Int]]
  // inverted index for reverse lookup
  val hash2ids = new HashMap[String, HashSet[ObjectId]]
  // max segment length
  var maxSegmentLength = 0

  def numValues: Int = valueIds.size

  def getValueId(index: Int) = valueIds(index)

  def wordIndexer: Indexer[String]

  def getValuePhrase(id: ObjectId): Seq[String] = id2phrases(id).map(wordIndexer(_))

  def addValue(recordClusterId: ObjectId, phrase: Seq[String], isObserved: Boolean): Unit = {
    val id = new ObjectId
    // increase max segment length only if observed value
    if (isObserved && maxSegmentLength < phrase.size)
      maxSegmentLength = phrase.size
    // add value
    valueIds += id
    id2phrases += id -> phrase.map(wordIndexer.indexOf_!(_))
    value2recordClusters(id) = value2recordClusters.getOrElse(id, Seq.empty) ++ Seq(recordClusterId)
    recordClusters2Values(recordClusterId) = recordClusters2Values.getOrElse(recordClusterId, Seq.empty) ++ Seq(id)
    // add inverted map
    for (key <- hashKeys(phrase)) {
      if (!hash2ids.contains(key)) hash2ids += key -> new HashSet[ObjectId]
      hash2ids(key) += id
    }
  }

  def getValueIdForRecordClusterId(clusterId: ObjectId): Seq[ObjectId] =
    recordClusters2Values.getOrElse(clusterId, Seq.empty)

  def getRecordClusterIdsForValueId(valueId: ObjectId): Seq[ObjectId] =
    value2recordClusters.getOrElse(valueId, Seq.empty)

  def clearValues: Unit = {
    valueIds.clear
    id2phrases.clear
    value2recordClusters.clear
    recordClusters2Values.clear
    hash2ids.clear
  }

  def getNeighborValues(phrase: Seq[String]): HashSet[ObjectId] = {
    val neighbors = new HashSet[ObjectId]
    for (key <- hashKeys(phrase)) {
      if (hash2ids.contains(key))
        neighbors ++= hash2ids(key)
    }
    neighbors
  }

  def simScore(src: ObjectId, dstPhrase: Seq[String]): Double = {
    require(id2phrases.contains(src), "value(" + name + ")[" + src + "] not found!")
    phraseSimScore(getValuePhrase(src), dstPhrase)
  }
}

class DefaultHashMapPersistentField(val name: String, val index: Int, val wordIndexer: Indexer[String],
                                    val isKey: Boolean, val simThreshold: Double, val simWeight: Double,
                                    val _hashKeys: (Seq[String]) => HashSet[String] = PhraseHash.unigramWordHash,
                                    val _phraseSimScore: (Seq[String], Seq[String]) => Double = PhraseSimFunc.cosineSimScore)
  extends HashMapPersistentField {
  def hashKeys(phrase: Seq[String]): HashSet[String] = _hashKeys(phrase)

  def phraseSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]): Double = _phraseSimScore(srcPhrase, dstPhrase)
}