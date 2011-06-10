package cc.refectorie.user.kedarb.dbalign.fields

import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dbalign.misc.KnowledgeBase
import collection.mutable.{HashMap, HashSet, ArrayBuffer}

/**
 * @author kedarb
 * @since 5/28/11
 */

trait MongoHashMapPersistentField extends Field with CanHashPhrase {
  // max segment length
  var maxSegmentLength = 0
  // value id -> record cluster ids
  val value2recordClusters = new HashMap[ObjectId, Seq[ObjectId]]
  // record cluster id -> value ids
  val recordClusters2Values = new HashMap[ObjectId, Seq[ObjectId]]
  // inverted index for reverse lookup
  val hash2ids = new HashMap[String, HashSet[ObjectId]]

  def kb: KnowledgeBase

  def fieldColl: MongoCollection

  def addValue(recordClusterId: ObjectId, phrase: Seq[String], isObserved: Boolean): Unit = {
    val id = new ObjectId
    if (isObserved && maxSegmentLength < phrase.size) maxSegmentLength = phrase.size
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> id
    // add words to the indexer
    builder += "phrase" -> phrase.toArray
    value2recordClusters(id) = value2recordClusters.getOrElse(id, Seq.empty) ++ Seq(recordClusterId)
    recordClusters2Values(recordClusterId) = recordClusters2Values.getOrElse(recordClusterId, Seq.empty) ++ Seq(id)
    // add inverted map
    for (key <- hashKeys(phrase)) {
      if (!hash2ids.contains(key)) hash2ids += key -> new HashSet[ObjectId]
      hash2ids(key) += id
    }
    fieldColl += builder.result
  }

  def numValues: Int = fieldColl.count.toInt

  def getValueId(index: Int): ObjectId = fieldColl.find().skip(index).limit(1).next._id.get

  def clearValues: Unit = {
    fieldColl.drop
    value2recordClusters.clear
    recordClusters2Values.clear
    hash2ids.clear
  }

  def getValuePhrase(id: ObjectId): Seq[String] = {
    val dbo = fieldColl.findOneByID(id).get
    dbo.as[BasicDBList]("phrase").toSeq.map(_.toString)
  }

  def getValueIdForRecordClusterId(clusterId: ObjectId): Seq[ObjectId] =
    recordClusters2Values.getOrElse(clusterId, Seq.empty)

  def getRecordClusterIdsForValueId(valueId: ObjectId): Seq[ObjectId] =
    value2recordClusters.getOrElse(valueId, Seq.empty)

  def getNeighborValues(phrase: Seq[String]): HashSet[ObjectId] = {
    val neighbors = new HashSet[ObjectId]
    for (key <- hashKeys(phrase)) {
      if (hash2ids.contains(key))
        neighbors ++= hash2ids(key)
    }
    neighbors
  }

  def simScore(src: ObjectId, dstPhrase: Seq[String]): Double = {
    val dboOpt = fieldColl.findOneByID(src)
    require(dboOpt.isDefined, "value(" + name + ")[" + src + "] not found!")
    phraseSimScore(getValuePhrase(src), dstPhrase)
  }
}

class DefaultMongoHashMapPersistentField(val kb: KnowledgeBase, val name: String, val index: Int, val isKey: Boolean,
                                         val simThreshold: Double, val simWeight: Double,
                                         val _hashKeys: (Seq[String]) => HashSet[String] = PhraseHash.unigramWordHash,
                                         val _phraseSimScore: (Seq[String], Seq[String]) => Double = PhraseSimFunc.cosineSimScore)
  extends MongoHashMapPersistentField {
  // initially clear values
  val fieldColl = {
    val coll = kb.getColl(name)
    coll.drop
    coll
  }

  def hashKeys(phrase: Seq[String]): HashSet[String] = _hashKeys(phrase)

  def phraseSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]): Double = _phraseSimScore(srcPhrase, dstPhrase)
}