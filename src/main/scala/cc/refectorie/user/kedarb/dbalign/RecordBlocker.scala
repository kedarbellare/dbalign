package cc.refectorie.user.kedarb.dbalign

import fields.Field
import cc.refectorie.user.kedarb.dynprog.segment.Segmentation
import cc.refectorie.user.kedarb.dynprog.utils.Utils._
import com.mongodb.casbah.Imports._
import collection.mutable.{HashSet, HashMap}

/**
 * @author kedarb
 * @since 4/29/11
 */

class RecordBlocker(val fields: Seq[Field], val words: Array[String], val possibleEnds: Array[Boolean],
                    val trueSegmentation: Segmentation, val trueInfer: Boolean) {
  val L = fields.size

  val N = words.size

  def getFieldsToRecordClusterIds(lset: Set[Int]): HashMap[Int, HashSet[ObjectId]] = {
    val keyFieldsToNbrValueIds = new HashMap[Int, HashSet[ObjectId]]
    for (l <- lset.iterator) keyFieldsToNbrValueIds(l) = new HashSet[ObjectId]
    // lookup segments in fields via inverted index and
    // add record clusters containing the neighbor value id
    def addNbrValueIdsForKeyField(l: Int, begin: Int, end: Int) {
      val fld = fields(l)
      val phrase = words.slice(begin, end)
      // only add record cluster ids of neighbor value ids where score >= 0
      val neighborValueIds = fld.getNeighborValues(phrase)
      // println("phrase[" + begin + ", " + end + "): " + phrase.mkString(" ") + " #nbrs=" + neighborValueIds.size)
      neighborValueIds.foreach {
        nbrValueId: ObjectId =>
        // TODO: ignore similarity?
        // if (!keyFieldsToNbrValueIds(l)(nbrValueId) && fld.simScore(nbrValueId, phrase) > fld.simThreshold)
          keyFieldsToNbrValueIds(l) += nbrValueId
      }
    }
    if (trueInfer) {
      // iterate over only true segments
      forIndex(trueSegmentation.numSegments, (s: Int) => {
        val segment = trueSegmentation.segment(s)
        if (lset(segment.label)) addNbrValueIdsForKeyField(segment.label, segment.begin, segment.end)
      })
    } else {
      for (l <- lset.iterator) {
        // iterate over all possible segments
        forIndex(N, (begin: Int) => {
          forIndex(begin + 1, math.min(begin + fields(l).maxSegmentLength, N) + 1, (end: Int) => {
            if (possibleEnds(end)) addNbrValueIdsForKeyField(l, begin, end)
          })
        })
      }
    }
    // initialize the key fields to records to empty sets
    val keyFieldsToRecordIds = new HashMap[Int, HashSet[ObjectId]]
    for (l <- lset.iterator) keyFieldsToRecordIds(l) = new HashSet[ObjectId]
    for (l <- lset.iterator; nbrValueId <- keyFieldsToNbrValueIds(l))
      keyFieldsToRecordIds(l) ++= fields(l).getRecordClusterIdsForValueId(nbrValueId)
    keyFieldsToRecordIds
  }

  def getKeyFieldsToRecordClusterIds: HashMap[Int, HashSet[ObjectId]] = {
    getFieldsToRecordClusterIds((for (l <- 0 until L if fields(l).isKey) yield l).toSet)
  }

  /**
   * Returns possible record cluster ids.
   * If join then take records that match on all key fields else take union.
   */
  def getPossibleRecordClusterIds(join: Boolean): HashSet[ObjectId] = {
    val keyFieldsToRecordIds = getKeyFieldsToRecordClusterIds
    val possibleRecordClusterIds = new HashSet[ObjectId]
    if (join) {
      // only take those record ids that match on all key fields
      require(keyFieldsToRecordIds.size > 0, "No key fields found!")
      val firstKeyFieldClusterIds = keyFieldsToRecordIds.values.head
      for (recordClusterId <- firstKeyFieldClusterIds) {
        var presentInAll = true
        for (otherRecordClusterIdSet <- keyFieldsToRecordIds.values) {
          if (!otherRecordClusterIdSet(recordClusterId)) presentInAll = false
        }
        if (presentInAll) possibleRecordClusterIds += recordClusterId
      }
    } else {
      for (otherRecordClusterIdSet <- keyFieldsToRecordIds.values) {
        possibleRecordClusterIds ++= otherRecordClusterIdSet
      }
    }
    possibleRecordClusterIds
  }

  def outputPossibleRecordClusterIds(possibleRecordClusterIds: HashSet[ObjectId]): Unit = {
    println
    println("For words: " + words.mkString(" ") + " #matches=" + possibleRecordClusterIds.size)
    possibleRecordClusterIds.foreach {
      recordClusterId: ObjectId =>
        println("\t**** Matched r=" + recordClusterId)
        for (fld <- fields if (fld.isKey)) {
          fld.getValueIdForRecordClusterId(recordClusterId).foreach {
            fieldId: ObjectId => println("\tf[" + fld + "]=" + fieldId + ": " +
              fld.getValuePhrase(fieldId).mkString(" "))
          }
        }
    }
  }
}