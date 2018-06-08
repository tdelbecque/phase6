package com.sodad.abstracts

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import Aliases._

case class WeightedTermRecord (pii: String, weights: HashMap[String, Double])
case class SparseVectorRecord (pii: String, v: Vector)
case class CreateWeightedDataSetRecord (numDocs: Long,
  docFreqsMap: Map[String, Int],
  data: Dataset[WeightedTermRecord])
case class CreateWeightedVectorDataSetRecord (
  idToTerm: List[String],
  termToId: Map[String, Int],
  data: Dataset[SparseVectorRecord])

object LSI {
  val stemmer = new Stemmer

  def termDocWeight (
    termFreqInDoc: Int,
    totalTermsInDoc: Int,
    termFreqInCorpus: Int,
    totalDocs: Int) = 
  {
    val tf = termFreqInDoc.toDouble / totalTermsInDoc
    val docFreq = totalDocs.toDouble / termFreqInCorpus
    val idf = math.log (docFreq)
    tf * idf
  }

  def createsTermFreqsDataSet (
    data: OrigDataSet,
    stopWords: Set[String] = Set.empty[String],
    minTermLength: Int = 3)
    (implicit session: SparkSession) : Dataset[BoWRecord] =
  {
    import session.implicits._
    val tokenPattern = s"\\b[a-z]{$minTermLength,}\\b".r
    data mapPartitions { it =>
      it map { r =>
        val stemmer = new Stemmer
        val termFreqs = tokenPattern.findAllIn (r.abstr toLowerCase).
          toList.
          map (stemmer.apply).
          filter (t => ! stopWords (t)).
          foldLeft (new HashMap[String, Int]) { (map, term) =>
            map += term -> (map.getOrElse (term, 0) + 1)
          }.toMap
        BoWRecord (r.pii, termFreqs)
      }
    }
  }


  def createDocFreqsMap (data: Dataset[BoWRecord]) 
  (implicit session: SparkSession) : Map[String, Int] = {
    import session.implicits._
    data.flatMap (_.counts.keys.toList).
      groupBy ("value").count.
      collect.map (x => (x getString 0, x.getLong (1).toInt)).
      toMap
  }

  def createWeightedDataSet (data: Dataset[BoWRecord], maybeSavePath: Option[String]) 
    (implicit session: SparkSession) : CreateWeightedDataSetRecord = {
    import session.implicits._
    val numDocs = data.count.toInt
    val docFreqsMap: Map[String, Int] = createDocFreqsMap (data)
    val weightedDataSet: Dataset[WeightedTermRecord] = data map { r =>
      val nTermsInDoc = r.counts.foldLeft (0) { case (n, (_, c)) => n + c }
      WeightedTermRecord ( r.pii,
        r.counts.foldLeft (new HashMap[String, Double]) { case (m, (t, c)) =>
          m += t -> termDocWeight (c, nTermsInDoc, docFreqsMap (t), numDocs)
        })
    }
    maybeSavePath foreach { path =>
      weightedDataSet.write.option ("path", path) save
    }
    CreateWeightedDataSetRecord (numDocs, docFreqsMap, weightedDataSet)
  }

  @deprecated("", "")
  def createWeightedDataSet (implicit session: SparkSession) = {
    import session.implicits._
    val stopwords = Control.usingFileLines ("stopwords")().toSet
    val abstracts = HuguesAbstracts.readOrigData
    val termFreqsDS : Dataset[BoWRecord] = 
      LSI.createsTermFreqsDataSet (abstracts, stopwords) cache
    val docFreqsMap: Map[String, Int] = createDocFreqsMap (termFreqsDS)
    val numDocs = termFreqsDS.count.toInt
    ( numDocs,
      docFreqsMap,
      termFreqsDS map { r =>
        val nTermsInDoc = r.counts.foldLeft (0) { case (n, (_, c)) => n + c }
        WeightedTermRecord ( r.pii,
          r.counts.foldLeft (new HashMap[String, Double]) { case (m, (t, c)) =>
            val weight = termDocWeight (c, nTermsInDoc, docFreqsMap (t), numDocs)
            m += t -> weight
          })
      })
  }

  def createWeightedVectorDataSet (
    data: Dataset[WeightedTermRecord], 
    docFreqs: Map[String, Int], 
    nToKeep: Int, 
    stopWords: Set[String] = Set.empty[String],
    maybeSavePath: Option[String] = None
  )
    (implicit session: SparkSession) : CreateWeightedVectorDataSetRecord = {
    import session.implicits._
    val idToTerm = docFreqs.toList.
      filter (t => ! stopWords (t._1)).
      sortWith ((a, b) => a._2 > b._2).
      slice (0, nToKeep).
      map (_._1)

    val termToId = idToTerm.zipWithIndex.toMap

    val bTermIds = session.sparkContext.broadcast (termToId)
    val vectorsDataSet : Dataset[SparseVectorRecord] = data.map { r =>
      val v = r.weights.filter { case (t, w) => bTermIds.value containsKey t }.
        map { case (t, w) => (bTermIds.value (t), w) }.
        toSeq
      SparseVectorRecord (r.pii, Vectors.sparse (nToKeep, v))
    }
    maybeSavePath foreach { path =>
      vectorsDataSet.write.option("path", path) save
    }
    CreateWeightedVectorDataSetRecord (
      idToTerm, termToId,
      vectorsDataSet
    )
  }

  @deprecated("", "")
  def createWeightedVectorDataSet (nToKeep: Int) 
    (implicit session: SparkSession) : CreateWeightedVectorDataSetRecord = {
    val x = createWeightedDataSet
    createWeightedVectorDataSet (x._3, x._2, nToKeep)
  }

  def normalizeRows (m: DenseMatrix) : DenseMatrix = {
    assert (m.numRows < Int.MaxValue / m.numCols)
    val buffer = new Array[Double](m.numRows * m.numCols)
    var offset : Int = 0
    for (r <- m.rowIter) {
      var s2 = 0.0
      for (i <- 0 until r.size) s2 += r(i)*r(i)
      val s = math.sqrt (s2)
      for (i <- 0 until r.size) buffer (offset + i) = r(i)/s
      offset += r.size
    }
    new DenseMatrix (m.numRows, m.numCols, buffer, isTransposed=true)
  }

  def computeLSI (data: RDD[SparseVectorRecord], dim: Int, maybeSaveDir: Option[String] = None) 
    (implicit session: SparkSession) = {
    import session.implicits._
    val vectors = data.map (_.v).cache
    val mat = new RowMatrix (vectors)
    val svd = mat.computeSVD (dim, computeU=true)

    val s = DenseMatrix diag svd.s
    val termsEmbedding = normalizeRows (svd.V multiply s)
    maybeSaveDir.foreach { d =>
      svd.U.rows.
        zip (data.map(_.pii)).
        map (x => (x._2, x._1.toArray)).
        toDF.write.option("path", s"$d/doc-lsiembedding").
        save
      HuguesAbstracts.saveLabeledEmbedding (
        s"$d/doc-lsiembedding",
        s"$d/labeled-lsiembedding")
    }
    svd
  }
 
}
