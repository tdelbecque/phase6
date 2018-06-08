package com.sodad.abstracts

import java.util.Properties
import org.apache.spark.sql.{SparkSession, Dataset}
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.LogisticRegression

case class Line (value: String)
case class OrigRecord (pii: String, abstr: String)

// Implements the Loan Pattern, see the Scala Cookbook
object Control {
  def using[A <: {def close () : Unit}, B](resource: A)(f: A => B) =
    try {
      f (resource)
    }
    finally {
      resource close
    }

  def usingFileLines[B](path: String)(f: String => B = {x : String => x}) : List[B] = 
    using(scala.io.Source.fromFile(path)) { source => {
      (for (l <- source.getLines) yield f (l)) toList
    }}
}

object Aliases {
  type OrigDataSet = Dataset[OrigRecord]
}

import Control._
import Aliases._

object X {
  val storagePrefix = "hdfs://pollux:54310/user/thierry"
  val workStoragePrefix = s"$storagePrefix/abstracts_work"

  val origSourcePath = "hdfs://pollux:54310/user/thierry/abstracts"
  val origDataPath = "hdfs://pollux:54310/user/thierry/abstracts_work/orig"

  val patternReplacementPath = "hdfs://pollux:54310/user/thierry/abstracts_work/pattern_replacement"
  val tokenizedPath = "hdfs://pollux:54310/user/thierry/abstracts_work/tokenized"
  val tokensPerDocPath = s"$storagePrefix/abstracts_work/tokenPerDoc"

  def getOrigDataSet (implicit session: SparkSession) : OrigDataSet = {
    import session.implicits._
    val matcher = """^S.+\.xml$""".r.pattern.matcher _

    val rawData = session.sqlContext.read.
      textFile (origSourcePath).
      map (_.split ("\t") toList).
      filter ( (z: List[String]) => z match {
        case x :: y :: Nil if matcher (x) matches => true
        case _ => false
      }).
      map { case pii :: abstr :: Nil =>
        (pii substring (0, pii.length - 4), 
          abstr replaceFirst ("^[A-Z][a-z]+([A-Z\u03B1-\u03C9\u0391-\u03A9])", "$1"))}
    rawData.rdd.partitionBy (new org.apache.spark.HashPartitioner (32)).
      toDF ("pii", "abstr").as[OrigRecord]
  }

  def loadOrigDataSet (implicit session: SparkSession) : Dataset[OrigRecord] = {
    import session.implicits._
    session.sqlContext.read.option("path", origDataPath).load.
      toDF ("pii", "abstr").
      as[OrigRecord]
  }

  def extractNumerics (ds: OrigDataSet)(implicit session: SparkSession) : Dataset[(String, String, String)] = {
    import session.implicits._
    ds mapPartitions { it => {
      val pattern = "((?:\\+\\s*/?)?(?:\\s*-)?)(?:\\s|\u00a0)*((?:\\d+\\.)*\\d+)(?:\\s|\u00a0)*([a-zA-Z]+(?:/[a-zA-Z]+)?)".r
      it flatMap { case OrigRecord (pii, abstr) => {
        val m = pattern findAllIn abstr
        val xs = scala.collection.mutable.ArrayBuffer.empty[(String, String, String)]
        while (m hasNext) {
          m.next ()
          xs += ((m.group(1), m.group(2), m.group(3)))
        }
        xs
      }}
    }}
  }

  /*
   *  Example
   *  
   *  var chimie = X.extractChemicals (data)
   *  var dico = usingFileLines("/usr/share/dict/american-english")(x => x toLowerCase).toSet
   *  using (new java.io.PrintWriter ("terms")) {x => 
   *       x.write (dico.intersect(chimie.toSet).mkString("\n")))
   *  }
   */
  def extractChemicals (ds: OrigDataSet, minLength: Integer = 4)
    (implicit session: SparkSession) = {
    import session.implicits._
    ds.
      mapPartitions {it => {
        val pattern = """\S*\d+,\d+-[^\s.]+""".r
        it flatMap {
          case OrigRecord (pii, abstr) =>
            (pattern findAllIn abstr).toList
        }}}.
      distinct.
      mapPartitions {it => {
        val pattern = "[^-+0-9()\\[\\]{},–<>\\s/'=`\"]+".r
        it flatMap (pattern.findAllIn (_).toList.filter (_.size >= minLength))
      }}.
      distinct.collect.toSet
  }

  def cleanChemical (xs: Set[String], 
    acceptedTermsPath: String = "/home/thierry/Elsevier/abstracts/terms") : Set[String] =
  {
    val commonDictionnary = usingFileLines("/usr/share/dict/american-english") {x =>
      x toLowerCase}.toSet
    val acceptedCommonTerms = usingFileLines (acceptedTermsPath)()
    val excludedCommonTerms = commonDictionnary -- acceptedCommonTerms
    val pattern = "[^a-zA-Z\u03B1-\u03C9\u0391-\u03A9]".r
    (xs -- excludedCommonTerms) filter (pattern.findFirstIn(_) isEmpty)
  }

  def extractPattern (ds: OrigDataSet, patternStr: String) 
    (implicit session: SparkSession) = {
    import session.implicits._
    ds.mapPartitions {it => {
      val pattern = patternStr.r
      it flatMap { case OrigRecord (pii, abstr) => (pattern findAllIn abstr) toList
      }}}
  }

  def replacePatternsIn (ds: OrigDataSet) (implicit a: A, session: SparkSession) = {
    import session.implicits._
    val data = ds
    val floatStr = """(?:\+/-|\+-|\+|-)?\s*\d+(?:\.\d+)*"""
    val pattern = a.chemicalPatternStr (100000).r
    val pattern2 = """[^\s]+sodad""".r
    val pattern_pct = s"$floatStr\\s*%".r
    val pattern_numexpr1 = s"[<>=]+\\s*$floatStr".r
    val pattern_numexpr2 = s"$floatStr\\s*[<>=]+".r
    val pattern_num = s"$floatStr".r
    val pattern_paren = """\(.*?\)""".r
    data mapPartitions { it => 
      it map { x  =>
        val s1 = pattern replaceAllIn (x.abstr, "sodad_chemical")
        val s2 = pattern2 replaceAllIn (s1, "sodad")
        val s3 = pattern_pct replaceAllIn (s2, " sodad_pct ")
        val s4 = pattern_numexpr1 replaceAllIn (s3, " sodad_numexpr ")
        val s5 = pattern_numexpr1 replaceAllIn (s4, " sodad_numexpr ")
        val s6 = pattern_num replaceAllIn (s5, " sodad_num ")
        val s7 = pattern_paren replaceAllIn (s6, "")
        val s = s7
        OrigRecord (x pii, s)
      }
    }
  }
  def loadPatternReplacements (implicit session: SparkSession) : OrigDataSet = {
    import session.implicits._
    session.sqlContext.read.option ("path", patternReplacementPath).load.as[OrigRecord]
  }

  /**
    * @return a Dataset[Seq[String]]
    */
  def loadTokenized (savePath: String = tokenizedPath) 
    (implicit session: SparkSession) = {
    import session.implicits._
    session.sqlContext.read.option ("path", savePath).load.map (_ getSeq[String] (0))
  }

  /**
    * Annotate with Stanford core NLP. 
    * Save a dataset of rows, where each row is an array of terms in a sentence.
    * So each row in this dataset stands for a line in a doc of the corpus.
    * (the pii is lost)
    */
  def tokenizePerSentenceAndSave (data: OrigDataSet, savePath: String = tokenizedPath) 
    (implicit session: SparkSession) : Unit = {
    import session.implicits._
    val tokenized = data mapPartitions { it => {
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit")
      val pipeline = new StanfordCoreNLP (props)
      it flatMap {case OrigRecord (pii, abstr) => 
        val ann = new Annotation (abstr)
        pipeline annotate ann

        (for (s <- ann get classOf[SentencesAnnotation]) yield (
          for (t <- (s get classOf [TokensAnnotation]).map (_.originalText())) yield t
        ).toList).toList
      }
    }}
    tokenized.write.option ("path", savePath) save
  }

  /**
    *  Word2Vec word embedding on a tokenized dataset
    */
  def computeWordEmbedding (embeddingSize: Int = 20, numPartitions: Int = 16, sample: Double = 1.0, savePath: String = tokenizedPath) 
    (implicit session: SparkSession) = {
    val data = loadTokenized(savePath).sample (sample) coalesce 16
    val w2v = new Word2Vec ()
    w2v.setNumPartitions (numPartitions)
    w2v.setVectorSize (embeddingSize)
    w2v.fit(data.rdd)
  }

  /**
    *  Saves the vector for each term 
    */
  def saveWordEmbedding (model: Word2VecModel, fileName: String = "embedding")
    (implicit session: SparkSession) = {
    import session.implicits._
    session.sparkContext.parallelize (model.getVectors.toList).toDF.as[(String, List[String])].
      write.option ("path", s"$storagePrefix/abstracts_work/$fileName").
      save
  }
  def loadWordEmbedding (fileName: String = "embedding")
    (implicit session: SparkSession) = {
    import session.implicits._
    session.sqlContext.read.option ("path", fileName).
      load.as[(String, Seq[Double])]
  }


  def tokenizePerDocAndSave (data: OrigDataSet) (implicit session: SparkSession) : Unit = {
    import session.implicits._
    val tokensPerDoc = data mapPartitions (it => {
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit")
      val pipeline = new StanfordCoreNLP (props)
      it flatMap {case OrigRecord (pii, abstr) =>
        val ann = new Annotation (abstr)
        pipeline annotate ann
        (for (s <- ann get classOf[SentencesAnnotation];
            t <- (s get classOf[TokensAnnotation]).map (_.originalText()))
        yield t).toList map ((_, pii))
      }
    })
    tokensPerDoc.rdd.coalesce (16).toDF.write.option ("path", tokensPerDocPath) save
  }
  def loadTokenPerDoc (savePath: String = tokensPerDocPath) 
    (implicit session: SparkSession) = {
    import session.implicits._
    session.sqlContext.read.option("path", tokensPerDocPath).load.
      coalesce(16).as[(String, String)]
  }

  def computeDocEmbeddingAndSave (
    termEmbeddingStoreName: String,
    docTokenizedStoreName: String,
    docEmbeddingStoreName: String)
    (implicit session: SparkSession) = {
    import session.implicits._
    val embedding = loadWordEmbedding (termEmbeddingStoreName)
    val tokens = loadTokenPerDoc (docTokenizedStoreName)
    val j = tokens.rdd.join (embedding.rdd).
      map {case (t, (pii, v)) => (pii, Array (1.0, v:_*))}
    val docEmbedding = j reduceByKey { (v1: Array[Double], v2: Array[Double]) =>
      for (i <- 0 until v1.size) v1(i) = v1(i) + v2(i)
      v1
    }
    docEmbedding.toDF.as[(String, Array[Double])].
      write.option ("path", docEmbeddingStoreName). 
      save
  }

  def classify (docEmbeddingStoreName: String, trainingProportion: Double) 
    (implicit session: SparkSession) = {
    import session.implicits._
    val x = session.sqlContext.read.
      option ("path", s"$workStoragePrefix/$docEmbeddingStoreName").
      load.as[(String, Array[Double])]
    val targetMap = (x map { case (pii, _) => pii slice (1,9) }).
      distinct.collect.sorted.zipWithIndex.toMap
    val data = x map { case (pii, xs) => LabeledPoint (
      targetMap (pii slice (1,9)).toDouble,
      Vectors dense (for (x <- xs.tail) yield x/xs.head))
    }
    val Array(training, testing) = data.randomSplit (Array(trainingProportion, 1.0 - trainingProportion))
    val lr = new LogisticRegression ()
    val lrModel = lr.fit (training)
//    val predict = lrModel.transform (testing).as[(Double, Vector, Vector, Vector, Double)].
//      map { x => (x._1, x._5) }
    (training, testing, lrModel, targetMap)
  }

}

class A (implicit session: SparkSession) {
  import session.implicits._
  implicit val self: A = this

  val data: Dataset[OrigRecord] = X.loadOrigDataSet 
  val chemicalsSeeds: Set[String] = X.cleanChemical (X.extractChemicals (data))
  def chemicalsHeads (n: Int) : Set[String] = chemicalsSeeds.map (_ slice (0, n))
  def chemicalPatternStr (nSeedTerms: Int, nHeader: Int = 10) : String = {
    val grec = "\u03B1-\u03C9\u0391-\u03A9"
    val header = s"[\\d(\\[][-\\d()\\[\\]{},= $grec]*"
    val combi = 
      s"(?:(?:${chemicalsHeads(nHeader).slice(0,nSeedTerms).toList.mkString ("|")})[a-zA-Z]*)"
    val trailer = s"(?:[-/\\d()\\[\\]{},=$grec]+[a-zA-Z]*)*"
    s"$header$combi$trailer"
  }
  def extractChemicals (nSeedTerms: Int, nHeader: Int = 10) : Dataset[String] = {
    val pattern = chemicalPatternStr (nSeedTerms, nHeader).r
    data.flatMap {
      case OrigRecord(_, a) => (pattern findAllIn a) toList
    } . distinct
  }
  def replacePatternsAndSave {
    val replacement = X replacePatternsIn data
    replacement.write.option ("path", X.patternReplacementPath).save
  }
}


object SimpleApp {

  def saveTokensFreqPerDoc (spark: SparkSession, data: Dataset[(String, String)], out: String) : Unit = {
    import spark.implicits._
    val tokensPerDoc = data mapPartitions (it => {
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit, pos, lemma")
      val pipeline = new StanfordCoreNLP (props)
      it map {case (pii, abstr) =>
        val ann = new Annotation (abstr)
        pipeline annotate ann
        (pii,
          (for (s <- ann get classOf[SentencesAnnotation];
            t <- (s get classOf[TokensAnnotation]).map (_.originalText()))
          yield t).
            toList.
            foldLeft(new HashMap[String, Int]()) {
              (map: HashMap[String, Int], t: String) => {
                map += t -> (map.getOrElse (t, 0) + 1)
                map
              }
            }
        )
      }
    })
    tokensPerDoc.write.
      option("path", "hdfs://pollux:54310/user/thierry/test/").
      saveAsTable (out)
  }

  def saveTokensPerDoc (spark: SparkSession, data: Dataset[(String, String)], name: String) : Unit = {
    import spark.implicits._
    val tokensPerDoc = data mapPartitions (it => {
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit, pos, lemma")
      val pipeline = new StanfordCoreNLP (props)
      it map {case (pii, abstr) =>
        val ann = new Annotation (abstr)
        pipeline annotate ann
        (pii,
          (for (s <- ann get classOf[SentencesAnnotation];
            t <- (s get classOf[TokensAnnotation]).map (_.originalText()))
          yield t).toList
        )
      }
    })
    tokensPerDoc.write.option("path", s"hdfs://pollux:54310/user/thierry/abstracts/out/$name").
      saveAsTable (name)
  }

  def main (args: Array[String]) {
    val spark = SparkSession.builder.appName ("Simple Application").
      getOrCreate()
    import spark.implicits._

    //val sourcePath = "file:///home/thierry/Elsevier/abstracts/files/files0"
    val sourcePath = "hdfs://pollux:54310/user/thierry/abstracts"

    val rawdata = spark.sqlContext.read.
      option ("header", "true").
      option ("inferSchema", "true").
      text (sourcePath).
      as[Line]

    val matcher = """^S.+\.xml$""".r.pattern.matcher _
    val data = rawdata.map {case Line (v) => v split "\t" toList} . filter (
      (z: List[String]) => z match {
        case x :: y :: Nil if matcher (x) matches => true
        case _ => false
      }) .
      map { case pii :: abstr :: Nil =>
        (pii substring (0, pii.length - 4), abstr replaceFirst ("^[A-Z][a-z]+([A-Z])", "$1"))} 
    data.rdd.partitionBy (new org.apache.spark.HashPartitioner (32))
    

    saveTokensPerDoc (spark, data, "tokensPerDoc")
/*
    val tokensPerDoc = data mapPartitions (it => {
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit, pos, lemma")
      val pipeline = new StanfordCoreNLP (props)
      it map {case (pii, abstr) =>
        val ann = new Annotation (abstr)
        pipeline annotate ann
        (pii,
          (for (s <- ann get classOf[SentencesAnnotation];
            t <- s get classOf[TokensAnnotation];
            l <- List(t get classOf [LemmaAnnotation]) if l.length > 3)
          yield l).
            toList.
            foldLeft(new HashMap[String, Int]()) {
              (map: HashMap[String, Int], t: String) => {
                map += t -> (map.getOrElse (t, 0) + 1)
                map
              }
            }
        )
      }
    }) cache ()

    println ("Doc read :" + tokensPerDoc.count ())

    tokensPerDoc.write.option("path", "hdfs://pollux:54310/abstracts/out").
      saveAsTable ("lemmas_per_doc")
    val bar = spark.sqlContext.read.option ("path", "hdfs://pollux:54310/user/thierry/test/").load
    println ("BAR :" + bar.count ())
 */


    spark.stop ()
  }
}
