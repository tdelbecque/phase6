package com.sodad.abstracts

import java.util.Properties
import java.io.{
  File, PrintWriter, 
  ObjectInputStream, ObjectOutputStream, 
  FileInputStream, FileOutputStream }
import org.apache.hadoop.fs.{FileSystem => HadoopFS, Path => HadoopPath}
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import scala.collection.JavaConversions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.LabeledPoint

import Aliases._

trait OrigDataSource {
  def getOrigDataSet (path: String)(implicit session: SparkSession) : OrigDataSet
}

trait Transformer extends java.io.Serializable {
  def transform (term: String) : String
}

class StemTransformer extends Transformer {
  val stemmer = new Stemmer
  def transform (term: String) = stemmer.stem (term)
}

class LowerCaseStemTransformer extends Transformer {
  val stemmer = new Stemmer
  def transform (term: String) = stemmer.stem (term.toLowerCase)
}

class LowerCaseTransformer extends Transformer {
  def transform (term: String) = term toLowerCase
}

case class ChemicalExtractRecord (
  pii: String,
  abstr: String,
  tags: Array[ChemSpotTag])

case class ExportChemicalsRecord (
  pii: String,
  text: String,
  _type: String,
  before: String = "",
  after: String = "")

case class TokenizedRecord (
  pii: String,
  terms: Seq[String])

case class BoWRecord (
  pii: String,
  counts: Map[String, Int])

case class JoinWordEmbeddingAndDocWeightsRecord (
  pii: String, 
  parameters: (Double, Array[Float]))

case class DocEmbeddingRecord (pii: String, embedding: Vector);

case class TrainingDataSetsRecord (
  training: Dataset[LabeledPoint], 
  testing: Dataset[LabeledPoint])

case class LogisticRegressionRecord (
  data: TrainingDataSetsRecord,
  predict: Dataset[(Double, Double)],
  model: LogisticRegressionModel)

case class TagTitleAbstractAndSaveRecord (pii: String, issn: String, title: String, abstr: String)

case class DoItConfiguration (
  val extractChemical: Boolean = false,
  val runLSI: Boolean = false,
  val lsiNToKeep: Int = 10000,
  val lsiDim: Int = 100,
  val maybeTokenTransformer : Option[Transformer] = None,
  val stopWordsFile: String = "/home/thierry/Elsevier/abstracts/stopwords",
  val stopWordsExtra: Set[String] = Set.empty[String]
)

object Application {
  val localWorkingDir = "work"
  val hadoopWorkingDir = "hdfs:///user/thierry"

  private def init {
    val d = new File (localWorkingDir)
    if (! d.exists) d.mkdir ()
  }
  init

  object implicits {
    implicit def getTokenizedDataSet (data: OrigDataSet)
      (implicit session: SparkSession) : Dataset[TokenizedRecord] =
      tokenizePerSentence (data, None, None)
    implicit def getOrigDataSet (path: String)
      (implicit session: SparkSession) : OrigDataSet = 
      loadOrigDataSet (path)
    implicit def getWeightedTermsDataSet (path: String)
      (implicit session: SparkSession) : Dataset[WeightedTermRecord] = {
      import session.implicits._
      session.read.option ("path", path).load.as[WeightedTermRecord]
    }
    implicit def readSetFromTextFile (path: String) : Set[String] = 
      Control.usingFileLines (path)().toSet
  }

  def loadOrigDataSet (path: String)
    (implicit session: SparkSession) : Dataset[OrigRecord] = {
    import session.implicits._
    session.sqlContext.read.option("path", path).load.
      toDF ("pii", "abstr").
      as[OrigRecord]
  }

  def extractChemical (data: OrigDataSet, maybeSavePath: Option[String]) 
    (implicit session: SparkSession) : Dataset[ChemicalExtractRecord] = {
    import session.implicits._
    val b = session.sparkContext.broadcast (new SerializableChemPlot)
    val chemicalsData = data.mapPartitions { it =>
      val tagger = b.value.getWorkload
      it map { x =>
        val ts = tagger tag x.abstr
        val tags = new Array[ChemSpotTag](ts size)
        for (i <- 0 until ts.size) {
          val m = ts.get (i)
          tags(i) = ChemSpotTag (m.getStart, m.getEnd, m.getText, m.getType.toString)
        }
        ChemicalExtractRecord (
          pii = x pii,
          abstr = x abstr,
          tags = tags )
      }
    }
    maybeSavePath foreach { savePath =>
      chemicalsData.write.option ("path", savePath) save
    }
    chemicalsData
  }

  def extractChemical (path: String, maybeSavePath: Option[String] = None)
    (implicit session: SparkSession) : Dataset[ChemicalExtractRecord] = 
    extractChemical (loadOrigDataSet (path), maybeSavePath)

  def exportChemical (
    data: Dataset[ChemicalExtractRecord], 
    before: Int = 0, 
    after: Int = 0, 
    maybeSavePath: Option[String] = None) 
    (implicit session: SparkSession) : Seq[ExportChemicalsRecord] = {
    import session.implicits._
    import scala.math.{min, max}
    val ret = data.flatMap { x =>
      x.tags.map { t =>
        val beforeStr = x.abstr.substring (max (t.start - before, 0), t.start)
        val afterStr = x.abstr.substring (
          min (t.end, x.abstr.length),
          min (t.end + after, x.abstr.length))
        ExportChemicalsRecord (x pii, t text, t _type, beforeStr, afterStr )
      }
    }.collect

    maybeSavePath foreach { path =>
      Control.using (new PrintWriter (new File (path))) { f =>
        f.println ("pii\ttype\tbefore\ttext\tafter")
        ret foreach { l =>
          f.println (s"${l pii}\t${l _type}\t${l before}\t${l text}\t${l after}")
        }
      }
    }
    ret
  }

  def loadChemical (path: String)
    (implicit session: SparkSession) : Dataset[ChemicalExtractRecord] = {
    import session.implicits._
    session.read.option ("path", path).load.as[ChemicalExtractRecord]
  }

  def replaceChemicalInRecord (r: ChemicalExtractRecord) : OrigRecord = {
    val fragments = scala.collection.mutable.ArrayBuffer.empty[String]
    var offset = 0;
    import r._
    tags.foreach { t => 
      fragments += abstr.slice (offset, t.start)
      offset = t.end
    }
    fragments += abstr.slice (offset, abstr size)
    OrigRecord (pii, fragments mkString " sodad_chemical ")
  }

  def replaceChemical (data: Dataset[ChemicalExtractRecord], maybeSavePath: Option[String]) 
    (implicit session: SparkSession) : OrigDataSet = {
    import session.implicits._
    val replacement = data map replaceChemicalInRecord
    maybeSavePath foreach { path =>
      replacement.write.option ("path", path) save
    }
    replacement
  }

  def defaultReplacementPatterns = {
    val floatStr = """(?:\+/-|\+-|\+|-)?\s*\d+(?:\.\d+)*"""
    List (
      s"$floatStr\\s*%".r -> " sodadpct ",
      s"[<>=]+\\s*$floatStr".r -> " sodadnumexpr ",
      s"$floatStr\\s*[<>=]+".r -> " sodadnumexpr ",
      s"$floatStr".r -> " sodadnum ",
      """\(.*?\)""".r -> " ",
      """\s+""".r -> " ")
  }

  def replacePatternsInRecord (r: OrigRecord, 
    patterns: Seq[(scala.util.matching.Regex, String)] = defaultReplacementPatterns) 
      : OrigRecord =
    OrigRecord ( r.pii,
      patterns.foldLeft (r.abstr) { case (a, (e, r)) => e replaceAllIn (a, r) } )
    
  def replacePatterns (data: OrigDataSet, 
    patterns: Seq[(scala.util.matching.Regex, String)] = defaultReplacementPatterns,
    maybeSavePath: Option[String])
    (implicit session: SparkSession): OrigDataSet = {
    import session.implicits._
    val replacement = data map (replacePatternsInRecord (_, patterns))
    maybeSavePath foreach { path =>
      replacement.write.option ("path", path) save
    }
    replacement
  }

  def annToStr (ann: Annotation) : String = {
    val posMap = Map(
      "VB" -> "verb", "VBD" -> "verb", "VBG" -> "verb", "VBN" -> "verb", "VBZ" -> "verb",
      "VBP" -> "verb", "VD" -> "verb", "VDD" -> "verb", "VDG" -> "verb", "VDN" -> "verb",
      "VDZ" -> "verb", "VDP" -> "verb", "VH" -> "verb", "VHD" -> "verb", "VHG" -> "verb",
      "VHN" -> "verb", "VHZ" -> "verb", "VHP" -> "verb", "VV" -> "verb", "VVD" -> "verb",
      "VVG" -> "verb", "VVN" -> "verb", "VVP" -> "verb", "VVZ" -> "verb", "NN" -> "noun",
      "NNS" -> "noun", "NP" -> "noun", "NPS" -> "noun", "JJ" -> "adj", "JJR" -> "adj",
      "JJS" -> "adj", "RB" -> "adv", "RBR" -> "adv", "RBS" -> "adv")

    (for (s <- ann get classOf[SentencesAnnotation]) yield (
      for (t <- s.get (classOf[TokensAnnotation]).map { x =>
        val tag = posMap.getOrElse (x.tag(), "other")
        val str = x.originalText () match {
          case "<" => "//<"
          case ">" => "//>"
          case "|" => "//|"
          case _ => x.originalText ()
        }
        val lemma = x.lemma () match {
          case "<" => "//<"
          case ">" => "//>"
          case "|" => "//|"
          case _ => x.lemma ()
        }
        s"<$str|$lemma|$tag|0|0>"
      })
      yield t).toList).flatMap { x => x }.toList.mkString (" ")
  }

  def foo (implicit session: SparkSession) = {
    import session.implicits._
    val props = new Properties ()
    props.put ("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP (props)

    val x = session.sqlContext.read.
      option ("header", "true").
      option ("delimiter", "\t").
      csv (HuguesAbstracts.path).
      collect.
      map { x => 
        val titleAnn = new Annotation (x getString 1)
        val abstrAnn = new Annotation (x getString 6)
        pipeline annotate titleAnn
        val titleAnnotated = annToStr (titleAnn)
        pipeline annotate abstrAnn
        val abstrAnnotated = annToStr (abstrAnn)
        s"${x getString 0}\t${x getString 3}\t${titleAnnotated}\t${abstrAnnotated}"
      }
  }

  def posTag (data: OrigDataSet)(implicit session: SparkSession) 
      : Dataset[String] = {
    import session.implicits._
    val posMap = Map(
      "VB" -> "verb",
      "VBD" -> "verb",
      "VBG" -> "verb",
      "VBN" -> "verb",
      "VBZ" -> "verb",
      "VBP" -> "verb",
      "VD" -> "verb",
      "VDD" -> "verb",
      "VDG" -> "verb",
      "VDN" -> "verb",
      "VDZ" -> "verb",
      "VDP" -> "verb",
      "VH" -> "verb",
      "VHD" -> "verb",
      "VHG" -> "verb",
      "VHN" -> "verb",
      "VHZ" -> "verb",
      "VHP" -> "verb",
      "VV" -> "verb",
      "VVD" -> "verb",
      "VVG" -> "verb",
      "VVN" -> "verb",
      "VVP" -> "verb",
      "VVZ" -> "verb",
      "NN" -> "noun",
      "NNS" -> "noun",
      "NP" -> "noun",
      "NPS" -> "noun",
      "JJ" -> "adj",
      "JJR" -> "adj",
      "JJS" -> "adj",
      "RB" -> "adv",
      "RBR" -> "adv",
      "RBS" -> "adv")

    val x = data mapPartitions { it => 
      val props = new Properties ()
      props.put ("annotators", "tokenize, ssplit, pos, lemma")
      val pipeline = new StanfordCoreNLP (props)
      it map { case OrigRecord (pii, abstr) =>
        val ann = new Annotation (abstr)
        pipeline annotate ann
        s"$pii\t1\t" + (for (s <- ann get classOf[SentencesAnnotation]) yield (
          for (t <- s.get (classOf[TokensAnnotation]).map { x => 
            val tag = posMap.getOrElse (x.tag(), "other")
            val str = x.originalText () match {
              case "<" => "//<"
              case ">" => "//>"
              case "|" => "//|"
              case _ => x.originalText ()
            }
            val lemma = x.lemma () match {
              case "<" => "//<"
              case ">" => "//>"
              case "|" => "//|"
              case _ => x.lemma ()
            }
            s"<$str|$lemma|$tag|0|0>"
          })
          yield t).toList).flatMap { x => x }.toList.mkString (" ")
      }
    }
    x
  }

  def posTag_2 (data: OrigDataSet)(implicit session: SparkSession) 
      : Dataset[(String, String)] = {
    import session.implicits._
    val posMap = Map(
      "VB" -> "verb",
      "VBD" -> "verb",
      "VBG" -> "verb",
      "VBN" -> "verb",
      "VBZ" -> "verb",
      "VBP" -> "verb",
      "VD" -> "verb",
      "VDD" -> "verb",
      "VDG" -> "verb",
      "VDN" -> "verb",
      "VDZ" -> "verb",
      "VDP" -> "verb",
      "VH" -> "verb",
      "VHD" -> "verb",
      "VHG" -> "verb",
      "VHN" -> "verb",
      "VHZ" -> "verb",
      "VHP" -> "verb",
      "VV" -> "verb",
      "VVD" -> "verb",
      "VVG" -> "verb",
      "VVN" -> "verb",
      "VVP" -> "verb",
      "VVZ" -> "verb",
      "NN" -> "noun",
      "NNS" -> "noun",
      "NP" -> "noun",
      "NPS" -> "noun",
      "JJ" -> "adj",
      "JJR" -> "adj",
      "JJS" -> "adj",
      "RB" -> "adv",
      "RBR" -> "adv",
      "RBS" -> "adv")

    val x = data mapPartitions { it => 
      val props = new Properties ()
      props.put ("annotators", "tokenize, ssplit, pos, lemma")
      val pipeline = new StanfordCoreNLP (props)
      it map { case OrigRecord (pii, abstr) =>
        val ann = new Annotation (abstr)
        pipeline annotate ann
        (pii, (for (s <- ann get classOf[SentencesAnnotation]) yield (
          for (t <- s.get (classOf[TokensAnnotation]).map { x => 
            val tag = posMap.getOrElse (x.tag(), "other")
            val str = x.originalText () match {
              case "<" => "//<"
              case ">" => "//>"
              case "|" => "//|"
              case _ => x.originalText ()
            }
            val lemma = x.lemma () match {
              case "<" => "//<"
              case ">" => "//>"
              case "|" => "//|"
              case _ => x.lemma ()
            }
            s"<$str|$lemma|$tag|0|0>"
          })
          yield t).toList).flatMap { x => x }.toList.mkString (" ")
        )
      }
    }
    x
  }

  def posTagTitleAbstractAndSave (maybeSavePath: Option[String]) 
    (implicit session: SparkSession) : Dataset[TagTitleAbstractAndSaveRecord]
  = {
    import session.implicits._
    val posTitle = posTag_2 (HuguesAbstracts.readOrigDataWithTitle).
      toDF ("pii", "title")
    val posAbstract = posTag_2 (HuguesAbstracts.readOrigDataWithAbstract).
      toDF ("pii", "abstr")
    val issns = HuguesAbstracts.readOrigData(3).
      toDF ("pii", "issn")
    val ret = posTitle.join (posAbstract, "pii").join (issns, "pii").
      as[TagTitleAbstractAndSaveRecord]
    maybeSavePath foreach { path => 
      ret.cache ()
      Control.using (new java.io.BufferedWriter (new java.io.FileWriter (path))) { f =>
        ret.collect.foreach { x =>
          f write s"${x.pii}\t${x.issn}\t${x.title}\t${x.abstr}"
          f newLine } } }
    ret
  }
 
  def tokenizePerSentence (
    data: OrigDataSet, 
    maybeTransformer: Option[Transformer], 
    maybeSavePath: Option[String]
  ) (implicit session: SparkSession) : Dataset[TokenizedRecord] = {
    import session.implicits._
    val transform : String => String = maybeTransformer match {
      case Some (s) => s.transform _
      case _ => x => x
    }
    val tokenized = data mapPartitions { it => 
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit")
      val pipeline = new StanfordCoreNLP (props)
      it flatMap { case OrigRecord (pii, abstr) =>
        val ann = new Annotation (abstr)
        pipeline annotate ann
        (for (s <- ann get classOf[SentencesAnnotation]) yield (
          for (t <- s.get (classOf[TokensAnnotation]).map { x => 
            transform (x.originalText ())})
          yield t).toList).map { TokenizedRecord (pii, _) }.toList
      }
    }
    maybeSavePath foreach ( tokenized.write.option ("path", _).save )
    tokenized
  }

  def tokenizePerSentence (
    path: String, 
    maybeTransformer: Option[Transformer] = None, 
    maybeSavePath: Option[String]=None
  ) (implicit session: SparkSession) : Dataset[TokenizedRecord] = 
    tokenizePerSentence (loadOrigDataSet (path), maybeTransformer, maybeSavePath)

  def bowPerDoc (data: Dataset[TokenizedRecord], maybeSavePath: Option[String] = None)
  (implicit session: SparkSession) : Dataset[BoWRecord] = {
    import scala.collection.mutable.HashMap
    type H = HashMap[String, Int]
    import session.implicits._
    val bow = data . map { r => (r.pii, r.terms) } . rdd .
      aggregateByKey (HashMap.empty[String, Int]) (
        { (u: H, v: Seq[String]) => 
          v.foldLeft (u) { (a, t) => 
            a += t -> (a.getOrElse (t, 0) + 1)
            a
          }
        },
        { (u1: H, u2: H) => 
          u2.foldLeft (u1) { case (a, (t, n)) =>
            a += t -> ( a.getOrElse (t, 0) + n)
            a
          }
        }
      ).toDF("pii", "counts").as[BoWRecord]
    maybeSavePath foreach { path =>
      bow.write.option ("path", path) save
    }
    bow
  }

  def computeWordEmbedding (
    data: Dataset[TokenizedRecord],
    embeddingSize: Int,
    numPartitions: Int = 16,
    sample: Double = 1.0, 
    maybeSavePath: Option[String] = None
  ) (implicit session: SparkSession) = {
    import session.implicits._
    val d = data.sample (sample).map (_.terms) cache
    val w2v = new Word2Vec ()
    w2v.setNumPartitions (numPartitions)
    w2v.setVectorSize (embeddingSize)
    val model = w2v.fit (d.rdd)
    d.unpersist ()
    maybeSavePath foreach { path => 
      session.sparkContext.parallelize (model.getVectors.toList).
        toDF.write.option ("path", path).
        save
    }
    model
  }

  def weightsPerTokenAndDoc (
    data: Dataset[WeightedTermRecord], 
    maybeSavePath: Option[String] = None)
    (implicit session: SparkSession) : Dataset[(String, (String, Double))] = {
    import session.implicits._
    val result = data.flatMap { case WeightedTermRecord (pii, m) =>
      m.foldLeft (List.empty[(String, (String, Double))]) {
        (a, r) => a :+ (r._1, (pii, r._2)) 
      }
    }
    maybeSavePath foreach { path =>
      result.write.option ("path", path) save
    }
    result
  }

  /**
    *  @param embeddingsDataSet maps term to coordinates in embedding
    *  @param weightsDataSet : maps pii to the tfidf weights of its terms
    */
  def joinWordEmbeddingAndDocWeights (
    embeddingsDataSet: Dataset[(String, Array[Float])], 
    weightsDataSet: Dataset[WeightedTermRecord]
  ) (implicit session: SparkSession) /*: Dataset[JoinWordEmbeddingAndDocWeightsRecord]*/ = {
    import session.implicits._
    val w = weightsPerTokenAndDoc (weightsDataSet)
    embeddingsDataSet.rdd.
      join (weightsPerTokenAndDoc (weightsDataSet).rdd).
      map {x => (x._2._2._1, (x._2._2._2, x._2._1))}.
      toDF("pii", "parameters").
      as[JoinWordEmbeddingAndDocWeightsRecord]
  }

  def joinWordEmbeddingAndDocWeights (embeddingsPath: String, weightsPath: String) 
    (implicit session: SparkSession) : Dataset[JoinWordEmbeddingAndDocWeightsRecord] = {
    import session.implicits._
    val e = session.sqlContext.read.option("path", embeddingsPath).
      load.as[(String, Array[Float])]
    val w = session.sqlContext.read.option("path", weightsPath).
      load.as[WeightedTermRecord]
    joinWordEmbeddingAndDocWeights (e, w)
  }

  def computeDocEmbedding (data: Dataset[JoinWordEmbeddingAndDocWeightsRecord], maybeSavePath: Option[String] = None) 
  (implicit session: SparkSession) : Dataset[DocEmbeddingRecord] = {
    import session.implicits._
    val ret = data.map { x =>
      val weight = x.parameters._1
      val embedding = x.parameters._2
      val xs: Array[Double] = Array (
        weight, 
        embedding.map (_ * x.parameters._1):_*)
      (x.pii, xs) 
    }.rdd.reduceByKey { (v1, v2) => 
      v1.zip (v2).map { x => x._1 + x._2 }
    }.mapValues { xs => 
      Vectors dense xs.tail.map (_/xs.head)
    }.toDF("pii", "embedding").as[DocEmbeddingRecord]
    maybeSavePath foreach { path => 
      ret.write.option ("path", path) save
    }
    ret
  }

  def computeLSIDocEmbedding (
    data: Dataset[WeightedTermRecord],
    docFreqs: Map[String, Int], 
    nToKeep: Int,
    dim: Int,
    stopWords: Set[String] = Set.empty[String],
    maybeSavePath: Option[String] = None
  ) (implicit session: SparkSession) : Dataset[DocEmbeddingRecord] = {
    import session.implicits._
    val x: CreateWeightedVectorDataSetRecord = 
      LSI.createWeightedVectorDataSet (data, docFreqs, nToKeep, stopWords)
    val v = x.data.rdd
    val svd = LSI.computeLSI (v, dim)
    val ret = svd.U.rows.
      zip (v map (_.pii)).
      map (x => DocEmbeddingRecord (x._2, Vectors dense x._1.toArray)).
      toDF("pii", "embedding").
      as[DocEmbeddingRecord]
    maybeSavePath foreach { path => 
      ret.write.option ("path", path) save
    }
    ret
  }

  /**
    *  creates a text data file, comma separated.
    *  each row starts with the head category of the journal, when the journal 
    *  belongs to an unique category.
    *  When the journal does not belong to a unique category, or the category was not found, 
    *  a special code is used (the upper value of the categories codes + 1)
    *  Following values are the embedding coordinates.
    */
  def setHC (data: Dataset[DocEmbeddingRecord]) (implicit session: SparkSession) = {
    import session.implicits._
    val categories = HeadCategories.createMainCategories
    val multiCategory = categories._1.size
    val categoriesMap : Map[String, Int] = HeadCategories.readMainCategories.mapValues { x =>
      if (x.categories.size == 1) categories._2 (x.categories (0))
      else multiCategory
    } map identity

    data.map { x =>
      ((categoriesMap.get (x.pii.substring (1,9)) match {
        case Some (c) => c
        case _ => multiCategory
      }) +: x.embedding.toArray) mkString ","
    }
  }

  def addHC (data: Dataset[DocEmbeddingRecord]) 
    (implicit session: SparkSession) : DataFrame = {
    import session.implicits._
    val categories = HeadCategories.createMainCategories
    val multiCategory = categories._1.size
    val categoriesMap : Map[String, Int] = HeadCategories.readMainCategories.mapValues { x =>
      if (x.categories.size == 1) categories._2 (x.categories (0))
      else multiCategory
    } map identity

    data.map { x => (
      x.pii,
      categoriesMap.get (x.pii.substring (1,9)) match {
        case Some (c) => c
        case _ => multiCategory
      },
      x.embedding)
    }.toDF ("pii", "category", "embedding")
  }

  def createJournalClassifierTrainingDataSet (
    data: Dataset[DocEmbeddingRecord],
    split: Double = 0.8
  ) (implicit session: SparkSession) : TrainingDataSetsRecord = {
    import session.implicits._
    val targetMap = (data map { x => x.pii slice (1,9) }).
      distinct.collect.sorted.zipWithIndex.toMap

    val labeledData = data map { x =>
      LabeledPoint ( targetMap (x.pii slice (1,9)), x embedding )
    }
    val Array(training, testing) = labeledData.
      randomSplit (Array (split, 1.0 - split))
    TrainingDataSetsRecord (training=training, testing=testing)
  }

  def computeLogisticRegressionModel (
    embeddingPath: String, 
    weightPath: String, 
    split: Double = 0.8
  ) (implicit session: SparkSession) : LogisticRegressionRecord = {
    import session.implicits._
    val wordEmbeddingAndDocWeights = 
      joinWordEmbeddingAndDocWeights (embeddingPath, weightPath)
    val docEmbedding = computeDocEmbedding (wordEmbeddingAndDocWeights)
    val trainData = createJournalClassifierTrainingDataSet (docEmbedding, split)
    val lr = new LogisticRegression ()
    val model = lr.fit (trainData.training)
    val predict = model.transform (trainData.testing).
      as[(Double, Vector, Vector, Vector, Double)].
      map { x => (x._1, x._5) }
    LogisticRegressionRecord (trainData, predict, model)
  }

  /**
    *  end to end processing
    *  
    *  All results will be saved in $localWorkingDir/$dir hdlf directory. 
    *  If this directory does not exist, it is created.
    * 
    *  If maybeData is None, HuguesAbstracts.readOrigData will be used.
    */ 
  def doit (dir: String, maybeData: Option[OrigDataSet] = None,
    configuration: DoItConfiguration = new DoItConfiguration)
    (implicit session: SparkSession) = {
    import session.implicits._
    def readFrom(path: String) = 
      session.sqlContext.read.option ("path", path).load

    val hfs = HadoopFS.get (session.sparkContext.hadoopConfiguration)
    val localDir = new File (s"$localWorkingDir/$dir")
    if (! localDir.exists) assert (localDir.mkdirs ())

    val chemicalPath = s"$dir/extract-chemicals"
    val replacementPath = s"$dir/replacement"
    val tokenizedPath = s"$dir/tokenized"
    val bowPerDocPath = s"$dir/bow-per-doc"
    val tfidfWeightedPath = s"$dir/tfidf-weighted"

    implicit def pathToOption (p: String) : Option[String] = Some(p)
    implicit def pathToHPath (p: String) : HadoopPath = 
      new HadoopPath (s"$hadoopWorkingDir/$p")

    def logReading (path: String) = println (s"reading $path")
    def logBuilding (path: String) = 
      println (s"building ${pathToHPath (path).toString}")

    val d1 : OrigDataSet = maybeData.getOrElse (HuguesAbstracts.readOrigData)

    val d2 : Option[Dataset[ChemicalExtractRecord]] =
      if (configuration.extractChemical)
        Some ( 
          if (hfs.exists (chemicalPath)) {
            logReading (chemicalPath)
            readFrom(chemicalPath).as[ChemicalExtractRecord]
          } else {
            logBuilding (chemicalPath)
            extractChemical (d1, chemicalPath)
          }
        )
      else
        None

    val d3 : OrigDataSet = d2.map (replaceChemical (_ , None)).getOrElse (d1)

    val d4 : OrigDataSet = 
      if (hfs.exists (replacementPath)) {
        logReading (replacementPath)
        readFrom(replacementPath).as[OrigRecord]
      } else {
        logBuilding (replacementPath)
        replacePatterns (d3, defaultReplacementPatterns, replacementPath)
      }

    val d5 : Dataset[TokenizedRecord] = 
      if (hfs.exists (tokenizedPath)) {
        logReading (tokenizedPath)
        readFrom (tokenizedPath).as[TokenizedRecord]
      } else {
        logBuilding (tokenizedPath)
        tokenizePerSentence ( d4, 
          configuration.maybeTokenTransformer, 
          tokenizedPath)
      }

    val d6 : Dataset[BoWRecord] = 
      if (hfs.exists (bowPerDocPath)) {
        logReading (bowPerDocPath)
        readFrom (bowPerDocPath).as[BoWRecord]
      } else {
        logBuilding (bowPerDocPath)
        bowPerDoc (d5, bowPerDocPath)
      }

    val r : CreateWeightedDataSetRecord = 
      if (hfs.exists (tfidfWeightedPath)) {
        logReading (tfidfWeightedPath)
        Control.using (new ObjectInputStream (
          new FileInputStream (s"$localWorkingDir/$dir/doc-freq"))) { stream =>
          val (numDocs, docFreqsMap) = 
            stream.readObject.asInstanceOf[(Long, Map[String, Int])]
          val data = readFrom (tfidfWeightedPath).as[WeightedTermRecord]
          CreateWeightedDataSetRecord (numDocs, docFreqsMap, data)
        }
      } else {
        logBuilding (tfidfWeightedPath)
        val r = LSI.createWeightedDataSet (d6, tfidfWeightedPath)
        Control.using (new ObjectOutputStream (
          new FileOutputStream (s"$localWorkingDir/$dir/doc-freq"))) {
          _.writeObject ( (r.numDocs, r.docFreqsMap) )
        }
        r
      }
    
    if (configuration.runLSI) {
      val vectors = LSI.createWeightedVectorDataSet (
        r.data, 
        r.docFreqsMap,
        configuration.lsiNToKeep,
        configuration.stopWordsExtra + configuration.stopWordsFile
      )
    }

  }

  def dotest (dir: String)(implicit session: SparkSession) = {
    import session.implicits._
    def readFrom(path: String) = 
      session.sqlContext.read.option ("path", path).load

    val hfs = HadoopFS.get (session.sparkContext.hadoopConfiguration)
    val localDir = new File (s"$localWorkingDir/$dir")
    if (! localDir.exists) assert (localDir.mkdirs ())

    val chemicalPath = s"$dir/extract-chemicals"
    val replacementPath = s"$dir/replacement"
    val tokenizedPath = s"$dir/tokenized"
    val bowPerDocPath = s"$dir/bow-per-doc"

    implicit def pathToOption (p: String) : Option[String] = Some(p)
    implicit def pathToHPath (p: String) : HadoopPath = 
      new HadoopPath (s"$hadoopWorkingDir/$p")

    val d1 : OrigDataSet = 
      HuguesAbstracts.readOrigData

    val d2 : Dataset[ChemicalExtractRecord] = 
      if (hfs.exists (chemicalPath))
        readFrom(chemicalPath).as[ChemicalExtractRecord]
      else
        null
    d2
  }

}

/**
  *  Compute doc embedding based on abstracts, and train a lr model to predict journal
  */
object ProcessAbstracts {
  def doit (implicit session: SparkSession) = {
    import session.implicits._
    import Control.using
    import java.io.{ObjectOutputStream, FileOutputStream}

    Application.doit ("w2v_100_abstracts", Some (HuguesAbstracts.readOrigDataWithAbstract))
    val tokenized = session.sqlContext.read.option ("path", "w2v_100_abstracts/tokenized").
      load.as[TokenizedRecord]
    val w2v_model = Application.computeWordEmbedding (tokenized, 100)
    using (new ObjectOutputStream (new FileOutputStream ("work/w2v_100_abstracts/w2v_model"))) { f =>
      f.writeObject (w2v_model)
    }
    session.sparkContext.parallelize (w2v_model.getVectors.toList).toDF.
      write.option ("path", "w2v_100_abstracts/w2v_100").save
    val wordEmbeddingAndDocWeights = Application.
      joinWordEmbeddingAndDocWeights ("w2v_100_abstracts/w2v_100", "w2v_100_abstracts/tfidf-weighted")
    val docEmbedding = Application.computeDocEmbedding (wordEmbeddingAndDocWeights)
    docEmbedding.toDF.write.option ("path", "w2v_100_abstracts/doc-embedding") save
    val lr = Application.computeLogisticRegressionModel ("w2v_100_abstracts/w2v_100", "w2v_100_abstracts/tfidf-weighted")
    lr.predict.toDF.write.option ("path", "w2v_100_abstracts/logistic-regression/predict").save
    lr.model.save ("w2v_100_abstracts/logistic-regression/model")
    ProcessAbstracts.exportModelData (Some("work/w2v_100_abstracts/model-data"))
  }

  def exportModelData (maybeSavePath: Option[String]) (implicit session: SparkSession) = {
    import session.implicits._
    val docEmbedding = session.sqlContext.read.option ("path", "w2v_100_abstracts/doc-embedding").
      load.as[DocEmbeddingRecord].
      map {x => (x pii, x embedding)}
    val issn = HuguesAbstracts.readOrigData (3).map {x => (x.pii, x.abstr)}
    val values = docEmbedding.rdd.join (issn.rdd).map { x => 
      val positionStr = x._2._1.toArray mkString "\t"
      s"${x._2._2}\t$positionStr"
    }
    maybeSavePath foreach { path => 
      Control.using (new java.io.PrintWriter (path)) { f =>
        values.collect.foreach (f println _)
      }
    }
    values
  }
}
