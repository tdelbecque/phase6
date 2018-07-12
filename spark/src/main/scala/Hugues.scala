package com.sodad.abstracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.
  { LogisticRegression, LogisticRegressionModel }
import org.apache.spark.sql.Dataset
import Aliases._

case class TrainedModel (
  trainingPii: Dataset[_],
  testingPii: Dataset[_],
  trainingPoints: Dataset[_],
  testingPoints: Dataset[_],
  predict: Dataset[_],
  model: LogisticRegressionModel
)

object HuguesAbstracts extends OrigDataSource {

  val storagePrefix = X.storagePrefix + "/data-hugues"
  val path = storagePrefix + "/tsv"
  val replacementPath = storagePrefix + "/replacement"
  val tokenizedPath = storagePrefix + "/tokenized"

  /**
    *  reads the original file and provides a Dataset[OrigRecord] (~ OrigDataSet)
    */
  def readOrigData (implicit session: SparkSession) : OrigDataSet = 
    getOrigDataSet (path, 6)

  def readOrigData (dataFieldIndice: Int) (implicit session: SparkSession) : OrigDataSet = 
    getOrigDataSet (path, dataFieldIndice)

  def readOrigDataWithTitle (implicit session: SparkSession) : OrigDataSet = 
    getOrigDataSet (path, 1)

  def readOrigDataWithAbstract (implicit session: SparkSession) : OrigDataSet =
    getOrigDataSet (path, 6)

  def readOrigDataWithJournal (implicit session: SparkSession) : OrigDataSet = 
    getOrigDataSet (path, 4)

  def getOrigDataSet (path: String, dataFieldIndice: Int) 
    (implicit session: SparkSession) : OrigDataSet = {
    import session.implicits._
    val x = session.sqlContext.read.
      option ("header", "true").
      option ("delimiter", "\t").
      csv (path).
      map { x => OrigRecord (x.getString (0), x.getString(dataFieldIndice)) }
    x
 }

  def getOrigDataSet (path: String) (implicit session: SparkSession) =
    getOrigDataSet (path, 6)

  /**
    *  replaces patterns in abstracts, and save it
    */
  def createAndSaveReplacement (savePath: String = replacementPath) 
    (implicit a: A, session: SparkSession) {
    val orig = readOrigData
    val replacement = X replacePatternsIn (orig)
    replacement.write.option ("path", savePath).save
  }

  /**
    *  load the replacement file
    */
  def loadPatternReplacements (savePath: String = replacementPath) 
    (implicit session: SparkSession) : OrigDataSet = {
    import session.implicits._
    session.sqlContext.read.option ("path", savePath).load.as[OrigRecord]
  }

  def tokenizePerSentenceAndSave (data: OrigDataSet, savePath: String = tokenizedPath) 
    (implicit session: SparkSession)  = {
    X tokenizePerSentenceAndSave (data, savePath)
  }

  def saveLabeledEmbedding (docEmbeddingPath: String, savePath: String)
    (implicit session: SparkSession) = {
    import session.implicits._
    val embedding = session.sqlContext.read.
      option("path", docEmbeddingPath).load
    val mainCategoriesLabels = HeadCategories.createMainCategories._2
    val docCategories = HeadCategories.readMainCategories
    val labeled = embedding.flatMap { x => 
      docCategories.get (x.getString(0) slice (1,9)).toList.
        flatMap { y => 
          y.categories.map { z => 
            (x.getString(0), mainCategoriesLabels (z), 
              x.getSeq[Double](1))}}}
    labeled.write.option ("path", savePath).save
  }

  def trainMainCategories (
    labeledEmbeddingPath: String,
    maybeSaveDirectory: Option[String] = None,
    split: Double = 0.8
  ) (implicit session: SparkSession) = 
  {
    import session.implicits._
    val labeled = session.sqlContext.read.
      option ("path", labeledEmbeddingPath).load.
      map { x => (
        x getString 0,
        new LabeledPoint (
          x getInt 1,
          Vectors dense (x getSeq[Double] 2).toArray[Double]))}.
      toDF ("pii", "data").as[(String, LabeledPoint)]

    val Array (piiTrain, piiTest) = labeled.map (_._1).distinct.
      randomSplit (Array (split, 1.0 - split))
    val training = piiTrain.join (labeled, 
      piiTrain("value") === labeled ("pii")).
      toDF ("dummy", "pii", "data").
      drop("dummy").
      as[(String, LabeledPoint)]
    val testing = piiTest.join (labeled, 
      piiTest("value") === labeled ("pii")).
      toDF ("dummy", "pii", "data").
      drop("dummy").
      as[(String, LabeledPoint)]
    val trainingPii = training.map (_._1)
    val trainingPoints = training.map (_._2)
    val testingPii = testing.map (_._1)
    val testingPoints = testing.map (_._2)

    val lr = new LogisticRegression
    val lrModel = lr.fit (trainingPoints)
    val predict = lrModel.transform (testingPoints)

    maybeSaveDirectory foreach { d =>
      trainingPii.write.option ("path", d + "/training-pii").save
      testingPii.write.option ("path", d + "/testing-pii").save
      trainingPoints.write.option ("path", d + "/training-points").save
      testingPoints.write.option ("path", d + "/testing-points").save
      predict.write.option ("path", d + "/predict").save
      lrModel.save (d + "/model")
    }
    TrainedModel (
      trainingPii,
      testingPii,
      trainingPoints,
      testingPoints,
      predict,
      lrModel)
  }

  def doit (implicit a: A, session: SparkSession) = {
    createAndSaveReplacement ()
    val replacement = loadPatternReplacements ()
    tokenizePerSentenceAndSave (replacement)
  }
}
