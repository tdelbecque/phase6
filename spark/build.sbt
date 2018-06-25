name := "Simple Project"
version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0" % "provided"
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"
libraryDependencies += "com.lambdaworks" %% "jacks" % "2.3.3"

libraryDependencies += "org.apache.lucene" % "lucene-core" % "7.3.1"
libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "7.3.1"

//scalacOptions ++= Seq("-feature", "-language:postfixOps")
scalacOptions ++= Seq("-deprecation", "-language:postfixOps")

//libraryDependencies += "com.eed3si9n" % "sbt-assembly_2.9.1_0.11.2" % "0.7.2"
