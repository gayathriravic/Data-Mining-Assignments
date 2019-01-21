name := "try"

version := "0.1"

scalaVersion := "2.11.0"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.3.1"

