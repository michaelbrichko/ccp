name := "cloud_computing_project_2"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-yarn
// libraryDependencies += "org.apache.spark" %% "spark-yarn" % "2.4.0"

// https://www.joda.org/joda-time/dependency-info.html
libraryDependencies += "joda-time" % "joda-time" % "2.10.1"
libraryDependencies += "org.joda" % "joda-convert" % "1.8.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}