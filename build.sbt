name := "wiki2graph"

version := "0.1"

scalaVersion := "2.12.8"

val sparkVersion = "2.4.0"
resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"
//libraryDependencies += "ml.sparkling" %% "sparkling-graph-operators" % "0.0.7"

