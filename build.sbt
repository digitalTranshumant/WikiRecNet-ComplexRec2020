name := "graphx"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.3.1"
resolvers +=  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"
libraryDependencies += "ml.sparkling" %% "sparkling-graph-operators" % "0.0.7"

