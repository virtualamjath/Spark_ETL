name := "CatBackPort"

version := "0.0.1"

scalaVersion := "2.10.5"

resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.2.0-cdh5.3.3" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.2.0-cdh5.3.3"% "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.2.0-cdh5.3.3"% "provided",
  "org.apache.spark" % "spark-catalyst_2.10" % "1.2.0-cdh5.3.3" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.2.0-cdh5.3.3" % "provided",
  "net.sf.opencsv" % "opencsv" % "2.3"
)
