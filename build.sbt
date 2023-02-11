name := "parquet-comparator"

version := "1.1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.8" % "provided",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.8_1.4.0" % "test",
  "junit" % "junit" % "4.13.2" % "test"
)