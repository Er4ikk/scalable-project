ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

libraryDependencies +=
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.1.0"
val sparkVersion = "3.5.4"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
libraryDependencies+="com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-1.9.17"

lazy val root = (project in file("."))
  .settings(
    name := "example-project-2",
    idePackagePrefix := Some("com.example")
  )


