ThisBuild / version := "0.1.0-SNAPSHOT"


/*libraryDependencies +=
  "org.scala-lang.modules" %% "scala-parallel-collections" % "0.6.0"

 */
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
libraryDependencies+="com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-1.9.17"

lazy val root = (project in file("."))
  .settings(
    name := "example-project-2",
    idePackagePrefix := Some("com.example")
  )

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("public")
)


