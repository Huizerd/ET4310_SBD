name := "Lab1-DS"
version := "0.1"
scalaVersion := "2.11.8"

lazy val example = (project in file("."))
  .settings(
    fork in run := true,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" 
  )
