name := "Lab2-RDD"
version := "0.1"
scalaVersion := "2.11.8"

fork in run := true
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1")
