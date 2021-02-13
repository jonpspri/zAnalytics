ThisBuild / organization := "com.ibm.scalademo"

lazy val root = (project in file("."))
  .settings(
    name := "izode-demo",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.1.0",
    ),
    defaultExcludes in Compile in unmanagedResources := "*.example"
  )
