ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "azurebench",
    organization := "com.github.fhackett",

    libraryDependencies += "org.rogach" %% "scallop" % "4.1.0",
    libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.8.1",
    libraryDependencies += "com.lihaoyi" %% "upickle" % "2.0.0",

    libraryDependencies += "com.hierynomus" % "sshj" % "0.33.0",

    libraryDependencies += "com.azure.resourcemanager" % "azure-resourcemanager" % "2.14.0",
    libraryDependencies += "com.azure" % "azure-identity" % "1.5.1",

    // so the Azure libs can correctly log stuff
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36",
  )
