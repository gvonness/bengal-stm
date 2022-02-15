ThisBuild / baseVersion := "0.1.2"
ThisBuild / organization := "ai.entrolution"
ThisBuild / organizationName := "Entrolution"
ThisBuild / publishGithubUser := "gvonness"
ThisBuild / publishFullName := "Greg von Nessi"
ThisBuild / startYear := Some(2020)
ThisBuild / endYear := Some(2021)

ThisBuild / homepage := Some(url("https://github.com/gvonness/bengal-stm"))
ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/gvonness/bengal-stm"),
          "git@github.com:gvonness/bengal-stm.git"
  )
)

ThisBuild / spiewakCiReleaseSnapshots := false
ThisBuild / spiewakMainBranches := Seq("main")

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

name := "bengal-stm"

version := "0.1.2"

scalaVersion := "2.13.8"
crossScalaVersions := Seq("2.12.15", "2.13.8")

Global / excludeLintKeys += idePackagePrefix

libraryDependencies += "org.typelevel" %% "cats-free"   % "2.7.0"
libraryDependencies += "org.typelevel" %% "cats-effect" % "3.3.4"
libraryDependencies += "org.scalatest" %% "scalatest"   % "3.2.11" % "test"

idePackagePrefix := Some("ai.entrolution")

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-encoding",
  "UTF-8",
  "-Xlint:_",
  "-Ywarn-unused:-implicits",
  "-Ywarn-value-discard",
  "-Ywarn-dead-code"
)

enablePlugins(SonatypeCiReleasePlugin)
