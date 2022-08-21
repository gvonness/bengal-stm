ThisBuild / baseVersion := "0.5.0"

ThisBuild / organization := "ai.entrolution"
ThisBuild / organizationName := "Greg von Nessi"
ThisBuild / publishGithubUser := "gvonness"
ThisBuild / publishFullName := "Greg von Nessi"

ThisBuild / homepage := Some(url("https://github.com/gvonness/bengal-stm"))
ThisBuild / scmInfo := Some(
  ScmInfo(url("https://github.com/gvonness/bengal-stm"),
          "git@github.com:gvonness/bengal-stm.git"
  )
)

ThisBuild / startYear := Some(2020)
ThisBuild / endYear := Some(2022)

ThisBuild / spiewakCiReleaseSnapshots := false
ThisBuild / spiewakMainBranches := Seq("main")

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / sonatypeRepository := "https://s01.oss.sonatype.org/service/local"

ThisBuild / scalaVersion := DependencyVersions.scala2p13Version
ThisBuild / crossScalaVersions := Seq(
  DependencyVersions.scala2p13Version,
  DependencyVersions.scala2p12Version,
  DependencyVersions.scala2p11Version
)

Global / idePackagePrefix := Some("ai.entrolution")
Global / excludeLintKeys += idePackagePrefix

lazy val commonSettings = Seq(
  scalaVersion := DependencyVersions.scala2p13Version,
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
)

lazy val bengalStm = (project in file("."))
  .enablePlugins(SonatypeCiReleasePlugin)
  .settings(
    commonSettings,
    name := "bengal-stm",
    libraryDependencies ++= Dependencies.bengalStm
  )
