import bintray.AttrMap
import bintray._

name := """lib-spark-manager"""

organization := "com.gilt"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.slf4j"               %  "slf4j-api"                % "1.7.7",
  "ch.qos.logback"          %  "logback-classic"          % "1.1.2",
  "fr.janalyse"             %% "janalyse-ssh"             % "0.9.13",
  "org.mockito"             %  "mockito-all"              % "1.9.5"          % "test",
  "org.specs2"              %% "specs2"                   % "2.4.1"          % "test",
  "org.scalatest"           %% "scalatest"                % "2.1.7"          % "test"
)

resolvers += "JAnalyse Repository" at "http://www.janalyse.fr/repository/"

publishMavenStyle := false

bintrayPublishSettings

bintray.Keys.repository in bintray.Keys.bintray := "repo"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

bintray.Keys.bintrayOrganization in bintray.Keys.bintray := None

