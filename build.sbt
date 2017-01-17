name := "tstreams-transaction-server"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation")

val sroogeGenOutput = "src/main/thrift/gen"

ScroogeSBT.autoImport.scroogeThriftOutputFolder in Compile <<= baseDirectory {
  base => base / sroogeGenOutput
}


ScroogeSBT.autoImport.scroogeBuildOptions in Compile := Seq()
unmanagedSourceDirectories in Compile += baseDirectory.value / "src/main/resources"
managedSourceDirectories in Compile += baseDirectory.value / sroogeGenOutput
managedSourceDirectories in Test += baseDirectory.value / "src" / "it"

resolvers ++= Seq(
  "twitter-repo" at "https://maven.twttr.com",
  "Oracle Maven2 Repo" at "http://download.oracle.com/maven"
)

libraryDependencies ++= Seq(
  "com.twitter" % "scrooge-core_2.11" % "4.13.0",
  "com.twitter" % "finagle-thrift_2.11" % "6.41.0",
  "org.rocksdb" % "rocksdbjni" % "5.0.1",
  "com.sleepycat" % "je" % "7.0.6",
  "org.scalactic" %% "scalactic" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "com.storm-enroute" % "scalameter_2.11" % "0.8.1",
  "io.netty" % "netty-all" % "4.1.6.Final",

  "org.slf4j" % "slf4j-simple" % "1.7.22",
  "org.apache.curator" % "curator-framework" % "2.11.1",
  "org.apache.curator" % "curator-recipes" % "2.11.1"
)