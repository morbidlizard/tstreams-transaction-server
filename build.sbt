name := "tstreams-transaction-server"

version := "1.0"

scalaVersion := "2.11.8"


val sroogeGenOutput = "src/main/thrift/gen"

ScroogeSBT.autoImport.scroogeThriftOutputFolder in Compile <<= baseDirectory {
  base => base / sroogeGenOutput
}
unmanagedSourceDirectories in Compile += baseDirectory.value / "src/main/resources"
managedSourceDirectories in Compile += baseDirectory.value / sroogeGenOutput

resolvers ++= Seq(
  "twitter-repo" at "https://maven.twttr.com",
  "Oracle Maven2 Repo" at "http://download.oracle.com/maven"
)

libraryDependencies ++= Seq(
  "org.apache.thrift" % "libthrift" % "0.5.0-1",
  "com.twitter" % "scrooge-core_2.11" % "4.11.0",
  "com.twitter" % "twitter-server_2.11" % "1.24.0",
  "com.twitter" % "finagle-thrift_2.11" % "6.39.0",
  "org.rocksdb" % "rocksdbjni" % "4.11.2",
  "com.sleepycat" % "je" % "7.0.6",
  "org.scalactic" %% "scalactic" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
  "com.storm-enroute" % "scalameter_2.11" % "0.8.1",
  "com.pauldijou" %% "jwt-core" % "0.9.0",

  "org.slf4j" % "slf4j-simple" % "1.7.21",
  "org.apache.curator" % "curator-framework" % "2.11.0",
  "org.apache.curator" % "curator-recipes" % "2.11.0",
  "org.apache.curator" % "curator-x-discovery" % "2.11.0"
)