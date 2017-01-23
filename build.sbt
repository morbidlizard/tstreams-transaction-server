name := "tstreams-transaction-server"

version := "1.0"

scalaVersion := "2.12.1"

//scalacOptions ++= Seq("-unchecked", "-deprecation")

val sroogeGenOutput = "src/main/thrift/gen"

ScroogeSBT.autoImport.scroogeThriftOutputFolder in Compile ~= (base => base / sroogeGenOutput)


ScroogeSBT.autoImport.scroogeBuildOptions in Compile := Seq()
unmanagedSourceDirectories in Compile += baseDirectory.value / "src/main/resources"
managedSourceDirectories in Compile += baseDirectory.value / sroogeGenOutput
unmanagedSourceDirectories in Test += baseDirectory.value / "src" / "it" / "scala"
parallelExecution in Test := false

resolvers ++= Seq(
  "twitter-repo" at "https://maven.twttr.com",
  "Oracle Maven2 Repo" at "http://download.oracle.com/maven"
)


libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.5",
  "com.twitter" %% "scrooge-core" % "4.13.0",
  "com.twitter" % "libthrift" % "0.5.0-7",
  "org.rocksdb" % "rocksdbjni" % "4.11.2",
  "com.sleepycat" % "je" % "7.0.6",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "io.netty" % "netty-all" % "4.1.6.Final",

  "org.slf4j" % "slf4j-log4j12" % "1.7.22",

  "org.apache.curator" % "curator-framework" % "2.11.1",
  "org.apache.curator" % "curator-test" % "2.11.1",
  "org.apache.curator" % "curator-recipes" % "2.11.1"
)