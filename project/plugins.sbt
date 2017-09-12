logLevel := Level.Warn

addSbtPlugin("com.twitter" % "scrooge-sbt-plugin" % "4.20.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.11")
libraryDependencies += "com.trueaccord.scalapb" %% "compilerplugin" % "0.6.2"