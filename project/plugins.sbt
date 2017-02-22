resolvers += Resolver.url("YPG-Data SBT Plugins", url("https://dl.bintray.com/ypg-data/sbt-plugins"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.mediative.sbt" % "sbt-mediative-core" % "0.3.1")
addSbtPlugin("com.mediative.sbt" % "sbt-mediative-oss" % "0.3.1")
addSbtPlugin("com.mediative.sbt" % "sbt-mediative-devops" % "0.3.1")
addSbtPlugin("com.github.tkawachi" % "sbt-doctest" % "0.4.1")
