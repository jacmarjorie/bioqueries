name := "gwas"

version := "0.1"

scalaVersion := "2.11.8"
val sparkVersion = "2.4.0"

resolvers += "typesafe" at "https://repo.typesafe.com/typesafe/releases/"
libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
libraryDependencies += "com.github.samtools" % "htsjdk" % "2.9.1"
libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.0.0"
libraryDependencies += "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2"
libraryDependencies += "com.typesafe.slick" %% "slick" % "3.3.2"
