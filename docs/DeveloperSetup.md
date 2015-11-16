# Developer Setup

Fire is mainly written in Java and a bit in Scala. It uses maven. The parent pom has 2 modules at this time:

* core
* examples

The number of modules would grow over time with things like customer 360, recommendations, various verticals etc.
getting added.

## Checking out the code with Git

git clone https://github.com/FireProjects/fire.git

## Building with maven

mvn package

## Importing into IntelliJ

IntelliJ can be downloaded from https://www.jetbrains.com/idea/

Add the scala plugin into IntelliJ. Then import the project as a Maven project into IntelliJ. Start with executing the
example workflows.

## Importing into Scala IDE for Eclipse

Fire can be imported into Scala IDE for Eclipse as a Maven project.

http://scala-ide.org/

Easiest way to get started it to run the example workflows under examples/src/main/java/fire/examples/workflow in your IDE.
