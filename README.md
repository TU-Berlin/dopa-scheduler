dopa-scheduler
==============

This project aims to schedule multiple sopremo jobs.

# Client API

## Compile instructions
Depending on whether you want to use the API in a maven project, or you just want a plain old jar file, you have two options:

1. Maven build:
  - Go into the project root directory and run
  ```
  mvn clean install -DsksipTests
  ```
  - This will install the project in your local maven repository. You can now use the artifacts within your projects.

2. Creating a single client API jar:
  - First complete the maven build as described above. This needs to be done as the build process needs to be able to access the compiled code of the other sub projects.
  - Then change into the meteor-scheduler-client subdirectory
  - Run the following command:
  ```
  mvn clean assembly:single -DskipTests
  ```
  - This will create the file
  ```
  meteor-scheduler-client/target/meteor-scheduler-client-0.4-SNAPSHOT-jar-with-dependencies.jar
  ```
  - You can now include this library in your project.

3. Installing the server
  - Change into the meteor-scheduler-server subdirectory
  - Run the following command:
  ```
  ./install
  ```

[![Build Status](https://travis-ci.org/TU-Berlin/dopa-scheduler.png)](https://travis-ci.org/TU-Berlin/dopa-scheduler)

