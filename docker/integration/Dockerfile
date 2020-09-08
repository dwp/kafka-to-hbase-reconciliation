FROM dwp-gradle-reconciliation:latest

COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
COPY src/ ./src
COPY build/ ./build
COPY .gradle/ ./gradle

<<<<<<< HEAD
#RUN gradle :unit build integrationClasses -x test -x unit -x integration-test
RUN gradle compileKotlin build integrationClasses -x test -x unit -x integration-test
=======
RUN gradle :unit build integrationClasses -x test -x unit -x integration-test
>>>>>>> origin/master
