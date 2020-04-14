mvn install:install-file -Dfile=./libs/cglib-nodep-2.2.jar -DgroupId=cglib \
-DartifactId=cglib -Dversion=2.2 -Dpackaging=jar
mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file -DgroupId=com.hp.hpl.jena \
-Dfile=./libs/jena-arq-2.9.3.jar
mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file -DgroupId=com.hp.hpl.jena \
-Dfile=./libs/jena-core-2.7.3.jar