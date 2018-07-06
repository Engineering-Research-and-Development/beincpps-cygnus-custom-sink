version=1.9.0_SNAPSHOT
flumeBasePath=/usr/cygnus

# Export some Maven options
export MAVEN_OPTS="-Xmx512m -XX:MaxPermSize=128m"

# Compile
mvn clean compile assembly:single

# Install
sshpass -p "cygnus" scp target/cygnus-common-$version-jar-with-dependencies.jar cygnus@192.168.40.128:$flumeBasePath/plugins.d/cygnus/libext/
#mvn install:install-file -Dfile=$flumeBasePath/plugins.d/cygnus/libext/cygnus-common-$version-jar-with-dependencies.jar -DgroupId=com.telefonica.iot -DartifactId=cygnus-common -Dversion=$version -Dpackaging=jar -DgeneratePom=true
