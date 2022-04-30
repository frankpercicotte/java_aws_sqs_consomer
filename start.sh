
export CLASSPATH=target/sqs_consomer-1.0-SNAPSHOT.jar
export className=App
echo "## Running $className..."ß
mvn exec:java -Dexec.mainClass="br.com.sqs_consomer.$className"