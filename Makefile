all: build benchmarks

build:
	mvn clean package

help: build
	java -jar example-account-jmh/target/benchmarks.jar -h

benchmarks:
	mkdir target
	java -jar example-account-jmh/target/benchmarks.jar -bm AverageTime -f 1 -t max -tu ms -o target/jmh.txt
