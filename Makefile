.PHONY: build image submit-job spark-base-image

spark-version := 2.3.1
SPARK_HOME=/usr/local/Cellar/apache-spark/2.3.1/libexec

build:
	sbt clean package

image:
	sbt clean package
	docker build -t inanme/my-spark:0  .
	docker push inanme/my-spark:0

submit-job:
	spark-submit \
		--master k8s://localhost:6443 \
		--deploy-mode cluster \
		--name estimate-pi \
		--class inanme.spark.EstimatePi \
		--conf spark.executor.instances=2 \
		--conf spark.kubernetes.container.image.pullPolicy=Always \
		--conf spark.kubernetes.container.image=inanme/my-spark:0 \
		local:///opt/spark/jars/my-spark_2.11-0.jar

spark-base-image:
	curl "http://www-eu.apache.org/dist/spark/spark-${spark-version}/spark-${spark-version}-bin-hadoop2.7.tgz" -o spark.tgz
	mkdir -p spark && tar xvf spark.tgz --strip 1 -C spark/
	cd spark && docker build -t inanme/spark:${spark-version} -f kubernetes/dockerfiles/spark/Dockerfile .
	rm -rf spark.tgz spark/
	docker push inanme/spark:${spark-version}
