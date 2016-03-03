# MongoRDD
Simple MongoRDD to read data from MongoDB into Spark

## Build Status
![MongoRDD Travis-CI Build Status](https://travis-ci.org/caffinc/MongoRDD.svg?branch=master)

## Dependencies

These are not absolute, but are current (probably) as of 3rd March, 2016. It should be trivial to upgrade or downgrade versions as required.

    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.11</artifactId>
            <version>1.6.0</version>
        </dependency>
        <dependency>
            <groupId>org.mongodb</groupId>
            <artifactId>mongo-java-driver</artifactId>
            <version>3.2.2</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-log4j12</artifactId>
            <version>1.7.18</version>
        </dependency>
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>de.flapdoodle.embed</groupId>
            <artifactId>de.flapdoodle.embed.mongo</artifactId>
            <version>1.50.2</version>
            <scope>test</scope>
        </dependency>
    </dependencies>


## Usage

MongoRDD extends the Spark RDD class and provides a way to read from MongoDB directly into Spark.

Usage in Scala:
	
	val sc = new SparkContext(conf)
	val mongoClientUri = "mongodb://localhost:27017"
	val database = "DBName"
	val collection = "CollectionName"
	val query = new Document(...)
	val partitions = 4
	new MongoRDD(sc, mongoClientUri, database, collection, query, partitions).map(...)

Usage in Java (Assume constants are the same):
	
	new JavaRDD<>(
        new MongoRDD(sc, mongoClientUri, database, collection, query, partitions),
	    ClassManifestFactory$.MODULE$.fromClass(Document.class)
    ).map(...)

[MongoClientURI](https://docs.mongodb.org/manual/reference/connection-string/ "Mongo Connection String") is one of the simplest ways to connect to a MongoDB instance. Feel free to extend this, and raise a Pull Request if you think it should be included in this repo.

## Tests

There is just one extensive test, which launches an embedded MongoDB instance and writes dummy values into it and tests the MongoRDD on a local Spark instance. The test Works on my Machine™ and Travis-CI (Which is awesome!).

It might not work on your machine for the following reasons:

* It uses an [Embedded MongoDB](https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo) instance, which requires several megabytes of download the first time it runs. This might be slow, and the test might timeout. Comment out the line which makes the `setUp()` fail on slow starts and try it out.
* You might have an older version of Spark in your dependencies which might have a bug while running on Windows. Are you able to run Spark for other stuff without issues?
* You're channeling evil spirits which don't like MongoDB. Pray to your God and hope for the best, or send me an email (admin@caffinc.com) if you think I can help :)

## Ideas

There are a few things that can be done to extend this:

* Provide other means of connecting to MongoDB
* Make the RDD generic, and provide an interface to convert BSON documents to other formats before returning (This can be achieved with a simple call to `map()` so it wasn't done)
* Make it available in Maven Central or Bintray
* Add more tests

If you can help with one or more of the above, or if you have suggestions of your own, send me an email or raise a PR and I will review it and add it.

## Help

If you face any issues trying to get this to work for you, shoot me an email: admin@caffinc.com.

Good luck!
