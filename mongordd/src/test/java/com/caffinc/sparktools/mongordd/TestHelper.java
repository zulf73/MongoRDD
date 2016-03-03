package com.caffinc.sparktools.mongordd;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.Document;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory$;

import java.io.Serializable;
import java.util.Map;

/**
 * Helper class for testing MongoRDD, made serializable
 *
 * @author Sriram
 */
public class TestHelper implements Serializable {
    private static final SparkContext SPARK_CONTEXT = new SparkContext("local[*]", "MongoRDD Test");

    /**
     * Creates a new JavaRDD from the MongoRDD and calculates the number of even and odd documents in the database
     *
     * @param mongoClientUri URI for MongoDB (Embedded)
     * @param database       Database to read data from
     * @param collection     Collection to read data from
     * @return Result of computing even and odd documents
     */
    public Map<String, Integer> run(String mongoClientUri, String database, String collection) {
        return new JavaRDD<>(new MongoRDD(SPARK_CONTEXT, mongoClientUri, database, collection, new Document(), 2), ClassManifestFactory$.MODULE$.fromClass(Document.class)).mapToPair(
                new PairFunction<Document, String, Integer>() {
                    public Tuple2<String, Integer> call(Document document) throws Exception {
                        if ((Integer) document.get("value") % 2 == 0) {
                            return new Tuple2<>("even", 1);
                        } else {
                            return new Tuple2<>("odd", 1);
                        }
                    }
                }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).collectAsMap();
    }
}
