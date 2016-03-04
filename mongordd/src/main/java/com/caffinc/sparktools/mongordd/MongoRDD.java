package com.caffinc.sparktools.mongordd;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCursor;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory$;

/**
 * Simple MongoRDD class which allows reading data out of Spark
 *
 * @author Sriram
 */
public class MongoRDD extends RDD<Document> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoRDD.class);
    private String mongoClientUriString;
    private String database;
    private String collection;
    private Document query;
    private int partitions;
    private int batchSize;

    /**
     * Constructs a MongoRDD which can be used as a starting point for RDD transformations
     *
     * @param sc                   Spark Context for this RDD
     * @param mongoClientUriString MongoClientURI to be used to connect to the MongoDB instance
     * @param database             Database name
     * @param collection           Collection name
     * @param query                Find Query to be used for fetching data from MongoDB
     * @param partitions           Partitions to break the query into. Skip and Limit for the find query run on MongoDB is computed using the number of partitions.
     */
    public MongoRDD(SparkContext sc, String mongoClientUriString, String database, String collection, Document query, int partitions) {
        super(sc, new ArrayBuffer<Dependency<?>>(), ClassManifestFactory$.MODULE$.fromClass(Document.class));
        this.mongoClientUriString = mongoClientUriString;
        this.database = database;
        this.collection = collection;
        this.query = query;
        this.partitions = partitions;
        long total = new MongoClient(new MongoClientURI(mongoClientUriString)).getDatabase(database).getCollection(collection).count(query);
        this.batchSize = (int) Math.ceil((double) total / partitions);
    }

    /**
     * Provides an Iterator for the Documents coming from MongoDB for the given partition
     *
     * @param partition   MongoMapPartition corresponding to this partition
     * @param taskContext Context of this task
     * @return MongoMapIterator to the underlying MongoDB, which is a wrapper for the MongoCursor returned by MongoDB's find query
     */
    @Override
    public Iterator<Document> compute(Partition partition, TaskContext taskContext) {
        LOG.info("Iterating partition {}", partition);
        return new MongoMapIterator(new MongoClient(new MongoClientURI(mongoClientUriString)).getDatabase(database).getCollection(collection)
                .find(query).skip(((MongoMapPartition) partition).from).limit(this.batchSize).iterator());
    }

    /**
     * Partitions the query into batches based on the number of partitions reqested
     *
     * @return Array of MongoMapPartitions
     */
    @Override
    public Partition[] getPartitions() {
        Partition[] partitionArray = new Partition[partitions];
        for (int i = 0; i < partitions; i++) {
            partitionArray[i] = new MongoMapPartition(i, i * batchSize, batchSize);
        }
        return partitionArray;
    }

    /**
     * AbstractIterator wrapper for MongoCursor
     *
     * @author Sriram
     */
    private class MongoMapIterator extends AbstractIterator<Document> {
        private MongoCursor cursor;

        /**
         * Constructor for wrapping the MongoCursor
         *
         * @param cursor MongoCursor to wrap
         */
        public MongoMapIterator(MongoCursor cursor) {
            this.cursor = cursor;
        }

        /**
         * Wrapper method for MongoCursor.hasNext()
         *
         * @return true if cursor has an object available, false otherwise
         */
        public boolean hasNext() {
            return cursor.hasNext();
        }

        /**
         * Wrapper method for MongoCursor.next()
         *
         * @return Next document in the cursor
         */
        public Document next() {
            return (Document) cursor.next();
        }
    }


    /**
     * Implements Spark Partitions for MongoDB queries, providing a way to partition find queries
     *
     * @author Sriram
     */
    private class MongoMapPartition implements Partition {
        private static final long serialVersionUID = 1L;
        private int index;
        private int from;
        private int batchSize;

        /**
         * Constructor for a new Partition
         *
         * @param index     Index of this partition
         * @param from      MongoDB index to start the query from, corresponds to the skip() used while calling find()
         * @param batchSize MongoDB batchSize to limit the query to, corresponds to the limit() used while calling find()
         */
        public MongoMapPartition(int index, int from, int batchSize) {
            this.index = index;
            this.from = from;
            this.batchSize = batchSize;
        }

        /**
         * Returns own index
         *
         * @return Index of this partition
         */
        public int index() {
            return index;
        }

        /**
         * Checks partition equality
         *
         * @param obj Partition object to compare with
         * @return true if equal, false otherwise
         */
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof MongoMapPartition) && ((MongoMapPartition) obj).index != index;
        }

        /**
         * Hashcode of this partition
         *
         * @return Simple hashCode, which is the index of this partition
         */
        @Override
        public int hashCode() {
            return index();
        }

        @Override
        public String toString() {
            return "MongoMapPartition[index=" + index + ", from=" + from + ", batchSize=" + batchSize + "]";
        }
    }
}