package com.caffinc.sparktools.mongordd;

import com.mongodb.*;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoCollection;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.bson.Document;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests the MongoRDD class
 *
 * @author Sriram
 */
public class MongoRDDTest {
    private static final Logger LOG = LoggerFactory.getLogger(MongoRDDTest.class);
    private static final String DATABASE = "test";
    private static final String COLLECTION = "emailsvc";

    private static MongodExecutable mongodExecutable = null;
    private static MongodProcess mongod = null;
    private static String mongoClientUri = null;

    /**
     * Starts an embedded MongoDB instance and writes dummy documents into it
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        final int port = 12345;
        new Thread() {
            @Override
            public void run() {
                try {
                    MongodStarter starter = MongodStarter.getDefaultInstance();
                    IMongodConfig mongodConfig = new MongodConfigBuilder()
                            .version(Version.Main.PRODUCTION)
                            .net(new Net(port, Network.localhostIsIPv6()))
                            .build();

                    mongodExecutable = starter.prepare(mongodConfig);
                    mongod = mongodExecutable.start();
                    //mongoClientUri = "mongodb://localhost:" + port;
                    mongoClientUri = "mongodb://unique:unique@cluster0-shard-00-02.3cmqe.mongodb.net:27017/";
                    MongoClient mongo = new MongoClient(new MongoClientURI(mongoClientUri));
                    MongoCollection collection = mongo.getDatabase(DATABASE).getCollection(COLLECTION);

                    MongoCursor<Document> cursor = collection.find().iterator();
                    try {
                        while (cursor.hasNext()) {
                        //System.out.println(cursor.next().toJson());
                        LOG.info(cursor.next().toJson());
                        }
                    } finally {
                        cursor.close();
                    }
                    mongo.close();
                    LOG.info("Wrote dummy database entries");
                } catch (Exception e) {
                    LOG.error("Could not start embedded MongoDB", e);
                }
            }
        }.start();
        long startTime = System.currentTimeMillis();
        while (mongod == null || !mongod.isProcessRunning()) {
            Thread.sleep(100); // Await Mongod start
            if (System.currentTimeMillis() - startTime > 10000L) {
                Assert.fail("MongoDB didn't start on time");
            }
        }
        LOG.info("Started embedded MongoDB");
    }

    private static List<Document> generateDummyDocuments() {
        LOG.info("Generating dummy documents");
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(new Document("_id", i).append("value", i));
        }
        return documents;
    }

    /**
     * Executes the computation on a local Spark cluster and tests the results
     *
     * @throws Exception
     */
    @Test
    public void testMongoRDD() throws Exception {
        LOG.info("Computing even and odd numbers");
        Map<String, Integer> map = new TestHelper().run(mongoClientUri, DATABASE, COLLECTION);
        Assert.assertEquals("Count of Even numbers in the database should be 50", 50, map.get("even").intValue());
        Assert.assertEquals("Count of Odd numbers in the database should be 50", 50, map.get("odd").intValue());
    }

    /**
     * Stops the embedded MongoDB instance
     *
     * @throws Exception
     */
    @AfterClass
    public static void tearDown() throws Exception {
        if (mongodExecutable != null) {
            mongodExecutable.stop();
            LOG.info("Shutdown embedded MongoDB");
        }
    }
}
