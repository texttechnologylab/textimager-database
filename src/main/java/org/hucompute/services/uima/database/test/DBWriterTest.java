package org.hucompute.services.uima.database.test;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.pipeline.SimplePipeline.runPipeline;

import java.io.File;
import java.io.IOException;

import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.hucompute.services.uima.database.mongo.MongoWriter;
import org.hucompute.services.uima.database.xmi.XmiReaderModified;

public class DBWriterTest {
	public static void main(String... args) throws UIMAException, IOException {

		CollectionReader reader = CollectionReaderFactory.createReader(
				XmiReaderModified.class,
				XmiReaderModified.PARAM_PATTERNS, "[+]**/*.xmi.gz",
				XmiReaderModified.PARAM_SOURCE_LOCATION, "testdata/biologie_sample",
				XmiReaderModified.PARAM_LANGUAGE, "de");

		runPipeline(reader,
				// getNeo4JWriter()
				getMongoWriter()
				// getCassandraWriter()
				// getBasexWriter()
				// getMysqlWriter()
				// getXMIWriter()
		);

	}
	
	public static AnalysisEngine getMongoWriter() throws ResourceInitializationException{
		return createEngine(
				MongoWriter.class, 
				MongoWriter.PARAM_DB_HOST, "127.0.0.1:27017",
				MongoWriter.PARAM_DB_DBNAME, "test_with_indexes",
				MongoWriter.PARAM_DB_COLLECTIONNAME, "wikipedia",
				MongoWriter.PARAM_DB_USER, "",
				MongoWriter.PARAM_DB_PW, "",				
				MongoWriter.PARAM_LOG_FILE_LOCATION,new File("dbtest/mongo_with_index.log"));
	}

}
