/**
 * 
 */
package org.hobbit.benchmark.versioning.systems;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.benchmark.versioning.util.VirtuosoSystemAdapterConstants;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author papv
 *
 */
public class R43plesSystemAdapter extends AbstractSystemAdapter {
		
	private static final Logger LOGGER = LoggerFactory.getLogger(R43plesSystemAdapter.class);
	private boolean dataGenFinished = false;
	private boolean dataLoadingFinished = false;
	private int totalversions = 0;
	
	// must match the "Generated data format" parameter given when starting the experiment
	long initialDatasetsSize = 0;

	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing R43ples test system...");
        super.init();        
		LOGGER.info("R43ples initialized successfully .");
    }

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedData(byte[])
	 */
	public void receiveGeneratedData(byte[] data) {
		
		ByteBuffer dataBuffer = ByteBuffer.wrap(data);
		// read the file path
		String receivedFilePath = RabbitMQUtils.readString(dataBuffer);
		// read the file contents
		byte[] fileContentBytes = RabbitMQUtils.readByteArray(dataBuffer);
		
		FileOutputStream fos = null;
		String revision = null;
		
		// since r43ples only supports loading of triples to revision from one 
		// file we build the appropriate files per revision
		if(receivedFilePath.startsWith("/versioning/ontologies") || receivedFilePath.startsWith("/versioning/data/v0")) {
			revision = "/versioning/toLoad/initial-version.nt";
		} else {
			String version = receivedFilePath.substring(18, receivedFilePath.lastIndexOf("/"));
			revision = "/versioning/toLoad/changeset-add-" + version + ".nt";
		}
		
		try {
			File outputFile = new File(revision);	
			fos = FileUtils.openOutputStream(outputFile, true);
			IOUtils.write(fileContentBytes, fos);
			fos.close();
			LOGGER.info(receivedFilePath + " received from Data Generator. and stored to " + revision);
		} catch (FileNotFoundException e) {
			LOGGER.error("Exception while creating/opening files to write received data.", e);
		} catch (IOException e) {
			LOGGER.error("Exception while writing data file", e);
		}
	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedTask(java.lang.String, byte[])
	 */
	public void receiveGeneratedTask(String tId, byte[] data) {
		LOGGER.info("Task " + tId + " received from task generator");
		boolean taskExecutedSuccesfully = true;
		
		ByteBuffer taskBuffer = ByteBuffer.wrap(data);
		// read the query type
		String taskType = RabbitMQUtils.readString(taskBuffer);
		// read the query
		String queryText = RabbitMQUtils.readString(taskBuffer);
		
		byte[][] resultsArray = null;
		// 1 stands for ingestion task
		// 2 for storage space task
		// 3 for SPARQL query task
		switch (Integer.parseInt(taskType)) {
			case 1:
				if(dataGenFinished) {
					// get the version that will be loaded.
					int version = Integer.parseInt(queryText.substring(8, queryText.indexOf(",")));
					totalversions++;
					
					// TODO: measuring of loading time have to be changed
					long start = System.currentTimeMillis();
					loadVersion(version);
					long end = System.currentTimeMillis();					
					long loadingTime = end - start;
					LOGGER.info("Version " + version + " loaded successfully in "+ loadingTime + " ms.");
					
					int count = 0;
					LOGGER.info("Getting number of triples that successfully loaded in version " + version + ".");
					String triplesLoadedQuery = "select (count(*) as ?loaded_triples) "
							+ "from <http://test.com/r43ples> REVISION \"" + (version + 2) + "\" "
							+ "where { ?s ?p ?o }";
					
					ResultSet results = executeQuery("for getting loaded triples", triplesLoadedQuery);
					if(results.hasNext()) {
					    count = results.next().getLiteral("loaded_triples").getInt();
					}
					LOGGER.info(count + " triples loaded for version " + version + ".");
					
					// cannot query for getting the loading triples and then import again in r43ples
					// as tdb stays lock for querying. so i am restarting the server each time
					// a version is loaded to overpass this issue
					serverRestart();
					try {
						Thread.sleep(1000 * 10);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					// TODO 
					// in v2.0 of the benchmark the number of changes should be reported instead of 
					// loaded triples, as we will also have deletions except of additions of triples
					resultsArray = new byte[3][];
					resultsArray[0] = RabbitMQUtils.writeString(taskType);
					resultsArray[1] = RabbitMQUtils.writeString(Integer.toString(count));
					resultsArray[2] = RabbitMQUtils.writeString(Long.toString(loadingTime));
				}
				break;
			case 2:
				// get the storage space required for all versions to be stored in virtuoso
				long finalDatabasesSize = FileUtils.sizeOfDirectory(new File("/database/dataset"));
				LOGGER.info("Total datasets size: "+ finalDatabasesSize / 1000f + " Kbytes.");
				
				resultsArray = new byte[2][];
				resultsArray[0] = RabbitMQUtils.writeString(taskType);
				resultsArray[1] = RabbitMQUtils.writeString(Long.toString(finalDatabasesSize));
			
				break;
			case 3:
				if(dataLoadingFinished) {
					String queryType = queryText.substring(21, 22);
					LOGGER.info("queryType: " + queryType);

					// rewrite queries in order to be answered
					String rewrittenQuery = rewriteQuery(queryType, queryText);

//					ResultSet results =  executeQuery_2(tId, rewrittenQuery);
					ResultSet results =  executeQuery(tId, rewrittenQuery);
					ByteArrayOutputStream queryResponseBos = new ByteArrayOutputStream();
					ResultSetFormatter.outputAsJSON(queryResponseBos, results);
					
					int resultSize = results.getRowNumber();
					
					resultsArray = new byte[4][];
					resultsArray[0] = RabbitMQUtils.writeString(taskType);
					resultsArray[1] = RabbitMQUtils.writeString(queryType);
					resultsArray[2] = RabbitMQUtils.writeString(Integer.toString(resultSize));
					resultsArray[3] = queryResponseBos.toByteArray();
					LOGGER.info("results: " + resultSize);
				}
				break;
		}

		byte[] results = RabbitMQUtils.writeByteArrays(resultsArray);
		try {
			sendResultToEvalStorage(tId, results);
			LOGGER.info("Results sent to evaluation storage" + (taskExecutedSuccesfully ? "." : " for unsuccessful executed task."));
		} catch (IOException e) {
			LOGGER.error("Exception while sending storage space cost to evaluation storage.", e);
		}
	}
	
	public String rewriteQuery(String queryType, String queryText) {
		String rewrittenQuery = "";
		String graphName = "http://test.com/r43ples";
		Pattern graphPattern = Pattern.compile("graph.version.(\\d+)>");
		
		Matcher graphMatcher = graphPattern.matcher(queryText);
		while (graphMatcher.find()) {
			int version = Integer.parseInt(graphMatcher.group(1));
			int revision = version + 2;
			queryText = queryText.replaceAll("<http://graph.version." + version + ">", "<" + graphName + "> REVISION \"" + revision + "\"");
		}
		try {
			rewrittenQuery = URLEncoder.encode(queryText, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOGGER.error("Error occured while trying to encode the query string", e);
		};
		return rewrittenQuery;
	}
	
	private String loadVersion(int versionNum) {
		LOGGER.info("Loading version " + versionNum + "...");
		String answer = null;
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "load.sh";
			String[] command = {"/bin/bash", scriptFilePath, Integer.toString(versionNum) };
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				LOGGER.info(line);		
			}
			p.waitFor();
			LOGGER.info("Version " + versionNum + " loaded successfully.");
			in.close();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		}	
		return answer;
	}
	
	private void serverRestart() {
		LOGGER.info("Restarting R43ples...");
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "server_restart.sh";
			String[] command = {"/bin/bash", scriptFilePath };
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				LOGGER.info(line);		
			}
			p.waitFor();
			in.close();
			LOGGER.info("R43ples restarted successfully.");
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		}	
	}
	
	private ResultSet executeQuery(String taskId, String query) {
		LOGGER.info("Executing task " + taskId + "..." );
		ResultSet results = null;
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "execute_query.sh";
			String[] command = {"/bin/bash", scriptFilePath, query};
			Process p = new ProcessBuilder(command).start();
			results = ResultSetFactory.fromJSON(p.getInputStream());
			p.waitFor();
			LOGGER.info("Task " + taskId + " executed successfully.");
		} catch (IOException e) {
            LOGGER.error("Exception while executing task " + taskId, e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing task " + taskId, e);
		}
		return results;
	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
    	if (VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED == command) {
			LOGGER.info("Received signal from Data Generator that data generation finished.");
    		dataGenFinished = true;
    	} else if(VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED == command) {
			LOGGER.info("Received signal that all generated data loaded successfully.");
    		dataLoadingFinished = true;
    	}
    	super.receiveCommand(command, data);
    }

	@Override
    public void close() throws IOException {
		LOGGER.info("Closing System Adapter...");
        // Always close the super class after yours!
        super.close();
        //
		LOGGER.info("System Adapter closed successfully.");

    }
}
