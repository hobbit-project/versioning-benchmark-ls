package org.hobbit.benchmark.versioning.components;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.hobbit.benchmark.versioning.Task;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.benchmark.versioning.util.SystemAdapterConstants;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data Generator class for Versioning Benchmark.
 * 
 * @author Vassilis Papakonstantinou (papv@ics.forth.gr)
 *
 */
public class VersioningDataGenerator extends AbstractDataGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningDataGenerator.class);
	
	// configuration parameters
	private int numberOfVersions;
	private int v0TotalSizeInTriples;
	private String sentDataForm;
	private String enabledQueryTypesParam;
	
	private String datasetPath = "/versioning/data";
	private String resultsPath = "/versioning/results";
	private String queriesPath = "/versioning/queries";

	private int[] triplesExpectedToBeAdded;
	private int[] triplesExpectedToBeDeleted;
	private int[] triplesExpectedToBeLoaded;
	
	private Properties enabledQueryTypes = new Properties();
	private boolean allQueriesDisabled = true;

	private AtomicInteger numberOfmessages = new AtomicInteger(0);
	
	private ArrayList<Task> tasks = new ArrayList<Task>();
	private Semaphore versionLoadedFromSystemMutex = new Semaphore(0);
		
	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing Data Generator '" + getGeneratorId() + "'");
		super.init();
		
		// Initialize data generation parameters through the environment variables given by user
		initFromEnv();
		
		// get the enabled queries 
		Pattern pattern = Pattern.compile("QT([1-8])=([0|1])[^\\w]*");
		Matcher matcher = pattern.matcher(enabledQueryTypesParam);
		String enabledQueryTypesParamProp = "";
		while (matcher.find()) {
			allQueriesDisabled = allQueriesDisabled ? !matcher.group(2).equals("1") : false;
			enabledQueryTypesParamProp += "QT" + matcher.group(1) + "=" + matcher.group(2) + "\n";
		}
		enabledQueryTypes.load(new StringReader(enabledQueryTypesParamProp));

		// TODO: download dataset info: triples to be added/deleted/loaded per version
		getDatasetInfoFromFTP();
		
		// TODO: download dataset
		getDatasetFromFTP();
		
		triplesExpectedToBeAdded = new int[numberOfVersions];
		triplesExpectedToBeDeleted = new int[numberOfVersions];
		triplesExpectedToBeLoaded = new int[numberOfVersions];

		// if all query types are disabled skip this part
		if(!allQueriesDisabled) {
			// TODO: download queries and expected results
			getQueriesFromFTP();
		}
		LOGGER.info("Data Generator initialized successfully.");
	}
	
	public void initFromEnv() {
		LOGGER.info("Getting data generator configuration parameters...");
		
		Map<String, String> env = System.getenv();
		String v0TotalSizeInTriplesStr = (String) getFromEnv(env, VersioningConstants.V0_SIZE_IN_TRIPLES, "");
		v0TotalSizeInTriples = v0TotalSizeInTriplesStr.equals("1.000.000") ? 1000000 : v0TotalSizeInTriplesStr.equals("5.000.000") ? 5000000 : 10000000;
		numberOfVersions = (Integer) getFromEnv(env, VersioningConstants.NUMBER_OF_VERSIONS, 0);
		sentDataForm = (String) getFromEnv(env, VersioningConstants.SENT_DATA_FORM, "");
		enabledQueryTypesParam = (String) getFromEnv(env, VersioningConstants.ENABLED_QUERY_TYPES, "");
	}
	
	public void getDatasetInfoFromFTP() {
		
	}
	
	public void getDatasetFromFTP() {
		
	}
	
	public void getQueriesFromFTP() {
		int taskId = 0;
		int querySubstParamCount = 0;
		String queriesPath = System.getProperty("user.dir") + File.separator + "queries" + File.separator;
		String queryString;
		
		// QT2
		if(enabledQueryTypes.getProperty("QT2", "0").equals("1")) {
			
		}
		
		// QT4
		// if the total number of versions is lower than 1 there are no historical versions
		if(enabledQueryTypes.getProperty("QT4", "0").equals("1")) {
			
		} 
		
		// QT6
		// same querySubstParamCount as QT5
		if(enabledQueryTypes.getProperty("QT6", "0").equals("1")) {
			
		}

		// QT7
		// can not be supported when we have 2 or less total versions, as there cannot exist cross-deltas
		if(enabledQueryTypes.getProperty("QT7", "0").equals("1")) {
			
		}

		// QT8
		if(enabledQueryTypes.getProperty("QT8", "0").equals("1")) {
			
		}
	}
	
	/**
     * A generic method for initialize benchmark parameters from environment variables
     * 
     * @param env		a map of all available environment variables
     * @param parameter	the property that we want to get
     * @param paramType	a dummy parameter to recognize property's type
     */
	@SuppressWarnings("unchecked")
	private <T> T getFromEnv(Map<String, String> env, String parameter, T paramType) {
		if (!env.containsKey(parameter)) {
			LOGGER.error(
					"Environment variable \"" + parameter + "\" is not set. Aborting.");
            throw new IllegalArgumentException(
            		"Environment variable \"" + parameter + "\" is not set. Aborting.");
        }
		try {
			if (paramType instanceof String) {
				return (T) env.get(parameter);
			} else if (paramType instanceof Integer) {
				return (T) (Integer) Integer.parseInt(env.get(parameter));
			} else if (paramType instanceof Long) {
				return (T) (Long) Long.parseLong(env.get(parameter));
			} else if (paramType instanceof Double) {
				return (T) (Double) Double.parseDouble(env.get(parameter));
			}
        } catch (Exception e) {
        	throw new IllegalArgumentException(
                    "Couldn't get \"" + parameter + "\" from the environment. Aborting.", e);
        }
		return paramType;
	}
	
	
	@Override
	// This method is used for sending the already generated data, tasks and gold standard
	// to the appropriate components.
	protected void generateData() throws Exception {
		try {						
			// Send data files to the system.
			// Starting for version 0 and continue to the next one until reaching the last version.
			// Waits for signal sent by the system that determines that the sent version successfully 
			// loaded, in order to proceed with the next one
	    	for(int version = 0; version < numberOfVersions; version++) {
	    		numberOfmessages.set(0);
	    		// if the benchmark is configured by the system to send change sets
	    		if(sentDataForm.equals("cs") || sentDataForm.equals("both")) {
	    			File changesetsPath = new File(datasetPath + "/changesets/c" + version);
	    			// send the files containing the triples to be added
	    			List<File> addedDataFiles = (List<File>) FileUtils.listFiles(changesetsPath, new String[] { "added.nt" }, false);
	    			for (File file : addedDataFiles) {
	    				String graphUri = "http://datagen.addset." + version + "." + file.getName();
	    				byte data[] = FileUtils.readFileToByteArray(file);
	    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
	    				sendDataToSystemAdapter(dataForSending);
	    				numberOfmessages.incrementAndGet();
	    			}
	    			
	    			// send the files containing the triples to be deleted	    			
	    			List<File> deletedDataFiles = (List<File>) FileUtils.listFiles(changesetsPath, new String[] { "deleted.nt" }, false);
	    			for (File file : deletedDataFiles) {
	    				String graphUri = "http://datagen.deleteset." + version + "." + file.getName();
	    				byte data[] = FileUtils.readFileToByteArray(file);
	    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
	    				sendDataToSystemAdapter(dataForSending);
	    				numberOfmessages.incrementAndGet();
	    			}
	    		} else if(sentDataForm.equals("ic") || sentDataForm.equals("both")) {
		    		// if the benchmark is configured by the system to send independent copy of each version.
	    			File independentcopiesPath = new File(datasetPath + "/independentcopies/v" + version);
	    			List<File> dataFiles = (List<File>) FileUtils.listFiles(independentcopiesPath, new String[] { "nt" }, false);
	    			for (File file : dataFiles) {
	    				String graphUri = "http://datagen.version." + version + "." + file.getName();
	    				byte data[] = FileUtils.readFileToByteArray(file);
	    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
	    				sendDataToSystemAdapter(dataForSending);
	    				numberOfmessages.incrementAndGet();
	    			}
	    		}
    	    	LOGGER.info("Data for version " + version + " successfully sent to System Adapter.");
			
            	LOGGER.info("Send signal to benchmark controller that all data (#" + numberOfmessages + ") of version " + version +" successfully sent to system adapter.");
            	ByteBuffer buffer = ByteBuffer.allocate(20);
            	buffer.putInt(triplesExpectedToBeAdded[version]);
            	buffer.putInt(triplesExpectedToBeDeleted[version]);
            	buffer.putInt(triplesExpectedToBeLoaded[version]);
            	buffer.putInt(getGeneratorId());
            	buffer.putInt(numberOfmessages.get());
    			sendToCmdQueue(VersioningConstants.DATA_GEN_VERSION_DATA_SENT, buffer.array());

    			LOGGER.info("Waiting until system receive and load the sent data.");
    			versionLoadedFromSystemMutex.acquire();
	    	}
		} catch (Exception e) {
            LOGGER.error("Exception while sending generated data to System Adapter.", e);
        } finally {
        	// After the bulk loading phase add a delay of 1 minute. 
        	// This is required in order to get reliable results regarding the final system's resource 
        	// usage, since in cAdvisor the period for disk stats collection is hard-coded at 1 minute
    		LOGGER.info("Waiting one minute after the bulk loading phase has ended...");
            Thread.sleep(1000 * 60);
        }
		
        try {
        	// send generated tasks along with their expected answers to task generator
        	for (Task task : tasks) {
        		byte[] data = SerializationUtils.serialize(task);       			
    			sendDataToTaskGenerator(data);
            	LOGGER.info("Task " + task.getTaskId() + " sent to Task Generator.");
        	}
        	LOGGER.info("All generated tasks successfully sent to Task Generator.");
        } catch (Exception e) {
            LOGGER.error("Exception while sending tasks to Task Generator.", e);
        }
	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == SystemAdapterConstants.BULK_LOADING_DATA_FINISHED) {
        	versionLoadedFromSystemMutex.release();
        }
        super.receiveCommand(command, data);
    }
	
	@Override
	public void close() throws IOException {
		LOGGER.info("Closing Data Generator...");
        super.close();
		LOGGER.info("Data Generator closed successfully.");
    }
}
