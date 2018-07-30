package org.hobbit.benchmark.versioning.components;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CharSequenceReader;
import org.apache.commons.lang3.SerializationUtils;
import org.hobbit.benchmark.versioning.Task;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.benchmark.versioning.util.SystemAdapterConstants;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Data Generator class for Versioning Benchmark - Large-scale.
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

	private String remoteDirectory;

	private int[] triplesExpectedToBeAdded;
	private int[] triplesExpectedToBeDeleted;
	private int[] triplesExpectedToBeLoaded;
	
	private Properties enabledQueryTypes = new Properties();
	private boolean allQueriesDisabled = true;

	private AtomicInteger numberOfmessages = new AtomicInteger(0);
	
	private Semaphore versionLoadedFromSystemMutex = new Semaphore(0);
	
	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing Data Generator '" + getGeneratorId() + "'");
		super.init();
		
		// Initialize data generation parameters through the environment variables given by user
		initFromEnv();
		
		remoteDirectory = "https://hobbitdata.informatik.uni-leipzig.de/SPVB-LS/" + v0TotalSizeInTriples + "T-"+ numberOfVersions + "V";
		
		// get the enabled queries 
		Pattern pattern = Pattern.compile("QT([1-8])=([0|1])[^\\w]*");
		Matcher matcher = pattern.matcher(enabledQueryTypesParam);
		String enabledQueryTypesParamProp = "";
		while (matcher.find()) {
			allQueriesDisabled = allQueriesDisabled ? !matcher.group(2).equals("1") : false;
			enabledQueryTypesParamProp += "QT" + matcher.group(1) + "=" + matcher.group(2) + "\n";
		}
		enabledQueryTypes.load(new StringReader(enabledQueryTypesParamProp));

		// initialize the appropriate tables with the triples that have to be added,
		// deleted and loaded for each version.
		getDatasetInfoFromFTP();		

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
		LOGGER.info("Getting dataset info...");
		triplesExpectedToBeAdded = new int[numberOfVersions];
		triplesExpectedToBeDeleted = new int[numberOfVersions];
		triplesExpectedToBeLoaded = new int[numberOfVersions];
		
		String remoteFile = remoteDirectory + "/data/version_info.csv";
		
		LOGGER.info("Dataset info:");
		try (InputStream inputStream = new URL (remoteFile).openStream()) {
			byte[] buffer = IOUtils.toByteArray(inputStream);
			try (
				Reader reader = new CharSequenceReader(new String(buffer));
				CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
						.withFirstRecordAsHeader()
	                    .withIgnoreHeaderCase()
	                    .withTrim());
			) {
				for (CSVRecord csvRecord : csvParser) {
					int version = Integer.parseInt(csvRecord.get("VersionNumber"));
					triplesExpectedToBeAdded[version] = Integer.parseInt(csvRecord.get("TriplesToBeAdded"));
					triplesExpectedToBeDeleted[version] = Integer.parseInt(csvRecord.get("TriplesToBeDeleted"));
					triplesExpectedToBeLoaded[version] = Integer.parseInt(csvRecord.get("TriplesToBeLoaded"));
					LOGGER.info("Version " + version + 
							"\tAdded: " + triplesExpectedToBeAdded[version] + 
							"\tDeleted:" + triplesExpectedToBeDeleted[version] +
							"\tTotal:" + triplesExpectedToBeLoaded[version]);

				}
			} catch (IOException e) {
				LOGGER.error("An error occured while reading \"version_info.csv\" file.", e);
			} 
		} catch (IOException e1) {
			LOGGER.error("An error occured while streaming \"version_info.csv\" file from FTP.", e1);
		} 
	}
	
	public void downloadAndSendVersion(int version, String versionRemoteDir) {
		LOGGER.info("Downloading and sending files of version " + version + "...");		
		// get the list of files that have to be downloaded
		String dataFiles = versionRemoteDir + "data_files.txt";
		String files[] = null;
		try (InputStream is = new URL(dataFiles).openStream()) {
			files = IOUtils.toString(is).split("\n");
		} catch (IOException e) {
			LOGGER.error("An error occured while reading files to be downloaded for version " + version, e);
		}
		// iterate through the list of files,  
		for (int i = 0; i < files.length; i++) {
			String graphUri = "http://datagen." + (files[i].contains("added") ? "addset" : "deleteset") + "." + version + "." + FilenameUtils.removeExtension(files[i]);
			String remoteFile = versionRemoteDir + files[i];
			LOGGER.info("Downloading file: " + files[i] + "...");
			try (GZIPInputStream inputStream = new GZIPInputStream (new URL(remoteFile).openStream())) {
				// de-compress file and send it to system adapter
				LOGGER.info("Decompressing file " + files[i] + "...");
				try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
					byte[] buffer = new byte[1024];
	                int len;
	                while((len = inputStream.read(buffer)) != -1) {
	                    out.write(buffer, 0, len);
	                }
	                byte[] data = out.toByteArray();
					byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][] { RabbitMQUtils.writeString(graphUri) }, data);
					sendDataToSystemAdapter(dataForSending);
				}
			} catch (IOException e) {
				LOGGER.error("An error occured while downloading file " + files[i], e);
			}
			numberOfmessages.incrementAndGet();
		}
	}
	
	public void downloadAndSendData(int version) {
		// in case that benchmark is configured to send change-sets 
		if(sentDataForm.equals("cs") || sentDataForm.equals("both")) {
			String dataRemoteDir = remoteDirectory + "/data/changesets/c" + version + "/";
			downloadAndSendVersion(version, dataRemoteDir);
			LOGGER.info("All files (changesets) of version " + version + " successfully sent to System Adapter.");
		} else if (sentDataForm.equals("ic") || sentDataForm.equals("both")) {
			String dataRemoteDir = remoteDirectory + "/data/independentcopies/v" + version + "/";
			downloadAndSendVersion(version, dataRemoteDir);
			LOGGER.info("All files (independent copies) of version " + version + " successfully sent to System Adapter.");
		}
	}
	
	private Task buildTask(int taskId, int queryType, int querySubType, int querySubstitutionParam, String queryFile) {
		Task task = new Task(Integer.toString(taskId));
		task.setQueryType(queryType);
		task.setQuerySubType(querySubType);
		task.setQuerySubstitutionParam(querySubstitutionParam);
		
		// download the query
		String queryFilePath = remoteDirectory + "/queries/" + queryFile;
		try (InputStream inputStream = new URL (queryFilePath).openStream()) {
			task.setQuery(IOUtils.toString(inputStream));
		} catch (IOException e) {
			LOGGER.error("An error occured while downloading file " + queryFile, e);
		}
		
		// download the expected results
		String resultsFile = FilenameUtils.removeExtension(queryFile) + "_results.json.gz";
		String resultsFilePath = remoteDirectory + "/results/" + resultsFile;
		try (GZIPInputStream inputStream = new GZIPInputStream (new URL(resultsFilePath).openStream())) {
			// de-compress file containing the expected results
			LOGGER.info("Decompressing file " + resultsFile + "...");
			try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
				byte[] buffer = new byte[1024];
                int len;
                while((len = inputStream.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                task.setExpectedAnswers(out.toByteArray());
			}
		} catch (IOException e) {
			LOGGER.error("An error occured while downloading file " + FilenameUtils.removeExtension(queryFile) + "_results.json.gz", e);
		}
		
		return task;
	}
	
	public void downloadAndSendQueries() {
		int taskId = 0;
		
		// get the list of all queries
		String queryFiles = remoteDirectory + "/queries/data_files.txt";
		String files[] = null;
		try (InputStream is = new URL(queryFiles).openStream()) {
			files = IOUtils.toString(is).split("\n");
		} catch (IOException e) {
			LOGGER.error("An error occured while reading query files to be downloaded", e);
		}
		
		// iterate through the list of files,  		
		for (int i = 0; i < files.length; i++) {
			Task task = null;
			int queryType = 0;
			int querySubType = 0; 
			int querySubstitutionParam = 0;
					
			// read the query type, subtype and substparam type
			Pattern pattern = Pattern.compile("versioningQuery(\\d)\\.(\\d)\\.(\\d)\\.sparql");
			Matcher matcher = pattern.matcher(files[i]);
			while (matcher.find()) {
				queryType = Integer.parseInt(matcher.group(1));
				querySubType = Integer.parseInt(matcher.group(2));
				querySubstitutionParam = Integer.parseInt(matcher.group(3));
			}
			
			try {
				switch (queryType) {
				case 2:
					if(enabledQueryTypes.getProperty("QT2", "0").equals("1")) {
						task = buildTask(taskId++, queryType, querySubType, querySubstitutionParam, files[i]);
						sendDataToTaskGenerator(SerializationUtils.serialize(task));
					}
					break;
				case 4:
					if(enabledQueryTypes.getProperty("QT4", "0").equals("1")) {
						task = buildTask(taskId++, queryType, querySubType, querySubstitutionParam, files[i]);
						sendDataToTaskGenerator(SerializationUtils.serialize(task));
					}
					break;
				case 6:
					if(enabledQueryTypes.getProperty("QT6", "0").equals("1")) {
						task = buildTask(taskId++, queryType, querySubType, querySubstitutionParam, files[i]);
						sendDataToTaskGenerator(SerializationUtils.serialize(task));
					}
					break;
				case 7:
					if(enabledQueryTypes.getProperty("QT7", "0").equals("1")) {
						task = buildTask(taskId++, queryType, querySubType, querySubstitutionParam, files[i]);
						sendDataToTaskGenerator(SerializationUtils.serialize(task));
					}
					break;
				case 8:
					if(enabledQueryTypes.getProperty("QT8", "0").equals("1")) {
						task = buildTask(taskId++, queryType, querySubType, querySubstitutionParam, files[i]);
						sendDataToTaskGenerator(SerializationUtils.serialize(task));
					}
					break;
				}
			} catch (Exception e) {
	            LOGGER.error("Exception while sending tasks to Task Generator.", e);
	        }
        	LOGGER.info("Task " + task.getTaskId() + " sent to Task Generator.");
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
	// This method is used for sending data, tasks and expected answers to the appropriate benchmark components.
	protected void generateData() throws Exception {
		try {						
			// Send data files to the system.
			// Starting for version 0 and continue to the next one until reaching the last version.
			// Waits for signal sent by the system that determines that the sent version successfully 
			// loaded, in order to proceed with the next one
	    	for(int version = 0; version < numberOfVersions; version++) {
	    		numberOfmessages.set(0);
	    		downloadAndSendData(version);
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
		
		// if all queries are disabled skip this part
		if(!allQueriesDisabled) {
			downloadAndSendQueries();
			LOGGER.info("All generated tasks successfully sent to Task Generator.");
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
