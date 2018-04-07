package dbtest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.SQSActions;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.model.DeleteArchiveResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.DeleteTopicRequest;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sns.model.UnsubscribeRequest;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.fabric.xmlrpc.base.Array;


public class GlacierRetriever {

    public static String vaultName = "test";
    public static String snsTopicName = "glacierFinished";
    public static String sqsQueueName = "glacierFinishedQueue";
    public static String sqsQueueARN;
    public static String sqsQueueURL;
    public static String snsTopicARN;
    public static String snsSubscriptionARN;
    public static String fileName = "/media/wojtek/fromaws/fromaws";
    public static String region = "eu-west-1";
    public static long sleepTime = 600; 
    public static AmazonGlacierClient client;
    public static AmazonSQSClient sqsClient;
    public static AmazonSNSClient snsClient;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(GlacierRetriever.class);
    
    public static void main(String[] args) throws IOException {
    	
    	EnvironmentVariableCredentialsProvider credentials = new EnvironmentVariableCredentialsProvider();
    	
        client = new AmazonGlacierClient(credentials);
        client.setEndpoint("https://glacier." + region + ".amazonaws.com");
        sqsClient = new AmazonSQSClient(credentials);
        sqsClient.setEndpoint("https://sqs." + region + ".amazonaws.com");
        snsClient = new AmazonSNSClient(credentials);
        snsClient.setEndpoint("https://sns." + region + ".amazonaws.com");

        try {
//            setupSQS();
//            setupSNS();

//            GJeS6ox_EzyxRWqB5uCjGr8X9c0h0D75m6-ex6uybKp5eMEnt2orjy9Ifjv42CG0iSE5783TF1L1M3hoinQsoOxCq3KW
//            dmF4SKHdMnc0ggSgvrbuHRXBho_v59Ih8hHNCOZFFecC8xj4v7QdRh6RNHJ22aI0xLL3xWeZqK1D29FFmttLygpDVJSX
        	
//            antejxA3sxdExhMTshmOnuEse2UFlVrRnI92cxRwSqAdMC67m_gj_EtJyzvjuZ5vestu6toVSxkmikVekUh6_WxiCBoh
            
//            String jobId = initiateJobRequest();
            String jobId = "dmF4SKHdMnc0ggSgvrbuHRXBho_v59Ih8hHNCOZFFecC8xj4v7QdRh6RNHJ22aI0xLL3xWeZqK1D29FFmttLygpDVJSX";
//            LOGGER,info("Jobid = " + jobId);
            
//            Boolean success = waitForJobToComplete(jobId, sqsQueueURL);
//            if (!success) { throw new Exception("Job did not complete successfully."); }
            
            downloadJobOutput(jobId);
            
            cleanUp();
            
        } catch (Exception e) {
            System.err.println("Inventory retrieval failed.");
            System.err.println(e);
        }   
    }

    private static void setupSQS() {
        CreateQueueRequest request = new CreateQueueRequest()
            .withQueueName(sqsQueueName);
        CreateQueueResult result = sqsClient.createQueue(request);  
        sqsQueueURL = result.getQueueUrl();
                
        GetQueueAttributesRequest qRequest = new GetQueueAttributesRequest()
            .withQueueUrl(sqsQueueURL)
            .withAttributeNames("QueueArn");
        
        GetQueueAttributesResult qResult = sqsClient.getQueueAttributes(qRequest);
        sqsQueueARN = qResult.getAttributes().get("QueueArn");
        
        Policy sqsPolicy = 
            new Policy().withStatements(
                    new Statement(Effect.Allow)
                    .withPrincipals(Principal.AllUsers)
                    .withActions(SQSActions.SendMessage)
                    .withResources(new Resource(sqsQueueARN)));
        Map<String, String> queueAttributes = new HashMap<String, String>();
        queueAttributes.put("Policy", sqsPolicy.toJson());
        sqsClient.setQueueAttributes(new SetQueueAttributesRequest(sqsQueueURL, queueAttributes)); 

    }
    private static void setupSNS() {
        CreateTopicRequest request = new CreateTopicRequest()
            .withName(snsTopicName);
        CreateTopicResult result = snsClient.createTopic(request);
        snsTopicARN = result.getTopicArn();

        SubscribeRequest request2 = new SubscribeRequest()
            .withTopicArn(snsTopicARN)
            .withEndpoint(sqsQueueARN)
            .withProtocol("sqs");
        SubscribeResult result2 = snsClient.subscribe(request2);
                
        snsSubscriptionARN = result2.getSubscriptionArn();
    }
    private static String initiateJobRequest() {
        
        JobParameters jobParameters = new JobParameters()
            .withType("inventory-retrieval")
            .withSNSTopic(snsTopicARN);
        
        InitiateJobRequest request = new InitiateJobRequest()
            .withVaultName(vaultName)
            .withJobParameters(jobParameters);
        
        InitiateJobResult response = client.initiateJob(request);
        
        return response.getJobId();
    }
    
    private static Boolean waitForJobToComplete(String jobId, String sqsQueueUrl) throws InterruptedException, JsonParseException, IOException {
        
        Boolean messageFound = false;
        Boolean jobSuccessful = false;
        ObjectMapper mapper = new ObjectMapper();
        JsonFactory factory = mapper.getFactory();
        
        while (!messageFound) {
        	LOGGER.info("checking queue. URL: " + sqsQueueUrl);
            List<Message> msgs = sqsClient.receiveMessage(
               new ReceiveMessageRequest(sqsQueueUrl).withMaxNumberOfMessages(10)).getMessages();
            
            LOGGER.info("Number of messages: " + msgs.size());

            if (msgs.size() > 0) {
                for (Message m : msgs) {
                    JsonParser jpMessage = factory.createJsonParser(m.getBody());
                    JsonNode jobMessageNode = mapper.readTree(jpMessage);
                    String jobMessage = jobMessageNode.get("Message").textValue();
                    
                    JsonParser jpDesc = factory.createJsonParser(jobMessage);
                    JsonNode jobDescNode = mapper.readTree(jpDesc);
                    String retrievedJobId = jobDescNode.get("JobId").textValue();
                    System.err.println(("Job ID: " + retrievedJobId));
                    String statusCode = jobDescNode.get("StatusCode").textValue();
                    if (retrievedJobId.equals(jobId)) {
                        messageFound = true;
                        if (statusCode.equals("Succeeded")) {
                            jobSuccessful = true;
                        }
                    }
                }
                
            } else {
            	LOGGER.info("sleeping for: " + sleepTime * 1000);
              Thread.sleep(sleepTime * 1000); 
            }
          }
        return (messageFound && jobSuccessful);
    }
    
    private static void downloadJobOutput(String jobId) throws IOException {
        
//        GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
//            .withVaultName(vaultName)
//            .withJobId(jobId);
//        GetJobOutputResult getJobOutputResult = client.getJobOutput(getJobOutputRequest);
    
//        FileWriter fstream = new FileWriter(fileName);
//        BufferedWriter out = new BufferedWriter(fstream);
//        BufferedReader in = new BufferedReader(new InputStreamReader(getJobOutputResult.getBody()));            
//        String inputLine;
//        try {
//            while ((inputLine = in.readLine()) != null) {
//                out.write(inputLine);
//            }
//        }catch(IOException e) {
//            throw new AmazonClientException("Unable to save archive", e);
//        }finally{
//            try {in.close();}  catch (Exception e) {}
//            try {out.close();}  catch (Exception e) {}             
//        }
    	
    	List<String> archNames = Arrays.asList(
    			"dSZ9J-7Ds7bs0GTVMzBWWxBVSFkxWNwR6yi6lLVkNpXm5-EQY5kn0PoWhAga85SokEKe36kz0qEW3JCNLCrMl5C_wkS2raGsCQGLvKM26CbKfUhVPhgqNqgRBZ3q_OoxKjcEMQ2IPQ",
    			"BBeh_3KwgtUEEgIQTYnpLJpt2aFBq0Mun4q7tH3dnMmRLhMmi_FE1H2xeLsm-MgOJHA17UWRcUupmFDEI7UwZPXJuDkunuwBMRpNgmlK1MvaN9Crwz1h-xQ5lHYzvJVjvvJrWFeRYg", 
    			"I4NN2tHRFlW4BGgehbUSXztY_cay5Lz1kIwEJkI2gUtYwdq6R9RxSmbB5_ZYi65DaBPMefC9-svpdSI0oQDSlOwxmMzlkIH720dlQlhfMgi-bYVAzAJS7TZ0tJS6lLb6HPyW7V9W7g",
    			"15Tnc1m1b0-12K0Rtb42f7aPPI8Oknw7oJlsD0EW9gFmzAQr9Ng1Q8XfXY_JJ_XnW1_dTJBznej8e9uJJ66uUyk4By_mBwJvEQBrCrIC05P_JrsDfJralWdFviPKrtm3gwCstJQPzA ");
    	
    	for (String archName : archNames) {
    		
        	DeleteArchiveRequest request = new DeleteArchiveRequest()
        	    .withVaultName(vaultName)
        	    .withArchiveId(archName);
        	DeleteArchiveResult deleteArchive = client.deleteArchive(request);
        	LOGGER.info("wojtek: " + deleteArchive.toString());
		}
    	
    	System.exit(0);
    	
        try {
            ArchiveTransferManager atm = new ArchiveTransferManager(client, sqsClient, snsClient);
            LOGGER.info("get");
            
            File file = new File(fileName);
            LOGGER.info("wojtek: "+file.getAbsolutePath());
            
            //dSZ9J-7Ds7bs0GTVMzBWWxBVSFkxWNwR6yi6lLVkNpXm5-EQY5kn0PoWhAga85SokEKe36kz0qEW3JCNLCrMl5C_wkS2raGsCQGLvKM26CbKfUhVPhgqNqgRBZ3q_OoxKjcEMQ2IPQ   _ - down loaded 660K (think that was a test file I initially uploaded)
            //BBeh_3KwgtUEEgIQTYnpLJpt2aFBq0Mun4q7tH3dnMmRLhMmi_FE1H2xeLsm-MgOJHA17UWRcUupmFDEI7UwZPXJuDkunuwBMRpNgmlK1MvaN9Crwz1h-xQ5lHYzvJVjvvJrWFeRYg     - downloaded 38.1 GB
            //I4NN2tHRFlW4BGgehbUSXztY_cay5Lz1kIwEJkI2gUtYwdq6R9RxSmbB5_ZYi65DaBPMefC9-svpdSI0oQDSlOwxmMzlkIH720dlQlhfMgi-bYVAzAJS7TZ0tJS6lLb6HPyW7V9W7g	 -
            //15Tnc1m1b0-12K0Rtb42f7aPPI8Oknw7oJlsD0EW9gFmzAQr9Ng1Q8XfXY_JJ_XnW1_dTJBznej8e9uJJ66uUyk4By_mBwJvEQBrCrIC05P_JrsDfJralWdFviPKrtm3gwCstJQPzA     -
            
            atm.download(vaultName, "I4NN2tHRFlW4BGgehbUSXztY_cay5Lz1kIwEJkI2gUtYwdq6R9RxSmbB5_ZYi65DaBPMefC9-svpdSI0oQDSlOwxmMzlkIH720dlQlhfMgi-bYVAzAJS7TZ0tJS6lLb6HPyW7V9W7g", file);
            LOGGER.info("finished");
            
        } catch (Exception e)
        { 
            LOGGER.info(e.toString()); 
        }
        
        LOGGER.info("Retrieved inventory to " + fileName);
    }
    
    private static void cleanUp() {
        snsClient.unsubscribe(new UnsubscribeRequest(snsSubscriptionARN));
        snsClient.deleteTopic(new DeleteTopicRequest(snsTopicARN));
        sqsClient.deleteQueue(new DeleteQueueRequest(sqsQueueURL));
    }
}
