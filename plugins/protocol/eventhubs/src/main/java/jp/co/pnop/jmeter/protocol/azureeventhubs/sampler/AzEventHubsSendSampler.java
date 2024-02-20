package jp.co.pnop.jmeter.protocol.azureeventhubs.sampler;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.testelement.property.PropertyIterator;
import org.apache.jmeter.testelement.property.StringProperty;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.core.amqp.exception.AmqpException;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import jp.co.pnop.jmeter.protocol.aad.config.AzAdCredential;
import jp.co.pnop.jmeter.protocol.aad.config.AzAdCredential.AzAdCredentialComponentImpl;
import jp.co.pnop.jmeter.protocol.amqp.sampler.AzAmqpMessage;
import jp.co.pnop.jmeter.protocol.amqp.sampler.AzAmqpMessages;

public class AzEventHubsSendSampler extends AbstractSampler implements TestStateListener {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(AzEventHubsSendSampler.class);

    private static final Map<String, ChronoUnit> chronoUnit = new HashMap<String, ChronoUnit>();
    static {
        chronoUnit.put("MILLIS", ChronoUnit.MILLIS);
        chronoUnit.put("SECONDS", ChronoUnit.SECONDS);
        chronoUnit.put("MINUTES", ChronoUnit.MINUTES);
        chronoUnit.put("HOURS", ChronoUnit.HOURS);
        chronoUnit.put("DAYS", ChronoUnit.DAYS);
    };

    private static final Set<String> APPLIABLE_CONFIG_CLASSES = new HashSet<>(
            Arrays.asList(
                    "org.apache.jmeter.config.gui.SimpleConfigGui"));

    public static final String NAMESPACE_NAME = "namespaceName";
    public static final String AUTH_TYPE = "authType";
    public static final String SHARED_ACCESS_KEY_NAME = "sharedAccessKeyName";
    public static final String SHARED_ACCESS_KEY = "sharedAccessKey";
    public static final String AAD_CREDENTIAL = "aadCredential";
    public static final String EVENT_HUB_NAME = "eventHubName";
    public static final String PARTITION_TYPE = "partitionType";
    public static final String PARTITION_VALUE = "partitionValue";
    public static final String MESSAGES = "azAmqpMessages";

    public static final String AUTHTYPE_SAS = "Shared access signature";
    public static final String AUTHTYPE_AAD = "Azure AD credential";

    public static final String PARTITION_TYPE_NOT_SPECIFIED = "Not specified";
    public static final String PARTITION_TYPE_ID = "ID";
    public static final String PARTITION_TYPE_KEY = "Key";

    private JMeterVariables vars = JMeterContextService.getContext().getVariables();

    @Override
    public SampleResult sample(Entry e) {
        boolean isSuccessful = false;

        SampleResult sampleResult = new SampleResult();
        sampleResult.setSampleLabel(this.getName());

        String threadName = Thread.currentThread().getName();
        String responseMessage = "";
        String requestBody = "";
        long bytes = 0;
        long sentBytes = 0;

        EventHubProducerClient eventHubProducerClient = null;
        EventHubClientBuilder eventHubProducerClientBuilder = new EventHubClientBuilder();

        // Verify if any messages are configured
        AzAmqpMessages amqpMessages = getMessages();

        try {
            if (amqpMessages == null) {
                throw new Exception(
                        "No AMQP messages have been found. Make sure to use the Event Hubs Message Add sampler first.");
            }

            sampleResult.sampleStart(); // Start timing

            requestBody = "Endpoint: sb://".concat(getNamespaceName()).concat("\n")
                    .concat("Event Hub: ").concat(getEventHubName());

            if (getAuthType().equals(AUTHTYPE_SAS)) {
                final String connectionString = "Endpoint=sb://".concat(getNamespaceName()).concat("/;")
                        .concat("SharedAccessKeyName=").concat(getSharedAccessKeyName()).concat(";")
                        .concat("SharedAccessKey=").concat(getSharedAccessKey());
                requestBody = requestBody.concat("\n")
                        .concat("Shared Access Policy: ").concat(getSharedAccessKeyName()).concat("\n")
                        .concat("Shared Access Key: **********");
                eventHubProducerClientBuilder = eventHubProducerClientBuilder.connectionString(connectionString,
                        getEventHubName());
            } else { // AUTHTYPE_AAD
                AzAdCredentialComponentImpl credential = AzAdCredential.getCredential(getAadCredential());
                requestBody = requestBody.concat(credential.getRequestBody());
                eventHubProducerClientBuilder = eventHubProducerClientBuilder.credential(getNamespaceName(),
                        getEventHubName(), credential.getCredential());
            }
            eventHubProducerClient = eventHubProducerClientBuilder.buildProducerClient();

            // prepare a batch of events to send to the event hub
            CreateBatchOptions batchOptions = new CreateBatchOptions();
            if (getPartitionValue().length() > 0) {
                switch (getPartitionType()) {
                    case PARTITION_TYPE_ID:
                        batchOptions.setPartitionId(getPartitionValue());
                        requestBody = requestBody.concat("\n").concat("Partition ID: ").concat(getPartitionValue());
                        break;
                    case PARTITION_TYPE_KEY:
                        batchOptions.setPartitionKey(getPartitionValue());
                        requestBody = requestBody.concat("\n").concat("Partition Key: ").concat(getPartitionValue());
                        break;
                }
            }
            EventDataBatch batch = eventHubProducerClient.createBatch(batchOptions);

            AzAmqpMessages clonedAmqpMessages = (AzAmqpMessages) amqpMessages.clone();
            PropertyIterator iter = clonedAmqpMessages.iterator();
            int msgCount = 0;
            while (iter.hasNext()) {
                msgCount++;
                AzAmqpMessage msg = (AzAmqpMessage) iter.next().getObjectValue();

                requestBody = requestBody.concat("\n\n")
                        .concat("[Event data #").concat(String.valueOf(msgCount)).concat("]\n")
                        .concat("Message type: ").concat(msg.getMessageType()).concat("\n")
                        .concat("Body: ").concat(msg.getMessage());
                EventData eventData;
                switch (msg.getMessageType()) {
                    case AzAmqpMessages.MESSAGE_TYPE_BASE64:
                        byte[] binMsg = Base64.getDecoder().decode(msg.getMessage().getBytes());
                        eventData = new EventData(binMsg);
                        break;
                    case AzAmqpMessages.MESSAGE_TYPE_FILE:
                        BufferedInputStream bi = null;
                        bi = new BufferedInputStream(new FileInputStream(msg.getMessage()));
                        eventData = new EventData(IOUtils.toByteArray(bi));
                        break;
                    default: // AzAmqpMessages.MESSAGE_TYPE_STRING
                        eventData = new EventData(msg.getMessage());
                }

                String messageId = msg.getMessageId();
                if (!messageId.isEmpty()) {
                    eventData.setMessageId(messageId);
                    requestBody = requestBody.concat("\n").concat("Message ID: ").concat(messageId);
                }

                String customProperties = msg.getCustomProperties();
                if (!customProperties.isEmpty()) {
                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> properties = mapper.readValue(customProperties,
                            new TypeReference<Map<String, Object>>() {
                            });
                    eventData.getProperties().putAll(properties);
                    requestBody = requestBody.concat("\n").concat("Custom properties: ").concat(customProperties);
                }

                String contentType = msg.getContentType();
                if (!contentType.isEmpty()) {
                    eventData.setContentType(contentType);
                    requestBody = requestBody.concat("\n").concat("Content Type: ").concat(contentType);
                }

                batch.tryAdd(eventData);
            }

            bytes = batch.getSizeInBytes();

            // send the batch of events to the event hub
            eventHubProducerClient.send(batch);

            sentBytes = batch.getSizeInBytes();
            sampleResult.latencyEnd();

            sampleResult.setDataType(SampleResult.TEXT);

            responseMessage = "OK";
            isSuccessful = true;
            sampleResult.sampleEnd(); // End timing
        } catch (AmqpException ex) {
            logger.error("Error calling {} sampler. ", threadName, ex);
            if (ex.isTransient()) {
                responseMessage = "A transient error occurred in ".concat(threadName)
                        .concat(" sampler. Please try again later.\n");
            }
            responseMessage = responseMessage.concat(ex.getMessage());
            sampleResult.setResponseData(ex.getMessage(), "UTF-8");
        } catch (FileNotFoundException ex) {
            sampleResult.setResponseData(ex.toString(), "UTF-8");
            responseMessage = ex.getMessage();
            logger.error("Error calling {} sampler. ", threadName, ex);
        } catch (Exception ex) {
            sampleResult.setResponseData(ex.toString(), "UTF-8");
            responseMessage = ex.getMessage();
            logger.error("Error calling {} sampler. ", threadName, ex);
        } finally {
            amqpMessages.removeAllMessages();
            setMessages(amqpMessages);

            sampleResult.setSamplerData(requestBody); // Request Body
            sampleResult.setBytes(bytes);
            sampleResult.setSentBytes(sentBytes);
            sampleResult.setResponseMessage(responseMessage);
        }

        sampleResult.setSuccessful(isSuccessful);
        return sampleResult;
    }

    @Override
    public void testStarted() {
        // Not implemented for now
    }

    @Override
    public void testStarted(String host) {
        // Not implemented for now
    }

    @Override
    public void testEnded() {
        // Not implemented for now
    }

    @Override
    public void testEnded(String host) {
        // Not implemented for now
    }

    /**
     * @see org.apache.jmeter.samplers.AbstractSampler#applies(org.apache.jmeter.config.ConfigTestElement)
     */
    @Override
    public boolean applies(ConfigTestElement configElement) {
        String guiClass = configElement.getProperty(TestElement.GUI_CLASS).getStringValue();
        return APPLIABLE_CONFIG_CLASSES.contains(guiClass);
    }

    public void setNamespaceName(String namespaceName) {
        setProperty(new StringProperty(NAMESPACE_NAME, namespaceName));
    }

    public String getNamespaceName() {
        return getPropertyAsString(NAMESPACE_NAME);
    }

    public void setAuthType(String authType) {
        setProperty(new StringProperty(AUTH_TYPE, authType));
    }

    public String getAuthType() {
        return getPropertyAsString(AUTH_TYPE);
    }

    public void setSharedAccessKeyName(String sharedAccessKeyName) {
        setProperty(new StringProperty(SHARED_ACCESS_KEY_NAME, sharedAccessKeyName));
    }

    public String getSharedAccessKeyName() {
        return getPropertyAsString(SHARED_ACCESS_KEY_NAME);
    }

    public void setSharedAccessKey(String sharedAccessKey) {
        setProperty(new StringProperty(SHARED_ACCESS_KEY, sharedAccessKey));
    }

    public String getSharedAccessKey() {
        return getPropertyAsString(SHARED_ACCESS_KEY);
    }

    public void setAadCredential(String aadCredential) {
        setProperty(new StringProperty(AAD_CREDENTIAL, aadCredential));
    }

    public String getAadCredential() {
        return getPropertyAsString(AAD_CREDENTIAL);
    }

    public void setEventHubName(String eventHubName) {
        setProperty(new StringProperty(EVENT_HUB_NAME, eventHubName));
    }

    public String getEventHubName() {
        return getPropertyAsString(EVENT_HUB_NAME);
    }

    public void setPartitionType(String partitionType) {
        setProperty(new StringProperty(PARTITION_TYPE, partitionType));
    }

    public String getPartitionType() {
        return getPropertyAsString(PARTITION_TYPE);
    }

    public void setPartitionValue(String partitionValue) {
        setProperty(new StringProperty(PARTITION_VALUE, partitionValue));
    }

    public String getPartitionValue() {
        return getPropertyAsString(PARTITION_VALUE).trim();
    }

    private void setMessages(AzAmqpMessages amqpMessages) {
        vars.putObject(MESSAGES, amqpMessages);
    }

    private AzAmqpMessages getMessages() {
        AzAmqpMessages amqpMessages = (AzAmqpMessages) vars.getObject(MESSAGES);
        return amqpMessages;
    }
}
