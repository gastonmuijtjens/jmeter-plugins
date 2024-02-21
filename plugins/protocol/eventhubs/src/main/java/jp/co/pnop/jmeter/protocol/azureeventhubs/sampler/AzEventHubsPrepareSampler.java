/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.pnop.jmeter.protocol.azureeventhubs.sampler;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.HashSet;

import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.property.PropertyIterator;
import org.apache.jmeter.testelement.property.TestElementProperty;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;
import org.apache.jmeter.testelement.TestStateListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jp.co.pnop.jmeter.protocol.amqp.sampler.AzAmqpMessage;
import jp.co.pnop.jmeter.protocol.amqp.sampler.AzAmqpMessages;

/**
 * Azure Event Hubs Sampler (non-Bean version)
 * <p>
 * JMeter creates an instance of a sampler class for every occurrence of the
 * element in every thread. [some additional copies may be created before the
 * test run starts]
 * <p>
 * Thus each sampler is guaranteed to be called by a single thread - there is no
 * need to synchronize access to instance variables.
 * <p>
 * However, access to class fields must be synchronized.
 *
 */
public class AzEventHubsPrepareSampler extends AbstractSampler implements TestStateListener {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(AzEventHubsPrepareSampler.class);

    private static final Set<String> APPLIABLE_CONFIG_CLASSES = new HashSet<>(
            Arrays.asList(
                    "org.apache.jmeter.config.gui.SimpleConfigGui"));

    public static final String MESSAGES = "azAmqpMessages";
    public static final String UI_MESSAGES = "messages";

    private static AtomicInteger classCount = new AtomicInteger(0); // keep track of classes created

    public AzEventHubsPrepareSampler() {
        super();
        classCount.incrementAndGet();
        trace("AzEventHubsPrepareSampler()");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SampleResult sample(Entry e) {
        trace("sample()");
        boolean isSuccessful = true;

        SampleResult res = new SampleResult();
        res.setSampleLabel(this.getName());

        String responseMessage = "";
        String requestBody = "";
        long bytes = 0;
        long sentBytes = 0;

        res.sampleStart();

        AzAmqpMessages messages = getMessages();
        if (messages == null) {
            messages = new AzAmqpMessages();
        }

        AzAmqpMessages uiMessages = getUiMessages();
        PropertyIterator iter = uiMessages.iterator();
        while (iter.hasNext()) {
            AzAmqpMessage msg = (AzAmqpMessage) iter.next().getObjectValue();
            log.info("Adding message '{}'", msg.getMessageId());
            messages.addMessage(msg);
        }

        setMessages(messages);

        res.setSamplerData(requestBody); // Request Body
        res.setBytes(bytes);
        res.setSentBytes(sentBytes);
        res.setResponseMessage(responseMessage);

        res.setSuccessful(isSuccessful);
        return res;
    }

    @Override
    public void testStarted() {
        testStarted(""); // $NON-NLS-1$
    }

    @Override
    public void testEnded() {
        testEnded(""); // $NON-NLS-1$
    }

    @Override
    public void testStarted(String host) {
        // ignored
    }

    // Ensure any remaining contexts are closed
    @Override
    public void testEnded(String host) {

    }

    /**
     * @see org.apache.jmeter.samplers.AbstractSampler#applies(org.apache.jmeter.config.ConfigTestElement)
     */
    @Override
    public boolean applies(ConfigTestElement configElement) {
        String guiClass = configElement.getProperty(TestElement.GUI_CLASS).getStringValue();
        return APPLIABLE_CONFIG_CLASSES.contains(guiClass);
    }

    /*
     * Helper method
     */
    private void trace(String s) {
        if (log.isDebugEnabled()) {
            log.debug("{} ({}) {} {} {}", Thread.currentThread().getName(), classCount.get(),
                    this.getName(), s, this.toString());
        }
    }

    public void setUiMessages(AzAmqpMessages messages) {
        setProperty(new TestElementProperty(UI_MESSAGES, messages));
    }

    public AzAmqpMessages getUiMessages() {
        return (AzAmqpMessages) getProperty(UI_MESSAGES).getObjectValue();
    }

    private void setMessages(AzAmqpMessages amqpMessages) {
        getThreadContext().getVariables().putObject(MESSAGES, amqpMessages);
    }

    private AzAmqpMessages getMessages() {
        AzAmqpMessages amqpMessages = (AzAmqpMessages) getThreadContext().getVariables().getObject(MESSAGES);
        return amqpMessages;
    }
}