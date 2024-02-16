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
package jp.co.pnop.jmeter.protocol.azureeventhubs.sampler.gui;

import java.awt.BorderLayout;

import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.BorderFactory;

import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.property.TestElementProperty;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import jp.co.pnop.jmeter.protocol.azureeventhubs.sampler.AzEventHubsPrepareSampler;

public class AzEventHubsPrepareSamplerGui extends AbstractSamplerGui implements ChangeListener {
    private static final long serialVersionUID = 1L;
    // private static final Logger log =
    // LoggerFactory.getLogger(AzEventHubsPrepareSamplerGui.class);
    private AzEventHubsMessagesPanel messagesPanel = new AzEventHubsMessagesPanel("Event data"); // $NON-NLS-1$

    public AzEventHubsPrepareSamplerGui() {
        init();
    }

    /**
     * A newly created component can be initialized with the contents of a Test
     * Element object by calling this method. The component is responsible for
     * querying the Test Element object for the relevant information to display
     * in its GUI.
     *
     * @param element
     *                the TestElement to configure
     */
    @Override
    public void configure(TestElement element) {
        super.configure(element);
        messagesPanel
                .configure((TestElement) element.getProperty(AzEventHubsPrepareSampler.UI_MESSAGES).getObjectValue());
    }

    @Override
    public TestElement createTestElement() {
        AzEventHubsPrepareSampler sampler = new AzEventHubsPrepareSampler();
        modifyTestElement(sampler);
        return sampler;
    }

    /**
     * Modifies a given TestElement to mirror the data in the gui components.
     *
     * @see org.apache.jmeter.gui.JMeterGUIComponent#modifyTestElement(TestElement)
     */
    @Override
    public void modifyTestElement(TestElement sampler) {
        sampler.clear();
        super.configureTestElement(sampler);
        sampler.setProperty(
                new TestElementProperty(AzEventHubsPrepareSampler.UI_MESSAGES, messagesPanel.createTestElement()));
    }

    /**
     * Implements JMeterGUIComponent.clearGui
     */
    @Override
    public void clearGui() {
        super.clearGui();
        messagesPanel.clear();
    }

    @Override
    public String getLabelResource() {
        return null; // $NON-NLS-1$
    }

    @Override
    public void stateChanged(ChangeEvent arg0) {
        // Do nothing
    }

    public String getStaticLabel() {
        return "Azure Event Hubs Prepare Sampler";
    }

    private JPanel createMessagesPanel() {
        JPanel panel = new JPanel(new BorderLayout(5, 0));
        panel.add(messagesPanel, BorderLayout.CENTER);
        return panel;
    }

    private void init() { // WARNING: called from ctor so must not be overridden (i.e. must be private or
                          // final)
        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());
        add(makeTitlePanel(), BorderLayout.NORTH);
        // MAIN PANEL
        VerticalPanel mainPanel = new VerticalPanel();
        VerticalPanel eventHubsConfigPanel = new VerticalPanel();
        eventHubsConfigPanel.setBorder(BorderFactory.createTitledBorder("Event Hubs Configuration"));
        mainPanel.add(eventHubsConfigPanel, BorderLayout.NORTH);
        mainPanel.add(createMessagesPanel(), BorderLayout.CENTER);

        add(mainPanel, BorderLayout.CENTER);
    }
}
