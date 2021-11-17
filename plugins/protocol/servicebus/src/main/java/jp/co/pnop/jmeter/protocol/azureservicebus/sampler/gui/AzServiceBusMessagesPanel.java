package jp.co.pnop.jmeter.protocol.azureservicebus.sampler.gui;

import org.apache.jorphan.gui.ObjectTableModel;
import org.apache.jorphan.reflect.Functor;

import jp.co.pnop.jmeter.protocol.amqp.sampler.AzAmqpMessage;
import jp.co.pnop.jmeter.protocol.amqp.sampler.gui.AzAmqpMessagesPanel;

public class AzServiceBusMessagesPanel extends AzAmqpMessagesPanel {

    public AzServiceBusMessagesPanel() {
        super("Messages");
    }

    /**
     * Initialize the table model used for the messages table.
     */
    @Override
    protected void initializeTableModel() {
        tableModel = new ObjectTableModel(
            new String[] { COLUMN_NAMES.get("MESSAGE_TYPE"), COLUMN_NAMES.get("MESSAGE"), COLUMN_NAMES.get("MESSAGE_ID"), "session Id", "partition key" },
            AzAmqpMessage.class,
            new Functor[] { new Functor("getMessageType"), new Functor("getMessage"), new Functor("getMessageId"), new Functor("getGroupId"), new Functor("getPartitionKey") },
            new Functor[] { new Functor("setMessageType"), new Functor("setMessage"), new Functor("setMessageId"), new Functor("setGroupId"), new Functor("setPartitionKey") },
            new Class[] { String.class, String.class, String.class, String.class, String.class }
        );
    }

    
}