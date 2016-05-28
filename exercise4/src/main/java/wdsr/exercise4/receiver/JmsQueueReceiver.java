package wdsr.exercise4.receiver;

import java.util.Enumeration;

import javax.annotation.Resource;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.xml.bind.Marshaller.Listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.sender.JmsSender;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
	 @Resource(mappedName = "jms/JMSConnectionFactory")
	    private ConnectionFactory connectionFactory;
	 
	    @Resource(mappedName = "jms/myQueue")
	    Queue myQueue;
	 
	    private String message;
	
	private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
	private final String url = "localhost:62616";
	/**
	 * Creates this object
	 * @param queueName Name of the queue to consume messages from.
	 */
	public JmsQueueReceiver(final String queueName){
		// ConnectionFactory connectionFactory = null;
		 
		/* try(JMSContext context = connectionFactory.createContext()) {
			 JMSConsumer consumer = context.createConsumer(queue);
			 Message m = consumer.receive();
		 }*/
		 
		  try {
	            Connection connection = connectionFactory.createConnection();
	            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	            QueueBrowser queueBrowser = session.createBrowser(myQueue);
	            Enumeration enumeration = queueBrowser.getEnumeration();
	            while (enumeration.hasMoreElements()) {
	                TextMessage o = (TextMessage) enumeration.nextElement();
	               //return "Receive " + o.getText();
	            }
	            session.close();
	            connection.close();
	        } catch (JMSException e) {
	            e.printStackTrace();
	        }
	        //return "";
	    }

	/**
	 * Registers the provided callback. The callback will be invoked when a price or volume alert is consumed from the queue.
	 * @param alertService Callback to be registered.
	 */
	public void registerCallback(AlertService alertService) {
	}
	
	/**
	 * Deregisters all consumers and closes the connection to JMS broker.
	 */
	public void shutdown() {
		// TODO
	}
}
