package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.ObjectMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender{
	private static final Logger log = LoggerFactory.getLogger(JmsSender.class);
	
	private final String queueName;
	private final String topicName;
	private final String url = "tcp://localhost:62616";
	private ConnectionFactory connectionFactory;

	public JmsSender(final String queueName, final String topicName) {
		this.queueName = queueName;
		this.topicName = topicName;
		this.connectionFactory = new ActiveMQConnectionFactory(url);
	}

	/**
	 * This method creates an Order message with the given parameters and sends it as an ObjectMessage to the queue.
	 * @param orderId ID of the product
	 * @param product Name of the product
	 * @param price Price of the product
	 */
	public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) throws JMSException{
		try {
			Connection con = connectionFactory.createConnection();
			con.start();
			Session session= con.createSession(false,Session.AUTO_ACKNOWLEDGE);
		
			Destination dest = session.createQueue(queueName);
			MessageProducer messageProducer = session.createProducer(dest);
			Order orderObj = new Order(orderId, product, price);
			ObjectMessage ob = session.createObjectMessage(orderObj);
            ob.setJMSType("Order");
            ob.setStringProperty("WDSR-System", "OrderProcessor");
			messageProducer.send(ob);
			
			session.close();
			con.close();
			messageProducer.close();
		} catch (JMSException e) {
			log.error(e.getErrorCode()+"- "+e.getMessage(),e);
		}
	}

	/**
	 * This method sends the given String to the queue as a TextMessage.
	 * @param text String to be sent
	 */
	public void sendTextToQueue(String text) {
		try {
			Connection con = connectionFactory.createConnection();
			con.start();
			Session session= con.createSession(false,Session.AUTO_ACKNOWLEDGE);
		
			Destination dest = session.createQueue(queueName);
			MessageProducer messageProducer = session.createProducer(dest);
			TextMessage textMessage = session.createTextMessage(text);
			messageProducer.send(textMessage);
			
			session.close();
			con.close();
			messageProducer.close();
		} catch (JMSException e) {
			log.error(e.getErrorCode()+"- "+e.getMessage(),e);
		}
	}

	/**
	 * Sends key-value pairs from the given map to the topic as a MapMessage.
	 * @param map Map of key-value pairs to be sent.
	 */
	public void sendMapToTopic(Map<String, String> map) {
		try {
			Connection con = connectionFactory.createConnection();
			con.start();
			Session session= con.createSession(false,Session.AUTO_ACKNOWLEDGE);
			
			Destination dest = session.createTopic(topicName);
			MessageProducer messageProducer = session.createProducer(dest);
			MapMessage mapMessage = session.createMapMessage();
			
			for(Map.Entry<String,String> entry: map.entrySet()){
				mapMessage.setString(entry.getKey(), entry.getValue());
			}	
			
			messageProducer.send(mapMessage);
			session.close();
			con.close();
			messageProducer.close();
			
		} catch (JMSException e) {
			log.error(e.getErrorCode()+"- "+e.getMessage(),e);
		}
	}
}
