package org.jms.pubsub;
import java.net.URI;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.poc.jms.listener.ConsumerMessageListener;

public class ProducerConsumer {
	
	public static void main(String[] args) throws Exception{
		
		BrokerService broker = BrokerFactory.createBroker(new URI("broker:(tcp://localhost:61616)"));
		broker.start();
		
		Connection con = null;
		
		try {
			//Producer
			ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
			con = connectionFactory.createConnection();
			
			Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Topic topic = session.createTopic("PushNotificationTopic");
			
			
			//consumer 1 subscribes to PushNotificationTopic 
			MessageConsumer consumer1 = session.createConsumer(topic);
			consumer1.setMessageListener(new ConsumerMessageListener("consumer 1"));
			
			//consumer2 subscribers to PushNotificationTopic
			MessageConsumer consumer2 = session.createConsumer(topic);
			consumer2.setMessageListener(new ConsumerMessageListener("consumer 2"));
			
			con.start();
			
			//publish
			String payload="This is a notification";
			Message msg = session.createTextMessage(payload);
			MessageProducer producer = session.createProducer(topic);
			System.out.println("Sending text : '"+ payload +"'");
			producer.send(msg);
			
			Thread.sleep(3000);
			session.close();
		}finally {
			if(con != null) {
				con.close();
			}
			broker.stop();
		}
	}

}
