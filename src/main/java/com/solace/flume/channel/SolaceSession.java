/* Copyright 2020 Solace Systems, Inc. All rights reserved.
 *
 * http://www.solace.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.solace.flume.channel;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class SolaceSession {
	private static final Logger LOG = LoggerFactory.getLogger(SolaceSession.class);

	  public final JCSMPProperties properties = new JCSMPProperties();
  
	  private JCSMPSession m_session = null;
	  private Queue m_queue = null;
	  private XMLMessageProducer m_producer = null;
	  private QueueRecieverThread m_receiverThread = null;
	  private SolaceChannel m_channel = null;
	  
	  /** Anonymous inner-class for handling publishing events */
      
	  private class PublishEventHandler implements JCSMPStreamingPublishEventHandler 
	  {
          public void responseReceived(String messageID) {
              LOG.debug("Producer received response for msg ID #%s%n",messageID);
          }
          public void handleError(String messageID, JCSMPException e, long timestamp) {
              System.out.printf("Producer received error for msg ID %s @ %s - %s%n",
                      messageID,timestamp,e);
          }
	  }
	  private class QueueRecieverThread implements Runnable
	  {
		public AtomicBoolean Stop = new AtomicBoolean(false);  
		public AtomicBoolean Running = new AtomicBoolean(true);  
		
		@Override
		public void run() 
		{
	        final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
	        flow_prop.setEndpoint(m_queue);
	        // set to "auto acknowledge" where the API will ack back to Solace at the
	        // end of the message received callback
	        flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
	        EndpointProperties endpoint_props = new EndpointProperties();
	        endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
	
	        FlowReceiver cons = null;
			try 
			{
				cons = m_session.createFlow(null, flow_prop, endpoint_props);
		        cons.start();
			} 
			catch (JCSMPException e) 
			{
				e.printStackTrace();
			}
	        LOG.info("Bound to Queue for consumption.");
	
	        while (Stop.get() == false)
	        {
	        	try 
	        	{
					BytesXMLMessage msg = cons.receive(1000);
					if (msg != null)
					{
						LOG.debug(">> A message arrived from the Solace queue");
						if ((msg instanceof BytesMessage) == false)
						{
							throw new JCSMPException("Expected a message of type BytesMessage, not a '" + msg.getClass().getName() + "'.");
						}
						BytesMessage flumeEventAsASolaceMessage = (BytesMessage) msg;
						m_channel.onMessageFromTheRouter(flumeEventAsASolaceMessage);
					}
					else
					{
						LOG.debug("Nothing arrived from the Solace queue");
					}
				} 
	        	catch (JCSMPException e) 
	        	{
					e.printStackTrace();
				} 
	        }
	        cons.close();
	        LOG.debug("receiver thread shutting down");
	        Running.set(false);
		}
	  }
      
	  public SolaceSession (SolaceChannel channel)
      {
		  m_channel = channel;
      }
	  
	  /**
	   * Stop the producer and consumer and end the session
	   */
	  public void stop()
	  {
		  LOG.debug("Signalling the receiver thread to stop.");
		  m_receiverThread.Stop.set(true);
		  
		  // and wait for it to stop
		  while (m_receiverThread.Running.get() == true )
		  {
			  try {
				Thread.sleep(50);
				LOG.debug("Waiting for receiver thread to shutdown.");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		  }
		  this.m_producer.close();
		  this.m_producer = null;
		  this.m_session.closeSession();
		  this.m_session = null;
		  LOG.debug("The Solace Producer and Session have been closed.");
	  }
	  
	  public boolean isRunning() {
		  return (this.m_session != null);
	  }
	  
      public void connect() throws JCSMPException
      {
          m_session = JCSMPFactory.onlyInstance().createSession(properties);
          m_session.connect();
      }
      
      /**
       * Create the producer and provisions the queue if required
       * 
       * @param strQueueName
       * @throws JCSMPException
       */
      public void createQueueProducer(String strQueueName) throws JCSMPException 
      {
          final EndpointProperties endpointProps = new EndpointProperties();
          // set queue permissions to "consume" and access-type to "exclusive"
          endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
          endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
          // create the queue object locally
          m_queue = JCSMPFactory.onlyInstance().createQueue(strQueueName);
          // Actually provision it, and do not fail if it already exists
          m_session.provision(m_queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);

          m_producer = m_session.getMessageProducer(new PublishEventHandler());
      }
      
      public void bindToQueueOnAnotherThread(String strQueueName) throws JCSMPException
      {
    	  if (m_queue == null) // temporal coupling.. improve this with lazy factory or other mechanism
    	  {
    		  throw new JCSMPException("Queue is not created, call createQueueProducer first.");
    	  }
    	  m_receiverThread = new QueueRecieverThread();
    	  
    	  Thread thread2 = new Thread(m_receiverThread);
    	  thread2.start(); 
      }
      
      public BytesMessage MessageFactory()
      {
    	  return JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
      }
      /**
       * Publishes a single message to the queue
       * @param flumeEventAsASolaceMessage
       * @throws JCSMPException
       */
      public void publishToQueue(BytesMessage flumeEventAsASolaceMessage) throws JCSMPException
      {
    	  if (m_producer == null) 
    	  {
    		  throw new JCSMPException("The queue is not bound to. Call bindToQueue() before publishToQueue()");
    	  }
    	  flumeEventAsASolaceMessage.setDeliveryMode(DeliveryMode.PERSISTENT);
    	  m_producer.send(flumeEventAsASolaceMessage, m_queue);
      }
}
