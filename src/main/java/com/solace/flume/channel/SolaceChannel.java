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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.channel.AbstractChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.SDTException;

@InterfaceAudience.Private
@InterfaceStability.Stable 
@Disposable
public class SolaceChannel extends AbstractChannel {
	private static final Logger LOG = LoggerFactory.getLogger(SolaceChannel.class);
	
	private int m_nTransactionCapacity = 1000; // default
	
	private static final String CONFIG_KEY_OVERALL_CAPACITY = "transactionCapacity";

	private static final String CONFIG_KEY_SOLACE_HOSTROUTER = "hostRouter";
	private static final String CONFIG_KEY_SOLACE_VPN = "VPN";
	private static final String CONFIG_KEY_SOLACE_CLIENTUSERNAME = "clientUserName";
	private static final String CONFIG_KEY_SOLACE_PW = "Password";
	private static final String CONFIG_KEY_SOLACE_QUEUENAME = "QueueName";
	
	private LinkedBlockingQueue<BytesMessage> m_queueOfSolaceMessages = new LinkedBlockingQueue<BytesMessage>(); 
	private SolaceChannelTransaction m_currentTransactionInProgress = null;
	private SolaceSession m_session = null; 
	private String m_strQueueName = "";

	public class SolaceChannelEvent
	{
		private Event flumeEvent;
		SolaceChannelEvent(Event event) {
			flumeEvent = event;
		}
		Event getEvent() { return flumeEvent;}
	}
	
	public class MessagePairEvent extends SolaceChannelEvent
	{
		private BytesMessage solaceMsg = null;
		MessagePairEvent(Event event, BytesMessage solaceMsg) {
			super(event);
			this.solaceMsg = solaceMsg;
		}
		BytesMessage getSolaceMessage() { return solaceMsg;}
	}
	
	public SolaceChannel()
	{
		m_session = new SolaceSession(this);	
	}
	
	
	@Override
	public Transaction getTransaction() 
	{
		m_currentTransactionInProgress = new SolaceChannelTransaction(this);
		return m_currentTransactionInProgress;
	}

	/**
	 * Accepts an event from the source.
	 */
	@Override
	public void put(Event flumeEvent) throws ChannelException 
	{
		LOG.debug("Event has been put, is null= " + (flumeEvent == null));
		
		// we must have an active transaction 
		if (m_currentTransactionInProgress == null)
		{
			throw new ChannelException("Can't accept an event, since no transaction is in progress");
		}
		
		// This check is discovery. it is not clear to me yet whether or not we have multiple transactions going at the same time
		// in different directions
		if (m_currentTransactionInProgress.Direction == SolaceChannelTransaction.TransactionDirection.eEgress )
		{
			throw new ChannelException("Can't accept an event, since the current transaction is an egress transaction!");
		}
		m_currentTransactionInProgress.Direction = SolaceChannelTransaction.TransactionDirection.eIngress;
		
		try 
		{
			// create an instance of the wrapper around the event
			SolaceChannelEvent ev = new SolaceChannelEvent(flumeEvent);
			
			m_currentTransactionInProgress.m_queue.put(ev);
			LOG.debug("Current ingress transaction now has " + m_currentTransactionInProgress.m_queue.size() + " events in it" );
		} 
		catch (Exception e) 
		{
			throw new ChannelException(e);
		}
	}

	public void onMessageFromTheRouter(BytesMessage flumeEventAsASolaceMessage)
	{
		m_queueOfSolaceMessages.add(flumeEventAsASolaceMessage);
	}
	
	/**
	 * Delivers an event to the sink, returns null if none available
	 */
	@Override
	public Event take() throws ChannelException 
	{
		LOG.debug("take() is called");
		
		// we must have an active transaction 
		if (m_currentTransactionInProgress == null)
		{
			throw new ChannelException("Can't deliver an event, since no transaction is in progress");
		}
		
		if (m_currentTransactionInProgress.Direction == SolaceChannelTransaction.TransactionDirection.eIngress )
		{
			throw new ChannelException("Can't deliver an event, since the current transaction is an Ingress transaction!");
		}
		m_currentTransactionInProgress.Direction = SolaceChannelTransaction.TransactionDirection.eEgress;

		Event rc = null;
		BytesMessage flumeEventAsASolaceMessage = this.m_queueOfSolaceMessages.poll();
		if (flumeEventAsASolaceMessage == null) 
		{
			LOG.debug("Leaving take() with NO event");
		}
		else
		{
			// ok, we have pulled an event out of the central storage queue, and we will now track it in 
			// the egress transaction. These events would only be ACKed upon commit.
			try {
				try 
				{
					// convert the solace message back to a flume event, and put in on the queue waiting to be taken by the sink
					// note the pair, we can't discard the solace msg yet, because it hasn't been acked
					LOG.debug("About to convert the solace message to a flume message.");

					Event flumeEvent = FlumeEventToSolaceMessageConverter.solaceToFlume(flumeEventAsASolaceMessage);
					rc = flumeEvent;
					LOG.debug("Successfully converted the solace message to a flume message.");
					
					MessagePairEvent pair = new MessagePairEvent(flumeEvent, flumeEventAsASolaceMessage);
					m_currentTransactionInProgress.m_queue.put(pair);
					LOG.debug("Current egress transaction now has " + m_currentTransactionInProgress.m_queue.size() + " events in it." );
				} 
				catch (SDTException e) 
				{
					e.printStackTrace();
				}
			} 
			catch (InterruptedException e) 
			{
				throw new ChannelException(e);
			}
			LOG.debug("Leaving take() with an event");
		}
		return rc;
	}
	
	private void commitIngress() {
		LOG.debug("its an ingress commitTransaction()");
		// accept the batch
		List<SolaceChannelEvent> allInBatch = new ArrayList<SolaceChannelEvent>();
		m_currentTransactionInProgress.m_queue.drainTo(allInBatch);
		LOG.debug("There are " + allInBatch.size() + " events in this transaction" );
		
		for (SolaceChannelEvent solEvent : allInBatch)
		{
			Event flumeEvent = solEvent.getEvent();
			String strCorrId = "";
			try 
			{
				// convert it to a Solace message and pump it into the Solace Q
				BytesMessage flumeEventAsASolaceMessage = FlumeEventToSolaceMessageConverter.flumeToSolace(this.m_session, flumeEvent);
				this.m_session.publishToQueue(flumeEventAsASolaceMessage);
				strCorrId = flumeEventAsASolaceMessage.getCorrelationId();
				LOG.debug("The event has been pushed into the Solace '" + this.m_strQueueName + "' queue with an Id of '" + strCorrId + "'.");
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	private void commitEgress() {
		LOG.debug("its an egress commitTransaction()");
		// this is an egress transaction, so we would need to ACK all of the messages in the xaction with the router
		// 
		
		List<SolaceChannelEvent> allInBatch = new ArrayList<SolaceChannelEvent>();
		m_currentTransactionInProgress.m_queue.drainTo(allInBatch);
		LOG.debug("There are " + allInBatch.size() + " events/messages in this transaction" );
		
		for (SolaceChannelEvent solEvent : allInBatch)
		{
			MessagePairEvent pair = (MessagePairEvent) solEvent; // cast it down
			BytesMessage solaceMsg = pair.getSolaceMessage();
			String correlationId = solaceMsg.getCorrelationId(); 
			solaceMsg.ackMessage();
			LOG.debug("ACKED Solace message " + correlationId);
		}
		
		// and just get rid of all the events and messages
		m_currentTransactionInProgress.m_queue.clear();
	}
	/**
	 * Called from our transaction class, which is called from either the source or the sink
	 */
	public void commitTransaction() {
		LOG.debug("channel entering commitTransaction()");
		if (m_session.isRunning()) {
			if (this.m_currentTransactionInProgress.Direction == SolaceChannelTransaction.TransactionDirection.eIngress)
			{
				commitIngress();
			}
			else
			{
				commitEgress();
			}
		}
		else {
			LOG.warn("Solace session is not running. Commit ignored.");
		}
	}
	public void rollbackTransaction() 
	{
		if (this.m_currentTransactionInProgress.Direction == SolaceChannelTransaction.TransactionDirection.eIngress)
		{
			// its on the way in, the source is rolling back so just drop the events
			m_currentTransactionInProgress.m_queue.clear();
		}
		else
		{
			// its on the way out and the sink is rolling back.. move the events back to central storage.. 
			List<SolaceChannelEvent> allInBatch = new ArrayList<SolaceChannelEvent>();
			m_currentTransactionInProgress.m_queue.drainTo(allInBatch);

			for (SolaceChannelEvent solEvent : allInBatch)
			{
				// egress xactions have pairs with solace messages
				MessagePairEvent pair = (MessagePairEvent) solEvent; // cast it down 

				// TODO: THAT this code results in the wrong ordering of messages! we are potentially putting them behind older messages!
				m_queueOfSolaceMessages.add(pair.getSolaceMessage());
			}
			
		}
		
	}
	public void closeTransaction()
	{
		this.m_currentTransactionInProgress = null;
	}
	
	/**
	 * Called by the flume framework when shutdown is in progress. Clean up here.
	 */
	@Override
	public void stop() {
		//TODO: any transaction behavior to be cleaned up here?
		LOG.debug("Attempting to shutdown the Solace session");
		this.m_session.stop();
		LOG.debug("The Solace session has been cleaned up");
		
		super.stop();
	}
	
	/**
	 * the flume framework calls this at startup: a chance for us to pick up the properties set in the flume config file 
	 * for the channel. An example config looks like this:
	 * 
	 * # Use a Solace channel 
	 * a1.channels.c1.type = com.solacesystems.flume.channel.SolaceChannel
	 * a1.channels.c1.capacity = 1000
	 * a1.channels.c1.transactionCapacity = 100
	 */
	@Override
	public void configure(Context flumeContext) 
	{
		if (flumeContext.containsKey(CONFIG_KEY_OVERALL_CAPACITY))
		{
			// NOTE: its not clear if we will use this setting or not when batch mode optimization is considered
			m_nTransactionCapacity = flumeContext.getInteger(CONFIG_KEY_OVERALL_CAPACITY);
		    LOG.info("Solace Channel transaction capacity set to : " + m_nTransactionCapacity);
		}
		
		// pull the properties directly out of the flume config and push them into the Solace props object. No need to interim
		// variables
		this.m_session.properties.setProperty(JCSMPProperties.HOST, getMandatoryStringProperty(flumeContext, CONFIG_KEY_SOLACE_HOSTROUTER));  // msg-backbone ip:port
		this.m_session.properties.setProperty(JCSMPProperties.VPN_NAME, getMandatoryStringProperty(flumeContext, CONFIG_KEY_SOLACE_VPN));  // msg-backbone ip:port
		this.m_session.properties.setProperty(JCSMPProperties.USERNAME, getMandatoryStringProperty(flumeContext, CONFIG_KEY_SOLACE_CLIENTUSERNAME));  // msg-backbone ip:port
		this.m_session.properties.setProperty(JCSMPProperties.PASSWORD, getMandatoryStringProperty(flumeContext, CONFIG_KEY_SOLACE_PW));  // msg-backbone ip:port
		this.m_strQueueName = getMandatoryStringProperty(flumeContext, CONFIG_KEY_SOLACE_QUEUENAME);
		
	    LOG.info("Solace Channel initialized: " + getName());
	    
	    try 
	    {
			this.m_session.connect();
			LOG.info("Connected to the Solace Event Broker");
			this.m_session.createQueueProducer(m_strQueueName);
			this.m_session.bindToQueueOnAnotherThread(m_strQueueName);
			LOG.info("Bound to queue '" + m_strQueueName + "' successfully");
		} 
	    catch (JCSMPException e) 
	    {
			e.printStackTrace();
		}
	}
	
	/**
	 * Utility method, gets a string property from the Flume context, logs a warning if the property is missing
	 * @param flumeContext
	 * @param propertyName
	 * @return
	 */
	private String getMandatoryStringProperty(Context flumeContext, String propertyName)
	{
		String strRc = "";
		if (flumeContext.containsKey(propertyName))
		{
			strRc = flumeContext.getString(propertyName);
		    LOG.debug("Solace Channel property '" + propertyName + "' = '" + strRc + "'.");
		}
		else
		{
			LOG.warn("Solace Channel property '" + propertyName + "' is missing! The channel will likely fail " +
					"without this property set in your flume properties files.");
		}
		return strRc;
	}
}
