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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;

/**
 * Converts a Solace bytes message (with only text properties in it's map) into a flume event
 * and vice-versa
 *
 */
public class FlumeEventToSolaceMessageConverter 
{
	private static long m_nNextCorreleationId = 1;
	/**
	 * Converts a Solace bytes message into a flume event
	 * @param msg
	 * @return
	 * @throws SDTException
	 */
	public static Event solaceToFlume(BytesMessage msg) throws SDTException
	{
		byte[] body = msg.getUserData();
		SDTMap map = msg.getProperties();
		Map<String, String> headers = new HashMap<String, String>();
		
		if (map != null)
		{
			Set<String> keys = map.keySet();
			for (String oneKey : keys)
			{
				String value = map.getString(oneKey);
				headers.put(oneKey, value);
			}
		}
		return EventBuilder.withBody(body, headers);
	}
	/**
	 * Converts a flume event to a Solace butes message
	 * 
	 * @param sess
	 * @param flumeEvent
	 * @return
	 * @throws SDTException
	 */
	public static BytesMessage flumeToSolace(SolaceSession sess, Event flumeEvent) throws SDTException
	{
		BytesMessage messageRc = sess.MessageFactory();
		messageRc.setCorrelationId("" + m_nNextCorreleationId);
		m_nNextCorreleationId++;
		
		byte[] givenPayload = flumeEvent.getBody();
		byte[] basePayload = Arrays.copyOf(givenPayload, givenPayload.length);
		messageRc.setUserData(basePayload);
		
	    Map<String, String> headerMap = flumeEvent.getHeaders();
		if (headerMap != null && headerMap.size() > 0) {
			SDTMap map = messageRc.getProperties();
			if (map == null)
			{
				map = JCSMPFactory.onlyInstance().createMap();
			}
			
			for (Map.Entry<String, String> entry : headerMap.entrySet()) {
				String name = entry.getKey();
				String value = entry.getValue();
				map.putString(name, value); 
			}
			messageRc.setProperties(map);
	    }
		return messageRc;
	}
}
