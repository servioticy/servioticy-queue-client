/*******************************************************************************
 * Copyright 2014 Barcelona Supercomputing Center (BSC)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/ 
package com.servioticy.queueclient;

import java.util.LinkedList;

import org.apache.commons.configuration.HierarchicalConfiguration;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class SimpleQueueClient extends QueueClient {
	
	LinkedList<Object> queue;

	public SimpleQueueClient(){
		this.queue = new LinkedList<Object>();
	}
	
	@Override
	protected boolean putWrapper(Object item) {
		queue.add(item);
		return true;
	}

	@Override
	protected void connectWrapper() throws QueueClientException {
	}

	@Override
	protected void disconnectWrapper() throws QueueClientException {
	}

	@Override
	protected void init(HierarchicalConfiguration config)
			throws QueueClientException {
	}

	@Override
	protected Object getWrapper() {
		return queue.getFirst();
	}

}
