package com.servioticy.queueclient;

import java.util.LinkedList;

import org.apache.commons.configuration.HierarchicalConfiguration;

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
