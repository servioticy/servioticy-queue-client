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

import net.spy.memcached.*;
import org.apache.commons.configuration.HierarchicalConfiguration;

import java.io.IOException;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class KestrelMemcachedClient extends QueueClient {

    private int expire = 0;

	MemcachedClient kestrelClient;

    public KestrelMemcachedClient() {
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public int getExpire() {
        return expire;
    }
	@Override
    protected boolean putImpl(Object item) {
        try{
			// This is blocking by now
			return kestrelClient.set(this.getRelativeAddress(), expire, item).get();
		} catch (Exception e) {
			logger.error("KestrelMemcached put failed ({})", e.getMessage());
			return false;
		} 
		
	}

	@Override
    protected Object getImpl() {
        return kestrelClient.get(this.getRelativeAddress());
		
	}
	
	@Override
    protected void connectImpl() throws QueueClientException {
        if(kestrelClient != null){
			return;
		}
		try {
			ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
			// VERY IMPORTANT! Otherwise, spymemcached optimizes away concurrent gets
	        builder.setShouldOptimize(false);
	        // Retry upon failure
	        builder.setFailureMode(FailureMode.Retry);
	        ConnectionFactory memcachedConnectionFactory = builder.build();
			kestrelClient = new MemcachedClient(
					memcachedConnectionFactory,
					AddrUtil.getAddresses(this.getBaseAddress()));
		} catch (IOException e) {
			kestrelClient = null;
			String errMsg = "Unable to connect to the queue (" + e.getMessage() + ").";
			logger.error(errMsg);
			throw new QueueClientException(errMsg);
		}
	}

	@Override
    protected void disconnectImpl() {
        if(kestrelClient == null){
			return;
		}

		kestrelClient.shutdown();

		kestrelClient = null;
	}

	@Override
	protected void init(HierarchicalConfiguration config) throws QueueClientException {
		checkBaseAddress();
		checkRelativeAddress();
		// If config is null, just load the defaults.
		if(config == null){
			config = new HierarchicalConfiguration();
		}
		this.expire = config.getInt("expire", 0);
	}
	
	private void checkBaseAddress() throws QueueClientException{
		if(this.getBaseAddress() == null){
			String errMsg = "Malformed configuration file: No servers defined for KestrelMemcachedClient (baseAddress).";
			logger.error(errMsg);
			throw new QueueClientException(errMsg);
		}
	}
	
	private void checkRelativeAddress() throws QueueClientException{
		if(this.getRelativeAddress() == null){
			String errMsg = "Malformed configuration file: No queue name defined for KestrelMemcachedClient (relativeAddress).";
			logger.error(errMsg);
			throw new QueueClientException(errMsg);
		}
	}

}
