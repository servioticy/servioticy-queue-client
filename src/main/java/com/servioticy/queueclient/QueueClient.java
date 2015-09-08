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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.List;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public abstract class QueueClient implements Serializable {

    final static protected Logger logger = LoggerFactory.getLogger(QueueClient.class);

    static final private String DEFAULT_CONFIG_PATH = "queue-client.xml";

    private String baseAddress;
    private String relativeAddress;
	private boolean connected;

	protected QueueClient() {
		
	}

	private static QueueClient createInstance(String address, String queueName, String className, HierarchicalConfiguration classConfig) throws QueueClientException{
        QueueClient instance;
        try {
            instance = (QueueClient) Class.forName(className).newInstance();
            instance.setAddress(address);
            instance.setQueueName(queueName);
            instance.init(classConfig);
        } catch (InstantiationException e) {
            String errMsg = "Unable to instantiate the queue class (" + e.getMessage() + ").";
            logger.error(errMsg);
            throw new QueueClientException(errMsg);
        } catch (IllegalAccessException e) {
            String errMsg = "Unable to load the queue class (" + e.getMessage() + ").";
            logger.error(errMsg);
            throw new QueueClientException(errMsg);
        } catch (ClassNotFoundException e) {
            String errMsg = "The queue class does not exist (" + e.getMessage() + ").";
            logger.error(errMsg);
            throw new QueueClientException(errMsg);
        }
		
		return instance;
	}
	
	private static QueueClient createInstance(String configPath) throws QueueClientException{

		File f = new File(configPath);
		if(!f.exists() && (Thread.currentThread().getContextClassLoader().getResource(configPath) == null)){
			String errMsg = "'"+ configPath +"' configuration file doesn't exist.";
			logger.error(errMsg);
			throw new QueueClientException(errMsg);
		}
        String className;
        HierarchicalConfiguration	config,
                queueConfig;
        String address;
        String queueName;
        try {
            config = new XMLConfiguration(configPath);
            config.setExpressionEngine(new XPathExpressionEngine());
            queueConfig = (HierarchicalConfiguration) config
                    .configurationAt("classConf");
			address = config.getString("address", "localhost:2181");
			queueName = config.getString("queueName", "updates");
            className = config.getString("className", "com.servioticy.queueclient.KafkaClient");
			
		}  catch (ConfigurationException e) {
			String errMsg = "'"+ configPath +"' configuration file is malformed (" + e.getMessage() + ").";
			logger.error(errMsg);
			throw new QueueClientException(errMsg);
		}
		
		return createInstance(address, queueName, className, queueConfig);
	}
	
	public static QueueClient factory(String address, String queueName, String className, HierarchicalConfiguration classConf) throws QueueClientException {
        return createInstance(address, queueName, className, classConf);
    }

    public static QueueClient factory(String configPath) throws QueueClientException {
        return createInstance(configPath);
    }

    public static QueueClient factory() throws QueueClientException {
        if(System.getProperty("queueClient.config") == null)
            return createInstance(DEFAULT_CONFIG_PATH);
        else
            return createInstance(System.getProperty("queueClient.config"));
    }
	
	public void setAddress(String qBaseAddress){
		this.baseAddress = qBaseAddress;
	}
	
	public void setQueueName(String qRelativeAddress){
		this.relativeAddress = qRelativeAddress;
	}
	
	public String getBaseAddress(){
		return this.baseAddress;
	}
	
	public String getRelativeAddress(){
		return this.relativeAddress;
	}
	
	public void connect() throws QueueClientException {
		if (this.isConnected()) {
			return;
		}

        this.connectImpl();

        this.setConnected(true);
	}

	public void disconnect()  throws QueueClientException{
		if (!this.isConnected()) {
			return;
		}

        this.disconnectImpl();

        this.setConnected(false);
	}

	public boolean put(Object item){
		if (!this.isConnected()) {
			return false;
		}
        return this.putImpl(item);
    }
	
	public int put(List<Object> items) {
		if(items == null || items.size() == 0){
			return 0;
		}

		int putCounter = 0;
		for(; putCounter < items.size(); putCounter++){
			if(!this.put(items.get(putCounter))){
				break;
			}
		}
		return putCounter;
	}
	
	public Object get(){
		if (!this.isConnected()) {
			return false;
		}
        return this.getImpl();
    }
	
	@Override
	protected void finalize() throws Throwable {
		disconnect();
		super.finalize();
	}

    protected abstract boolean putImpl(Object item);

    protected abstract Object getImpl();

    protected abstract void connectImpl() throws QueueClientException;

    protected abstract void disconnectImpl() throws QueueClientException;

    protected abstract void init(HierarchicalConfiguration config) throws QueueClientException;

	public boolean isConnected() {
		return this.connected;
	}

	private void setConnected(boolean connected) {
		this.connected = connected;
	}

//	public boolean isTemporalConnection() {
//		return temporalConnection;
//	}
//
//	public void setTemporalConnection(boolean temporalConnection) {
//		this.temporalConnection = temporalConnection;
//	}
}
