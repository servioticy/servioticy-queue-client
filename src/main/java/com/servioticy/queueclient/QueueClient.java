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

import java.io.Serializable;
import java.util.List;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public abstract class QueueClient implements Serializable {

    final static protected Logger logger = LoggerFactory.getLogger(QueueClient.class);

    static final private String DEFAULT_CONFIG_PATH = "default.xml";

    private String baseAddress;
    private String relativeAddress;
	private boolean connected;

	protected QueueClient() {
		
	}

	private static QueueClient createInstance(String configPath) throws QueueClientException{
		if(configPath == null){
			return null;
		}
		
		QueueClient instance;
		String 	className,
				type;
		HierarchicalConfiguration 	config,
									queueConfig;
		
		try {
            config = new XMLConfiguration(Thread.currentThread().getContextClassLoader().getResource(configPath));
            config.setExpressionEngine(new XPathExpressionEngine());

            if(!config.containsKey("defaultQueue/queueType")){
				String errMsg = "No default queue. Fix your configuration file.";
				logger.error(errMsg);
				throw new QueueClientException(errMsg);
			}
			type = config.getString("defaultQueue/queueType");
			className = config.getString("queue[type='" + type + "']/className");
			instance = (QueueClient) Class.forName(className).newInstance();
			instance.setBaseAddress(config.getString("defaultQueue/baseAddress", null));
			instance.setRelativeAddress(config.getString("defaultQueue/relativeAddress", null));
			queueConfig = (HierarchicalConfiguration) config
					.configurationAt("queue[type='" + type + "']/concreteConf");
			instance.init(queueConfig);
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
		} catch (ConfigurationException e) {
			String errMsg = "'"+ configPath +"' configuration file is malformed (" + e.getMessage() + ").";
			logger.error(errMsg);
			throw new QueueClientException(errMsg);
		}
		
		return instance;
	}
	
	private static QueueClient createInstance(String configPath, String type, String baseAddress, String relativeAddress) throws QueueClientException{

		if(configPath == null){
			return null;
		}
		
		QueueClient instance;
		String className;
		HierarchicalConfiguration	config,
									queueConfig;
		try {
            config = new XMLConfiguration(Thread.currentThread().getContextClassLoader().getResource(configPath));
            config.setExpressionEngine(new XPathExpressionEngine());

            className = config.getString("queue[type='" + type + "']/className");
			instance = (QueueClient) Class.forName(className).newInstance();
			instance.setBaseAddress(baseAddress);
			instance.setRelativeAddress(relativeAddress);
			queueConfig = (HierarchicalConfiguration) config
					.configurationAt("queue[type='" + type + "']/concreteConf");
			instance.init(queueConfig);
			
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
		} catch (ConfigurationException e) {
			String errMsg = "'"+ configPath +"' configuration file is malformed (" + e.getMessage() + ").";
			logger.error(errMsg);
			throw new QueueClientException(errMsg);
		}
		
		return instance;
	}
	
	public static QueueClient factory() throws QueueClientException{
		return createInstance(DEFAULT_CONFIG_PATH);
	}
	
	public static QueueClient factory(String configPath) throws QueueClientException {
		return createInstance(configPath);
	}
	
	public static QueueClient factory(String qType, String qBaseAddress, String qRelativeAddress) throws QueueClientException{
		return createInstance(DEFAULT_CONFIG_PATH, qType, qBaseAddress, qRelativeAddress);
	}
	
	public static QueueClient factory(String configPath, String qType, String qBaseAddress, String qRelativeAddress) throws QueueClientException{
		return createInstance(configPath, qType, qBaseAddress, qRelativeAddress);
		
	}
	
	public void setBaseAddress(String qBaseAddress){
		this.baseAddress = qBaseAddress;
	}
	
	public void setRelativeAddress(String qRelativeAddress){
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

		this.connectWrapper();

		this.setConnected(true);
	}

	public void disconnect()  throws QueueClientException{
		if (!this.isConnected()) {
			return;
		}

		this.disconnectWrapper();

		this.setConnected(false);
	}

	public boolean put(Object item){
		if (!this.isConnected()) {
			return false;
		}	
		return this.putWrapper(item);
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
		return this.getWrapper();
	}
	
	@Override
	protected void finalize() throws Throwable {
		disconnect();
		super.finalize();
	}

	protected abstract boolean putWrapper(Object item);
	protected abstract Object getWrapper();
	
	protected abstract void connectWrapper() throws QueueClientException;

	protected abstract void disconnectWrapper() throws QueueClientException;

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
