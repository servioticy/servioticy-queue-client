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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.LinkedList;

import org.junit.Assert;
import org.junit.Test;

import com.servioticy.queueclient.QueueClient;
import com.servioticy.queueclient.QueueClientException;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 * 
 */
public class QueueClientTest {

	@Test
	public void testFactoryDefault() {
		try {
			QueueClient sqc = QueueClient.factory();
			Assert.assertTrue("TestQueueClient returned", sqc instanceof TestQueueClient);
			Assert.assertTrue("'init' called", ((TestQueueClient) sqc).initCalled);
		} catch (QueueClientException e) {
			// TODO Auto-generated catch block
			fail("Factory failed: " + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testFactoryConfigPathDefault() {
		try {
			QueueClient sqc = QueueClient.factory("/conf/nondefault.xml");
			Assert.assertTrue("TestQueueClient returned", sqc instanceof TestQueueClient);
			Assert.assertTrue("'init' called", ((TestQueueClient) sqc).initCalled);
		} catch (QueueClientException e) {
			fail("Factory failed: " + e.getMessage() + "\n" + e.getStackTrace());
		}
	}
	
	@Test
	public void testFactory(){
		try {
			QueueClient sqc = QueueClient.factory("test2", null, null);
			Assert.assertTrue("TestQueueClient returned", sqc instanceof TestQueueClient);
			Assert.assertTrue("'init' called", ((TestQueueClient) sqc).initCalled);
		} catch (QueueClientException e) {
			fail("Factory failed: " + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testFactoryConfigPath() {
		try {
			QueueClient sqc = QueueClient.factory("/conf/nondefault.xml", "test2", null, null);
			Assert.assertTrue("TestQueueClient returned", sqc instanceof TestQueueClient);
			Assert.assertTrue("'init' called", ((TestQueueClient) sqc).initCalled);
		} catch (QueueClientException e) {
			fail("Factory failed: " + e.getMessage() + "\n" + e.getStackTrace());
		}
	}
	
	@Test(expected = QueueClientException.class)
	public void testFactoryNonexistentFileDefault() throws QueueClientException{
		QueueClient.factory("/conf/nonexistent.xml");

	}
	
	@Test(expected = QueueClientException.class)
	public void testFactoryStringDirConf() throws QueueClientException{
		QueueClient.factory("/conf");

	}
	
	@Test(expected = QueueClientException.class)
	public void testFactoryInitException() throws QueueClientException{
		QueueClient.factory("initException", null, null);
	}

	@Test
	public void testConnectAndDisconnect() {
		try {
			QueueClient sqc = QueueClient.factory();
			sqc.connect();
			Assert.assertTrue("Is connected", sqc.isConnected());
			sqc.disconnect();
			Assert.assertTrue("Is not connected", !sqc.isConnected());
			Assert.assertTrue("'disconnectWrapper' called", ((TestQueueClient) sqc).disconnectCalled);
		} catch (QueueClientException e) {
			fail("Connect failed: " + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test(expected = QueueClientException.class)
	public void testConnectException() throws QueueClientException {
		QueueClient sqc = QueueClient.factory("connectException", null, null);
		sqc.connect();
	}
	
	@Test(expected = QueueClientException.class)
	public void testDisconnectException() throws QueueClientException {
		QueueClient sqc = QueueClient.factory("disconnectException", null, null);
		sqc.connect();
		sqc.disconnect();
	}

	@Test
	public void testPutObjectDisconnected() {
		try {
			QueueClient sqc = QueueClient.factory();
			Assert.assertFalse("Nothing is put because it is not connected", sqc.put(true));
		} catch (QueueClientException e) {
			fail("Put object failed: " + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

	@Test
	public void testPutListOfObject() {
		try{
			QueueClient sqc = QueueClient.factory();
			sqc.connect();
			int elements = sqc.put(new LinkedList<Object>(Arrays.asList(true, false, true)));
			Assert.assertTrue("Only the first element is put", elements == 1);
		} catch (QueueClientException e) {
			fail("Put list of objects failed: " + e.getMessage() + "\n" + e.getStackTrace());
		}
	}

}
