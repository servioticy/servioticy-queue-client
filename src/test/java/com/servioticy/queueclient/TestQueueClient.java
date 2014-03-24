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

import org.apache.commons.configuration.HierarchicalConfiguration;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class TestQueueClient extends QueueClient {

    private Boolean connectException;
    private Boolean disconnectException;
    private Boolean initQueueException;

    public Boolean initCalled = false;
    public Boolean connectCalled = false;
    public Boolean disconnectCalled = false;

    protected TestQueueClient() {
        this.connectException = false;
        this.disconnectException = false;
        this.initQueueException = false;
    }

    @Override
    protected boolean putImpl(Object item) {
        if (item instanceof Boolean) {
            return (Boolean) item;
        }
        return false;
    }

    @Override
    protected void connectImpl() throws QueueClientException {
        connectCalled = true;
        if (this.connectException) {
            String errMsg = "TestQueueClient expected exception.";
            logger.error(errMsg);
            throw new QueueClientException(errMsg);
        }

    }

    @Override
    protected void disconnectImpl() throws QueueClientException {
        disconnectCalled = true;
        if (this.disconnectException) {
            String errMsg = "TestQueueClient expected exception.";
            logger.error(errMsg);
            throw new QueueClientException(errMsg);
        }

    }

    @Override
    protected void init(HierarchicalConfiguration config)
            throws QueueClientException {
        initCalled = true;
        if (config != null) {
            this.connectException = config.getBoolean("connectException", false);
            this.disconnectException = config.getBoolean("disconnectException", false);
            this.initQueueException = config.getBoolean("initQueueException", false);
        }

        if (this.initQueueException) {
            String errMsg = "TestQueueClient expected exception.";
            logger.error(errMsg);
            throw new QueueClientException(errMsg);
        }

    }

    @Override
    protected Object getImpl() {
        // TODO Auto-generated method stub
        return null;
    }

}
