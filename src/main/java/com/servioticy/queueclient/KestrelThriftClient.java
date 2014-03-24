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
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 *         <p/>
 *         Derived from:
 *         https://github.com/nathanmarz/storm-kestrel/blob/master/src/jvm/backtype/storm/spout/KestrelThriftSpout.java
 */
public class KestrelThriftClient extends QueueClient {

    private static class KestrelClientInfo {
        public Long blacklistTillTimeMs;
        public String host;
        public int port;

        private _KestrelThriftClient client;

        public KestrelClientInfo(String host, int port) {
            this.host = host;
            this.port = port;
            this.blacklistTillTimeMs = 0L;
            this.client = null;
        }

        public _KestrelThriftClient getValidClient() throws TException {
            if (this.client == null) { // If client was blacklisted, remake it.
                logger.info("Attempting reconnect to kestrel " + this.host + ":" + this.port);
                this.client = new _KestrelThriftClient(this.host, this.port);
            }
            return this.client;
        }

        public void closeClient() {
            if (this.client != null) {
                this.client.close();
                this.client = null;
            }
        }
    }

    private List<KestrelClientInfo> _kestrels;
    public static final long BLACKLIST_TIME_MS = 1000 * 60;

    public KestrelThriftClient() {
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        //TODO: should switch this to maxTopologyMessageTimeout
        Number timeout = (Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS);
        _messageTimeoutMillis = 1000 * timeout.intValue();
        _collector = collector;
        _emitIndex = 0;
        _kestrels = new ArrayList<KestrelClientInfo>();
        int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
        int myIndex = context.getThisTaskIndex();
        int numHosts = _hosts.size();
        if (numTasks < numHosts) {
            for (String host : _hosts) {
                _kestrels.add(new KestrelClientInfo(host, _port));
            }
        } else {
            String host = _hosts.get(myIndex % numHosts);
            _kestrels.add(new KestrelClientInfo(host, _port));
        }
    }

    public void close() {
        for (KestrelClientInfo info : _kestrels) info.closeClient();

        // Closing the client connection causes all the open reliable reads to be aborted.
        // Thus, clear our local buffer of these reliable reads.
        _emitBuffer.clear();

        _kestrels.clear();
    }

    private void blacklist(KestrelClientInfo info, Throwable t) {
        logger.warn("Failed to read from Kestrel at " + info.host + ":" + info.port, t);

        //this case can happen when it fails to connect to Kestrel (and so never stores the connection)
        info.closeClient();
        info.blacklistTillTimeMs = System.currentTimeMillis() + BLACKLIST_TIME_MS;

        int index = _kestrels.indexOf(info);

        // we just closed the connection, so all open reliable reads will be aborted. empty buffers.
        for (Iterator<EmitItem> i = _emitBuffer.iterator(); i.hasNext(); ) {
            EmitItem item = i.next();
            if (item.sourceId.index == index) i.remove();
        }
    }

    @Override
    protected boolean putImpl(Object item) {
        return false;
    }

    @Override
    protected Object getImpl() {
        return null;
    }

    @Override
    protected void connectImpl() throws QueueClientException {
    }

    @Override
    protected void disconnectImpl() throws QueueClientException {
    }

    @Override
    protected void init(HierarchicalConfiguration config) throws QueueClientException {
    }
}
