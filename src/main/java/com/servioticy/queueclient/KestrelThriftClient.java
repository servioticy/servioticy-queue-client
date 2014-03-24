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

import net.lag.kestrelcom.thrift.Item;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.thrift.TException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 *
 * Derived from:
 *         https://github.com/nathanmarz/storm-kestrel/blob/master/src/jvm/backtype/storm/spout/KestrelThriftSpout.java
 *         https://github.com/dustin/java-memcached-client/blob/master/src/main/java/net/spy/memcached/transcoders/BaseSerializingTranscoder.java
 */
public class KestrelThriftClient extends QueueClient {

    private int expire = 0;

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

    public void setExpire(int expire) {
        this.expire = expire;

    }

    public int getExpire() {
        return expire;
    }

    private void blacklist(KestrelClientInfo info, Throwable t) {
        logger.warn("Failed to read from Kestrel at " + info.host + ":" + info.port, t);

        //this case can happen when it fails to connect to Kestrel (and so never stores the connection)
        info.closeClient();
        info.blacklistTillTimeMs = System.currentTimeMillis() + BLACKLIST_TIME_MS;
    }

    @Override
    protected boolean putImpl(Object item) {
        int numKestrels = _kestrels.size();
        int kestrelIndex = Math.abs(item.hashCode()) % numKestrels;
        KestrelClientInfo info;
        long now = System.currentTimeMillis();
        for (int i = 0; i < numKestrels; i++) {
            info = _kestrels.get((kestrelIndex + i) % numKestrels);
            if (now > info.blacklistTillTimeMs) {
                List<ByteBuffer> items = new ArrayList<ByteBuffer>();
                items.add(ByteBuffer.wrap(serialize(item)));
                try {
                    if (item instanceof String) {
                        info.getValidClient().put(getRelativeAddress(), (String) item, 0);
                        return true;
                    } else if (info.getValidClient().put(getRelativeAddress(), items, 0) == 1) {
                        return true;
                    }
                } catch (TException e) {
                    blacklist(info, e);
                    continue;
                }
            }
        }
        return false;
    }

    @Override
    protected Object getImpl() {
        int numKestrels = _kestrels.size();
        int firstKestrelIndex = (new Random()).nextInt() % numKestrels;
        KestrelClientInfo info;
        long now = System.currentTimeMillis();
        List<Item> items = null;
        for (int i = 0; i < numKestrels; i++) {
            info = _kestrels.get((firstKestrelIndex + i) % numKestrels);
            if (now > info.blacklistTillTimeMs) {
                try {
                    items = info.getValidClient().get(getRelativeAddress(), 1, 0, 0);
                } catch (TException e) {
                    blacklist(info, e);
                    continue;
                }
                for (Item item : items) {
                    Object retItem = deserialize(item.get_data());
                    if (retItem == null) {
                        continue;
                    }
                    return retItem;
                }
            }
            continue;
        }
        return null;
    }

    @Override
    protected void connectImpl() throws QueueClientException {
        String[] _hosts = this.getBaseAddress().split(" ");

        _kestrels = new ArrayList<KestrelClientInfo>();
        int numHosts = _hosts.length;
        for (String host : _hosts) {
            if (host.equals("")){
                continue;

            }
            String[] addrPort = host.split(":");
            _kestrels.add(new KestrelClientInfo(addrPort[0], Integer.parseInt(addrPort[1])));
        }
    }

    @Override
    protected void disconnectImpl() throws QueueClientException {
        for (KestrelClientInfo info : _kestrels) info.closeClient();
        _kestrels.clear();
    }

    @Override
    protected void init(HierarchicalConfiguration config) throws QueueClientException {
        checkBaseAddress();
        checkRelativeAddress();
        // If config is null, just load the defaults.
        if (config == null) {
            config = new HierarchicalConfiguration();
        }
        this.expire = config.getInt("expire", 0);
    }

    private void checkBaseAddress() throws QueueClientException {
        if (this.getBaseAddress() == null) {
            String errMsg = "Malformed configuration file: No servers defined for KestrelMemcachedClient (baseAddress).";
            logger.error(errMsg);
            throw new QueueClientException(errMsg);
        }
    }

    private void checkRelativeAddress() throws QueueClientException {
        if (this.getRelativeAddress() == null) {
            String errMsg = "Malformed configuration file: No queue name defined for KestrelMemcachedClient (relativeAddress).";
            logger.error(errMsg);
            throw new QueueClientException(errMsg);
        }
    }

    protected byte[] serialize(Object o) {
        if (o == null) {
            throw new NullPointerException("Can't serialize null");
        }
        byte[] rv = null;
        ByteArrayOutputStream bos = null;
        ObjectOutputStream os = null;
        try {
            bos = new ByteArrayOutputStream();
            os = new ObjectOutputStream(bos);
            os.writeObject(o);
            os.close();
            bos.close();
            rv = bos.toByteArray();
        } catch (IOException e) {
            logger.warn("Non-serializable object", e);
        }
        return rv;
    }

    protected Object deserialize(byte[] in) {
        Object rv = null;
        ByteArrayInputStream bis = null;
        ObjectInputStream is = null;
        try {
            if (in != null) {
                bis = new ByteArrayInputStream(in);
                is = new ObjectInputStream(bis);
                rv = is.readObject();
                is.close();
                bis.close();
            }
        } catch (IOException e) {
            logger.warn("Caught IOException decoding %d bytes of data",
                    in == null ? 0 : in.length, e);
        } catch (ClassNotFoundException e) {
            logger.warn("Caught CNFE decoding %d bytes of data",
                    in == null ? 0 : in.length, e);
        }
        return rv;
    }
}
