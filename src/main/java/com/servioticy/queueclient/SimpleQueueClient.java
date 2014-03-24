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

import java.io.*;
import java.util.LinkedList;

/**
 * @author Álvaro Villalba Navarro <alvaro.villalba@bsc.es>
 */
public class SimpleQueueClient extends QueueClient implements Serializable {

    String filePath;

    public SimpleQueueClient() {
    }

    LinkedList<Object> readQueue() throws ClassNotFoundException, IOException {
        LinkedList<Object> queue;
        try {
            FileInputStream fileIn;
            fileIn = new FileInputStream(filePath);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            queue = (LinkedList<Object>) in.readObject();
            in.close();
            fileIn.close();
        } catch (FileNotFoundException e) {
            queue = new LinkedList<Object>();
        }
        return queue;
    }

    void writeQueue(LinkedList<Object> queue) throws IOException {
        File file = new File(filePath);
        file.delete();
        file.createNewFile();
        FileOutputStream fileOut = new FileOutputStream(file);
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(queue);
        out.close();
        fileOut.close();
    }

    @Override
    protected boolean putImpl(Object item) {
        LinkedList<Object> queue;

        try {
            queue = readQueue();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return false;
        }
        queue.add(item);

        try {
            writeQueue(queue);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    protected void connectImpl() throws QueueClientException {
        filePath = this.getBaseAddress() + this.getRelativeAddress();
    }

    @Override
    protected void disconnectImpl() throws QueueClientException {
        filePath = null;
    }

    @Override
    protected void init(HierarchicalConfiguration config)
            throws QueueClientException {
    }

    @Override
    protected Object getImpl() {
        LinkedList<Object> queue;
        Object returnValue;
        try {
            queue = readQueue();
        } catch (Exception e) {
            return null;
        }
        if (queue.isEmpty()) {
            return null;
        }
        returnValue = queue.getFirst();
        queue.removeFirst();

        try {
            writeQueue(queue);
        } catch (Exception e) {
        }

        return returnValue;
    }

}
