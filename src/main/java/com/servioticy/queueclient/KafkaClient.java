package com.servioticy.queueclient;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Properties;

/**
 * Created by alvaro on 07/09/15.
 */
public class KafkaClient extends QueueClient {
    private KafkaProducer<String, byte[]> producer;
    @Override
    protected boolean putImpl(Object item) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(item);
            this.producer.send(new ProducerRecord<String, byte[]>(this.getQueueName(), Integer.toHexString(item.hashCode()), bos.toByteArray())).get();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    protected Object getImpl() {
        return null;
    }

    @Override
    protected void connectImpl() throws QueueClientException {
        if(this.producer != null){
            return;
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getAddress().replace(" ", ","));
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.producer = new KafkaProducer<String, byte[]>(props);

    }

    @Override
    protected void disconnectImpl() throws QueueClientException {
        if(this.producer == null){
            return;
        }

        this.producer.close();
        this.producer = null;
    }

    @Override
    protected void init(HierarchicalConfiguration config) throws QueueClientException {
        this.producer = null;
    }
}
