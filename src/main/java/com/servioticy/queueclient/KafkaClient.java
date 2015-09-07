package com.servioticy.queueclient;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by alvaro on 07/09/15.
 */
public class KafkaClient extends QueueClient {
    private KafkaProducer<Integer, Object> producer;
    @Override
    protected boolean putImpl(Object item) {
        try {
            this.producer.send(new ProducerRecord<Integer, Object>(this.getRelativeAddress(), item.hashCode(), item)).get();
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
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBaseAddress().replace(" ", ","));
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        this.producer = new KafkaProducer<Integer, Object>(props);

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
