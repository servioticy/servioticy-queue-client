package com.servioticy.queueclient;

/**
 * Created by alvaro on 10/09/15.
 */
public class Test {
    public static void main(String[] args) throws QueueClientException {
        QueueClient qc = QueueClient.factory("minerva-1001:9092 minerva-1002:9092 minerva-1003:9092",
                "updates", "com.servioticy.queueclient.KafkaClient", null);
        qc.connect();
        qc.put("lala");

    }
}
