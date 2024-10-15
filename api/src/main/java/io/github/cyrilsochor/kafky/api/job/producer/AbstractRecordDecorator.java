package io.github.cyrilsochor.kafky.api.job.producer;

import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class AbstractRecordDecorator implements RecordDecorator {

    protected RecordProducer nextProducer;

    protected abstract ProducerRecord<Object, Object> decorate(ProducerRecord<Object, Object> source) throws Exception;

    @Override
    public void setNextProducer(RecordProducer nextProducer) {
        this.nextProducer = nextProducer;
    }

    @Override
    public RecordProducer getNextProducer() {
        return this.nextProducer;
    }

    @Override
    public void init() throws Exception {
        nextProducer.init();
    }

    @Override
    public ProducerRecord<Object, Object> produce() throws Exception {
        return decorate(nextProducer.produce());
    }

    @Override
    public void close() throws Exception {
        nextProducer.close();
    }

}
