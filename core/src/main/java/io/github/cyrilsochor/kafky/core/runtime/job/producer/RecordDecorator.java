package io.github.cyrilsochor.kafky.core.runtime.job.producer;

public interface RecordDecorator extends RecordProducer {

    void setNextProducer(RecordProducer nextRecordProducer);

    RecordProducer getNextProducer();

}
