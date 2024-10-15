package io.github.cyrilsochor.kafky.api.job.producer;

public interface RecordDecorator extends RecordProducer {

    void setNextProducer(RecordProducer nextRecordProducer);

    RecordProducer getNextProducer();

}
