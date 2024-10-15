package io.github.cyrilsochor.kafky.api.job.producer;

import io.github.cyrilsochor.kafky.api.component.Component;
import io.github.cyrilsochor.kafky.api.job.Consumer;

public interface ProducedRecordListener extends Consumer<ProducedRecord>, Component {

}
