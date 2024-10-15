package io.github.cyrilsochor.kafky.api.job;

import io.github.cyrilsochor.kafky.api.component.Component;

public interface Consumer<V> extends Component {

    void consume(V value) throws Exception;

}
