package io.github.cyrilsochor.kafky.api.job;

import io.github.cyrilsochor.kafky.api.component.Component;

public interface Producer<V> extends Component {

    V produce() throws Exception;

}
