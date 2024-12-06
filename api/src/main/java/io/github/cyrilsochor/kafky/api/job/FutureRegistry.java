package io.github.cyrilsochor.kafky.api.job;

import java.util.concurrent.Future;

public interface FutureRegistry {

    void addFuture(Future<?> future);

}
