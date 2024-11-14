package io.github.cyrilsochor.kafky.core.runtime;

public record IterationResult(
        boolean last,
        long consumendMessagesCount,
        long producedMessagesCount) {

    public static IterationResult of(boolean last,
        long consumendMessagesCount,
        long producedMessagesCount) {
        return new IterationResult(last, consumendMessagesCount, producedMessagesCount);
    }

    public static IterationResult go() {
        return new IterationResult(false, 0, 0);
    }

    public static IterationResult stop() {
        return new IterationResult(true, 0, 0);
    }

    public static IterationResult consumedAndGo(int i) {
        return new IterationResult(false, i, 0);
    }

    public static IterationResult producedAndGo(int i) {
        return new IterationResult(false, 0, i);
    }

    public static IterationResult consumedAndStop(int i) {
        return new IterationResult(true, i, 0);
    }

    public static IterationResult producedAndStop(int i) {
        return new IterationResult(true, 0, i);
    }

    public static IterationResult consumed(int i, boolean last) {
        return new IterationResult(last, i, 0);
    }

    public static IterationResult produced(int i, boolean last) {
        return new IterationResult(last, 0, i);
    }

}
