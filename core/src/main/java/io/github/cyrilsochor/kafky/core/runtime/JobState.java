package io.github.cyrilsochor.kafky.core.runtime;

public enum JobState {
    INITIALIZING(false, 0),

    PREPARING(false, 0),
    PREPARED(false, 0),

    STARTING(false, 0),
    STARTED(false, 0),

    RUNNING(false, 0),

    CANCELING(false, 0),
    CANCELED(true, 0),

    SUCCESS(true, 0),
    FAILED(true, 1),
    ;

    final boolean finite;
    final int exitStatus;

    JobState(boolean finite, int exitStatus) {
        this.finite = finite;
        this.exitStatus = exitStatus;
    }

    public boolean isFinite() {
        return finite;
    }

    public int getExitStatus() {
        return exitStatus;
    }

}