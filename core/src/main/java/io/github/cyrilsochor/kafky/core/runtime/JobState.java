package io.github.cyrilsochor.kafky.core.runtime;

public enum JobState {
    INITIALIZING(false, 0), // creating object without connectoin to target service

    PREPARING(false, 0), // preparing data without connectoin to target service
    PREPARED(false, 0), // all data are prepared

    STARTING(false, 0), // creating connection to target service
    STARTED(false, 0), // connection to target service is created

    RUNNING(false, 0), // sending primary data to target service 

    CANCELING(false, 0), // received CANCEL request but stil closing the connection to target service
    CANCELED(true, 0), // received CANCEL request and connection to target service is closed, waiting for application exit

    SUCCESS(true, 0), // everything is done without error, waiting for application exit
    FAILED(true, 1), // everything is done with some error, waiting for application exit
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