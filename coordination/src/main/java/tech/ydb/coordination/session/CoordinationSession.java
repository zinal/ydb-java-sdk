package tech.ydb.coordination.session;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.coordination.SessionRequest;
import tech.ydb.coordination.SessionResponse;
import tech.ydb.coordination.observer.Observer;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcReadWriteStream;

/**
 * @author Kirill Kurdyukov
 */
public class CoordinationSession {

    private final GrpcReadWriteStream<SessionResponse, SessionRequest> coordinationStream;
    private final AtomicBoolean isWorking = new AtomicBoolean(true);
    private final CompletableFuture<SessionResponse> stoppedFuture = new CompletableFuture<>();
    private final AtomicReference<Long> sessionId = new AtomicReference<>(null);

    private static final Logger logger = LoggerFactory.getLogger(CoordinationSession.class);

    public CoordinationSession(GrpcReadWriteStream<SessionResponse, SessionRequest> coordinationStream) {
        this.coordinationStream = coordinationStream;
    }

    @Nullable
    public Long getSessionId() {
        return sessionId.get();
    }

    public CompletableFuture<Status> start(Observer observer) {
        return coordinationStream.start(
                message -> {
                    logger.trace("Message received:\n{}", message);

                    if (message.hasSessionStopped()) {
                        stoppedFuture.complete(message);
                    }

                    if (message.hasFailure()) {
                        observer.onFailure(message.getFailure().getStatus());
                    }

                    if (isWorking.get()) {
                        if (message.hasSessionStarted()) {
                            sessionId.set(message.getSessionStarted().getSessionId());
                        }

                        if (message.hasPing()) {
                            coordinationStream.sendNext(
                                    SessionRequest.newBuilder().setPong(
                                            SessionRequest.PingPong.newBuilder()
                                                    .setOpaque(message.getPing().getOpaque())
                                                    .build()
                                    ).build()
                            );
                        } else {
                            observer.onNext(message);
                        }
                    }
                }
        );
    }

    /**
     * First message used to start/restore a session
     */
    public void sendStartSession(SessionRequest.SessionStart sessionStart) {
        send(
                SessionRequest.newBuilder()
                        .setSessionStart(sessionStart)
                        .build()
        );
    }

    /**
     * Used for checking liveness of the connection
     */
    public void sendPingPong(SessionRequest.PingPong pingPong) {
        send(
                SessionRequest.newBuilder()
                        .setPing(pingPong)
                        .build()
        );
    }

    /**
     * Used to acquire a semaphore
     * <p>
     * WARNING: a single session cannot acquire the same semaphore multiple times
     * <p>
     * Later requests override previous operations with the same semaphore,
     * e.g. to reduce acquired count, change timeout or attached data.
     */
    public void sendAcquireSemaphore(SessionRequest.AcquireSemaphore acquireSemaphore) {
        send(
                SessionRequest.newBuilder()
                        .setAcquireSemaphore(acquireSemaphore)
                        .build()
        );
    }

    /**
     * Used to release a semaphore
     * <p>
     * WARNING: a single session cannot release the same semaphore multiple times
     * <p>
     * The release operation will either remove current session from waiters
     * queue or release an already owned semaphore.
     */
    public void sendReleaseSemaphore(SessionRequest.ReleaseSemaphore releaseSemaphore) {
        send(
                SessionRequest.newBuilder()
                        .setReleaseSemaphore(releaseSemaphore)
                        .build()
        );
    }

    /**
     * Used to describe semaphores and watch them for changes
     * <p>
     * WARNING: a describe operation will cancel previous watches on the same semaphore
     */
    public void sendDescribeSemaphore(SessionRequest.DescribeSemaphore describeSemaphore) {
        send(
                SessionRequest.newBuilder()
                        .setDescribeSemaphore(describeSemaphore)
                        .build()
        );
    }

    /**
     * Used to create a new semaphore
     */
    public void sendCreateSemaphore(SessionRequest.CreateSemaphore createSemaphore) {
        send(
                SessionRequest.newBuilder()
                        .setCreateSemaphore(createSemaphore)
                        .build()
        );
    }

    /**
     * Used to change semaphore data
     */
    public void sendUpdateSemaphore(SessionRequest.UpdateSemaphore updateSemaphore) {
        send(
                SessionRequest.newBuilder()
                        .setUpdateSemaphore(updateSemaphore)
                        .build()
        );
    }

    /**
     * Used to delete an existing semaphore
     */
    public void sendDeleteSemaphore(SessionRequest.DeleteSemaphore deleteSemaphore) {
        send(
                SessionRequest.newBuilder()
                        .setDeleteSemaphore(deleteSemaphore)
                        .build()
        );
    }

    private void send(SessionRequest sessionRequest) {
        logger.trace("Send message: {}", sessionRequest);

        coordinationStream.sendNext(sessionRequest);
    }

    @PreDestroy
    public void stop() {
        if (isWorking.compareAndSet(true, false)) {
            coordinationStream.sendNext(
                    SessionRequest.newBuilder()
                            .setSessionStop(
                                    SessionRequest.SessionStop.newBuilder()
                                            .build()
                            ).build()
            );

            try {
                stoppedFuture.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.error("Failed stopping awaiting", e);
            }

            coordinationStream.close();
        }
    }
}
