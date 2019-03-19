// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.servicebus;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledFuture;

import com.microsoft.azure.servicebus.primitives.ExceptionUtil;
import com.microsoft.azure.servicebus.primitives.MessageLockLostException;
import com.microsoft.azure.servicebus.primitives.MessagingEntityType;
import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.OperationCancelledException;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.SessionLockLostException;
import com.microsoft.azure.servicebus.primitives.StringUtil;
import com.microsoft.azure.servicebus.primitives.TimeoutException;
import com.microsoft.azure.servicebus.primitives.Timer;
import com.microsoft.azure.servicebus.primitives.TimerType;

class MessageAndSessionPump extends InitializableEntity implements IMessageAndSessionPump {
    private static final Duration MINIMUM_MESSAGE_LOCK_VALIDITY = Duration.ofSeconds(4);
    private static final Duration MAXIMUM_RENEW_LOCK_BUFFER = Duration.ofSeconds(10);
    private static final Duration SLEEP_DURATION_ON_ACCEPT_SESSION_EXCEPTION = Duration.ofMinutes(1);
    private static final int UNSET_PREFETCH_COUNT = -1; // Means prefetch count not set
    private static final CompletableFuture<Void> COMPLETED_FUTURE = CompletableFuture.completedFuture(null);

    private final ConcurrentHashMap<String, IMessageSession> openSessions;
    private final MessagingFactory factory;
    private final String entityPath;
    private final ReceiveMode receiveMode;
    private final MessagingEntityType entityType;
    private IMessageReceiver innerReceiver;

    private boolean handlerRegistered = false;
    private IMessageHandler messageHandler;
    private ISessionHandler sessionHandler;
    private MessageHandlerOptions messageHandlerOptions;
    private SessionHandlerOptions sessionHandlerOptions;
    private int prefetchCount;
    private ExecutorService customCodeExecutor;

    public MessageAndSessionPump(MessagingFactory factory, String entityPath, MessagingEntityType entityType, ReceiveMode receiveMode) {
        super(StringUtil.getShortRandomString());
        this.factory = factory;
        this.entityPath = entityPath;
        this.entityType = entityType;
        this.receiveMode = receiveMode;
        this.openSessions = new ConcurrentHashMap<>();
        this.prefetchCount = UNSET_PREFETCH_COUNT;
    }

    @Deprecated
    @Override
    public void registerMessageHandler(IMessageHandler handler) throws InterruptedException, ServiceBusException {
        this.registerMessageHandler(handler, new MessageHandlerOptions());
    }    
    
    @Override
    public void registerMessageHandler(IMessageHandler handler, ExecutorService executorService) throws InterruptedException, ServiceBusException {
        this.registerMessageHandler(handler, new MessageHandlerOptions(), executorService);
    }

    @Deprecated
    @Override
    public void registerMessageHandler(IMessageHandler handler, MessageHandlerOptions handlerOptions) throws InterruptedException, ServiceBusException {
    	this.registerMessageHandler(handler, handlerOptions, ForkJoinPool.commonPool());
    }
    
    @Override
    public void registerMessageHandler(IMessageHandler handler, MessageHandlerOptions handlerOptions, ExecutorService executorService) throws InterruptedException, ServiceBusException {
    	assertNonNulls(handler, handlerOptions, executorService);
        this.setHandlerRegistered();
        this.messageHandler = handler;
        this.messageHandlerOptions = handlerOptions;
        this.customCodeExecutor = executorService;

        this.innerReceiver = ClientFactory.createMessageReceiverFromEntityPath(this.factory, this.entityPath, this.entityType, this.receiveMode);
        if(this.prefetchCount != UNSET_PREFETCH_COUNT)
        {
            this.innerReceiver.setPrefetchCount(this.prefetchCount);
        }
        for (int i = 0; i < handlerOptions.getMaxConcurrentCalls(); i++) {
            this.receiveAndPumpMessage();
        }
    }

    @Deprecated
    @Override
    public void registerSessionHandler(ISessionHandler handler) throws InterruptedException, ServiceBusException {
        this.registerSessionHandler(handler, new SessionHandlerOptions());
    }    
    
    @Override
    public void registerSessionHandler(ISessionHandler handler, ExecutorService executorService) throws InterruptedException, ServiceBusException {
        this.registerSessionHandler(handler, new SessionHandlerOptions(), executorService);
    }

    @Deprecated
    @Override
    public void registerSessionHandler(ISessionHandler handler, SessionHandlerOptions handlerOptions) throws InterruptedException, ServiceBusException {
    	this.registerSessionHandler(handler, handlerOptions, ForkJoinPool.commonPool());
    }
    
    @Override
    public void registerSessionHandler(ISessionHandler handler, SessionHandlerOptions handlerOptions, ExecutorService executorService) throws InterruptedException, ServiceBusException {
    	assertNonNulls(handler, handlerOptions, executorService);
        this.setHandlerRegistered();
        this.sessionHandler = handler;
        this.sessionHandlerOptions = handlerOptions;
        this.customCodeExecutor = executorService;

        for (int i = 0; i < handlerOptions.getMaxConcurrentSessions(); i++) {
            this.acceptSessionAndPumpMessages();
        }
    }
    
    private static void assertNonNulls(Object handler, Object options, ExecutorService executorService)
    {
    	if(handler == null || options == null || executorService == null)
    	{
    		throw new IllegalArgumentException("None of the arguments can be null.");
    	}
    }

    private synchronized void setHandlerRegistered() {
        this.throwIfClosed(null);

        // Only one handler is allowed to be registered per client
        if (this.handlerRegistered) {
            throw new UnsupportedOperationException("MessageHandler or SessionHandler already registered.");
        }

        this.handlerRegistered = true;
    }

    private void receiveAndPumpMessage() {
        if (!this.getIsClosingOrClosed()) {
            CompletableFuture<IMessage> receiveMessageFuture = this.innerReceiver.receiveAsync(this.messageHandlerOptions.getMessageWaitDuration());
            receiveMessageFuture.handleAsync((message, receiveEx) -> {
                if (receiveEx != null) {
                    receiveEx = ExceptionUtil.extractAsyncCompletionCause(receiveEx);
                    this.notifyExceptionToMessageHandler(receiveEx, ExceptionPhase.RECEIVE);
                    this.receiveAndPumpMessage();
                } else {
                    if (message == null) {
                        this.receiveAndPumpMessage();
                    } else {
                        // Start renew lock loop
                        final MessgeRenewLockLoop renewLockLoop;
                        if (this.innerReceiver.getReceiveMode() == ReceiveMode.PEEKLOCK) {
                            Instant stopRenewMessageLockAt = Instant.now().plus(this.messageHandlerOptions.getMaxAutoRenewDuration());
                            renewLockLoop = new MessgeRenewLockLoop(this.innerReceiver, this, message, stopRenewMessageLockAt);
                            renewLockLoop.startLoop();
                        } else {
                            renewLockLoop = null;
                        }

                        CompletableFuture<Void> onMessageFuture;
                        try {
                            onMessageFuture = COMPLETED_FUTURE.thenComposeAsync((v) -> this.messageHandler.onMessageAsync(message), this.customCodeExecutor);
                        } catch (Exception onMessageSyncEx) {
                            onMessageFuture = new CompletableFuture<Void>();
                            onMessageFuture.completeExceptionally(onMessageSyncEx);
                        }
                        
                        // Some clients are returning null from the call
                        if(onMessageFuture == null)
                        {
                            onMessageFuture = COMPLETED_FUTURE;
                        }

                        onMessageFuture.handleAsync((v, onMessageEx) -> {
                            if (onMessageEx != null) {
                                onMessageEx = ExceptionUtil.extractAsyncCompletionCause(onMessageEx);
                                this.notifyExceptionToMessageHandler(onMessageEx, ExceptionPhase.USERCALLBACK);
                            }
                            if (this.innerReceiver.getReceiveMode() == ReceiveMode.PEEKLOCK) {
                                if (renewLockLoop != null) {
                                    renewLockLoop.cancelLoop();
                                }
                                CompletableFuture<Void> updateDispositionFuture;
                                ExceptionPhase dispositionPhase;
                                if (onMessageEx == null) {
                                    // Complete message
                                    dispositionPhase = ExceptionPhase.COMPLETE;
                                    if (this.messageHandlerOptions.isAutoComplete()) {
                                        updateDispositionFuture = this.innerReceiver.completeAsync(message.getLockToken());
                                    } else {
                                        updateDispositionFuture = CompletableFuture.completedFuture(null);
                                    }
                                } else {
                                    // Abandon message
                                    dispositionPhase = ExceptionPhase.ABANDON;
                                    if(this.messageHandlerOptions.isAutoComplete())
                                    {
                                        updateDispositionFuture = this.innerReceiver.abandonAsync(message.getLockToken());
                                    }
                                    else
                                    {
                                        updateDispositionFuture = CompletableFuture.completedFuture(null);
                                    }
                                }

                                updateDispositionFuture.handleAsync((u, updateDispositionEx) -> {
                                    if (updateDispositionEx != null) {
                                        updateDispositionEx = ExceptionUtil.extractAsyncCompletionCause(updateDispositionEx);
                                        this.notifyExceptionToMessageHandler(updateDispositionEx, dispositionPhase);
                                    }
                                    this.receiveAndPumpMessage();
                                    return null;
                                }, MessagingFactory.INTERNAL_THREAD_POOL);
                            } else {
                                this.receiveAndPumpMessage();
                            }

                            return null;
                        }, MessagingFactory.INTERNAL_THREAD_POOL);
                    }

                }

                return null;
            }, MessagingFactory.INTERNAL_THREAD_POOL);
        }
    }

    private void acceptSessionAndPumpMessages() {
        if (!this.getIsClosingOrClosed()) {
            CompletableFuture<IMessageSession> acceptSessionFuture = ClientFactory.acceptSessionFromEntityPathAsync(this.factory, this.entityPath, this.entityType, null, this.receiveMode);
            acceptSessionFuture.handleAsync((session, acceptSessionEx) -> {
                if (acceptSessionEx != null) {
                    acceptSessionEx = ExceptionUtil.extractAsyncCompletionCause(acceptSessionEx);

                    // Timeout exception means no session available.. it is expected so no need to notify client
                    if (!(acceptSessionEx instanceof TimeoutException)) {
                        this.notifyExceptionToSessionHandler(acceptSessionEx, ExceptionPhase.ACCEPTSESSION);
                    }

                    if (!(acceptSessionEx instanceof OperationCancelledException)) {
                        // don't retry if OperationCancelled by service.. may be entity itself is deleted
                        // In case of any other exception, sleep and retry
                        Timer.schedule(() -> {
                            MessageAndSessionPump.this.acceptSessionAndPumpMessages();
                        }, SLEEP_DURATION_ON_ACCEPT_SESSION_EXCEPTION, TimerType.OneTimeRun);
                    }
                } else {
                    // Received a session.. Now pump messages..
                    if(this.prefetchCount != UNSET_PREFETCH_COUNT)
                    {
                        try {
                            session.setPrefetchCount(this.prefetchCount);
                        } catch (ServiceBusException e) {
                            // Should not happen as long as reactor is running. So ignoring
                        }
                    }
                    this.openSessions.put(session.getSessionId(), session);
                    SessionRenewLockLoop sessionRenewLockLoop = new SessionRenewLockLoop(session, this);
                    sessionRenewLockLoop.startLoop();
                    SessionTracker sessionTracker = new SessionTracker(this, session, sessionRenewLockLoop);
                    for (int i = 0; i < this.sessionHandlerOptions.getMaxConcurrentCallsPerSession(); i++) {
                        this.receiveFromSessionAndPumpMessage(sessionTracker);
                    }
                }

                return null;
            }, MessagingFactory.INTERNAL_THREAD_POOL);
        }
    }

    private void receiveFromSessionAndPumpMessage(SessionTracker sessionTracker) {
        if (!this.getIsClosingOrClosed()) {
            IMessageSession session = sessionTracker.getSession();
            CompletableFuture<IMessage> receiverFuture = session.receiveAsync(this.sessionHandlerOptions.getMessageWaitDuration());
            receiverFuture.handleAsync((message, receiveEx) -> {
                if (receiveEx != null) {
                    receiveEx = ExceptionUtil.extractAsyncCompletionCause(receiveEx);
                    this.notifyExceptionToSessionHandler(receiveEx, ExceptionPhase.RECEIVE);
                    sessionTracker.shouldRetryOnNoMessageOrException().thenAcceptAsync((shouldRetry) -> {
                        if (shouldRetry) {
                            this.receiveFromSessionAndPumpMessage(sessionTracker);
                        }
                    }, MessagingFactory.INTERNAL_THREAD_POOL);
                } else {
                    if (message == null) {
                        sessionTracker.shouldRetryOnNoMessageOrException().thenAcceptAsync((shouldRetry) -> {
                            if (shouldRetry) {
                                this.receiveFromSessionAndPumpMessage(sessionTracker);
                            }
                        }, MessagingFactory.INTERNAL_THREAD_POOL);
                    } else {
                        sessionTracker.notifyMessageReceived();
                        // There is no need to renew message locks as session messages are locked for a day
                        ScheduledFuture<?> renewCancelTimer = Timer.schedule(() -> {
                                    sessionTracker.sessionRenewLockLoop.cancelLoop();
                                },
                                this.sessionHandlerOptions.getMaxAutoRenewDuration(),
                                TimerType.OneTimeRun);
                        CompletableFuture<Void> onMessageFuture;
                        try {
                            onMessageFuture = COMPLETED_FUTURE.thenComposeAsync((v) -> this.sessionHandler.onMessageAsync(session, message), this.customCodeExecutor);
                        } catch (Exception onMessageSyncEx) {
                            onMessageFuture = new CompletableFuture<Void>();
                            onMessageFuture.completeExceptionally(onMessageSyncEx);
                        }

                        // Some clients are returning null from the call
                        if(onMessageFuture == null)
                        {
                            onMessageFuture = COMPLETED_FUTURE;
                        }
                        
                        onMessageFuture.handleAsync((v, onMessageEx) -> {
                            renewCancelTimer.cancel(true);
                            if (onMessageEx != null) {
                                onMessageEx = ExceptionUtil.extractAsyncCompletionCause(onMessageEx);
                                this.notifyExceptionToSessionHandler(onMessageEx, ExceptionPhase.USERCALLBACK);
                            }
                            if (this.receiveMode == ReceiveMode.PEEKLOCK) {
                                CompletableFuture<Void> updateDispositionFuture;
                                ExceptionPhase dispositionPhase;
                                if (onMessageEx == null) {
                                    // Complete message
                                    dispositionPhase = ExceptionPhase.COMPLETE;
                                    if (this.sessionHandlerOptions.isAutoComplete()) {
                                        updateDispositionFuture = session.completeAsync(message.getLockToken());
                                    } else {
                                        updateDispositionFuture = CompletableFuture.completedFuture(null);
                                    }
                                } else {
                                    // Abandon message
                                    dispositionPhase = ExceptionPhase.ABANDON;
                                    if (this.sessionHandlerOptions.isAutoComplete())
                                    {
                                        updateDispositionFuture = session.abandonAsync(message.getLockToken());
                                    }
                                    else
                                    {
                                        updateDispositionFuture = CompletableFuture.completedFuture(null);
                                    }
                                }

                                updateDispositionFuture.handleAsync((u, updateDispositionEx) -> {
                                    if (updateDispositionEx != null) {
                                        updateDispositionEx = ExceptionUtil.extractAsyncCompletionCause(updateDispositionEx);
                                        this.notifyExceptionToSessionHandler(updateDispositionEx, dispositionPhase);
                                    }
                                    this.receiveFromSessionAndPumpMessage(sessionTracker);
                                    return null;
                                }, MessagingFactory.INTERNAL_THREAD_POOL);
                            } else {
                                this.receiveFromSessionAndPumpMessage(sessionTracker);
                            }

                            return null;
                        }, MessagingFactory.INTERNAL_THREAD_POOL);
                    }

                }

                return null;
            }, MessagingFactory.INTERNAL_THREAD_POOL);
        }
    }

    @Override
    CompletableFuture<Void> initializeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        CompletableFuture[] closeFutures = new CompletableFuture[this.openSessions.size() + 1];
        int arrayIndex = 0;
        for (IMessageSession session : this.openSessions.values()) {
            closeFutures[arrayIndex++] = session.closeAsync();
        }
        closeFutures[arrayIndex] = this.innerReceiver == null ? CompletableFuture.completedFuture(null) : this.innerReceiver.closeAsync();
        return CompletableFuture.allOf(closeFutures);
    }

    private static class SessionTracker {
        private final int numberReceivingThreads;
        private final IMessageSession session;
        private final MessageAndSessionPump messageAndSessionPump;
        private final SessionRenewLockLoop sessionRenewLockLoop;
        private int waitingRetryThreads;
        private CompletableFuture<Boolean> retryFuture;

        SessionTracker(MessageAndSessionPump messageAndSessionPump, IMessageSession session, SessionRenewLockLoop sessionRenewLockLoop) {
            this.messageAndSessionPump = messageAndSessionPump;
            this.session = session;
            this.sessionRenewLockLoop = sessionRenewLockLoop;
            this.numberReceivingThreads = messageAndSessionPump.sessionHandlerOptions.getMaxConcurrentCallsPerSession();
            this.waitingRetryThreads = 0;
        }

        public IMessageSession getSession() {
            return this.session;
        }

        synchronized void notifyMessageReceived() {
            if (this.retryFuture != null && !this.retryFuture.isDone()) {
                this.waitingRetryThreads = 0;
                this.retryFuture.complete(true);
            }
        }

        synchronized CompletableFuture<Boolean> shouldRetryOnNoMessageOrException() {
            if (this.retryFuture == null || this.retryFuture.isDone()) {
                this.retryFuture = new CompletableFuture<Boolean>();
            }
            this.waitingRetryThreads++;
            if (this.waitingRetryThreads == this.numberReceivingThreads) {
                this.retryFuture.complete(false);

                // close current session and accept another session
                ScheduledFuture<?> renewCancelTimer = Timer.schedule(() -> {
                            SessionTracker.this.sessionRenewLockLoop.cancelLoop();
                        },
                        this.messageAndSessionPump.sessionHandlerOptions.getMaxAutoRenewDuration(),
                        TimerType.OneTimeRun);
                CompletableFuture<Void> onCloseFuture;
                try {
                    onCloseFuture = COMPLETED_FUTURE.thenComposeAsync((v) -> this.messageAndSessionPump.sessionHandler.OnCloseSessionAsync(session), this.messageAndSessionPump.customCodeExecutor);
                } catch (Exception onCloseSyncEx) {
                    onCloseFuture = new CompletableFuture<Void>();
                    onCloseFuture.completeExceptionally(onCloseSyncEx);
                }
                
                // Some clients are returning null from the call
                if(onCloseFuture == null)
                {
                    onCloseFuture = COMPLETED_FUTURE;
                }

                onCloseFuture.handleAsync((v, onCloseEx) -> {
                    renewCancelTimer.cancel(true);
                    if (onCloseEx != null) {
                        onCloseEx = ExceptionUtil.extractAsyncCompletionCause(onCloseEx);
                        this.messageAndSessionPump.notifyExceptionToSessionHandler(onCloseEx, ExceptionPhase.USERCALLBACK);
                    }

                    this.sessionRenewLockLoop.cancelLoop();
                    this.session.closeAsync().handleAsync((z, closeEx) ->
                    {
                        if (closeEx != null) {
                            closeEx = ExceptionUtil.extractAsyncCompletionCause(closeEx);
                            this.messageAndSessionPump.notifyExceptionToSessionHandler(closeEx, ExceptionPhase.SESSIONCLOSE);
                        } else {
                        }

                        this.messageAndSessionPump.openSessions.remove(this.session.getSessionId());
                        this.messageAndSessionPump.acceptSessionAndPumpMessages();
                        return null;
                    }, MessagingFactory.INTERNAL_THREAD_POOL);
                    return null;
                }, MessagingFactory.INTERNAL_THREAD_POOL);

            }

            return this.retryFuture;
        }
    }

    private abstract static class RenewLockLoop {
        private boolean cancelled = false;

        protected RenewLockLoop() {
        }

        protected abstract void loop();

        protected abstract ScheduledFuture<?> getTimerFuture();

        protected boolean isCancelled() {
            return this.cancelled;
        }

        public void startLoop() {
            this.loop();
        }

        public void cancelLoop() {
            if (!this.cancelled) {
                this.cancelled = true;
                ScheduledFuture<?> timerFuture = this.getTimerFuture();
                if (timerFuture != null && !timerFuture.isDone()) {
                    timerFuture.cancel(true);
                }
            }
        }

        protected static Duration getNextRenewInterval(Instant lockedUntilUtc, String identifier) {
            Duration remainingTime = Duration.between(Instant.now(), lockedUntilUtc);
            if (remainingTime.isNegative()) {
                // Lock likely expired. May be there is clock skew. Assume some minimum time
                remainingTime = MessageAndSessionPump.MINIMUM_MESSAGE_LOCK_VALIDITY;
            }

            Duration buffer = remainingTime.dividedBy(2).compareTo(MAXIMUM_RENEW_LOCK_BUFFER) > 0 ? MAXIMUM_RENEW_LOCK_BUFFER : remainingTime.dividedBy(2);
            return remainingTime.minus(buffer);
        }
    }

    private static class MessgeRenewLockLoop extends RenewLockLoop {
        private IMessageReceiver innerReceiver;
        private MessageAndSessionPump messageAndSessionPump;
        private IMessage message;
        private Instant stopRenewalAt;
        private String messageIdentifier;
        ScheduledFuture<?> timerFuture;

        MessgeRenewLockLoop(IMessageReceiver innerReceiver, MessageAndSessionPump messageAndSessionPump, IMessage message, Instant stopRenewalAt) {
            super();
            this.innerReceiver = innerReceiver;
            this.messageAndSessionPump = messageAndSessionPump;
            this.message = message;
            this.stopRenewalAt = stopRenewalAt;
            this.messageIdentifier = String.format("message with locktoken : %s, sequence number : %s", this.message.getLockToken(), this.message.getSequenceNumber());
        }

        @Override
        protected ScheduledFuture<?> getTimerFuture() {
            return this.timerFuture;
        }

        @Override
        protected void loop() {
            if (!this.isCancelled()) {
                Duration renewInterval = this.getNextRenewInterval();
                if (renewInterval != null && !renewInterval.isNegative()) {
                    this.timerFuture = Timer.schedule(() -> {
                        this.innerReceiver.renewMessageLockAsync(message).handleAsync((v, renewLockEx) ->
                        {
                            if (renewLockEx != null) {
                                renewLockEx = ExceptionUtil.extractAsyncCompletionCause(renewLockEx);
                                this.messageAndSessionPump.notifyExceptionToMessageHandler(renewLockEx, ExceptionPhase.RENEWMESSAGELOCK);
                                if (!(renewLockEx instanceof MessageLockLostException || renewLockEx instanceof OperationCancelledException)) {
                                    this.loop();
                                }
                            } else {
                                this.loop();
                            }

                            return null;
                        }, MessagingFactory.INTERNAL_THREAD_POOL);
                    }, renewInterval, TimerType.OneTimeRun);
                }
            }
        }

        private Duration getNextRenewInterval() {
            if (this.message.getLockedUntilUtc().isBefore(stopRenewalAt)) {
                return RenewLockLoop.getNextRenewInterval(this.message.getLockedUntilUtc(), this.messageIdentifier);
            } else {
                return null;
            }
        }
    }

    private static class SessionRenewLockLoop extends RenewLockLoop {
        private IMessageSession session;
        private MessageAndSessionPump messageAndSessionPump;
        private String sessionIdentifier;
        ScheduledFuture<?> timerFuture;

        SessionRenewLockLoop(IMessageSession session, MessageAndSessionPump messageAndSessionPump) {
            super();
            this.session = session;
            this.messageAndSessionPump = messageAndSessionPump;
            this.sessionIdentifier = String.format("session with id:%s", this.session.getSessionId());
        }

        @Override
        protected ScheduledFuture<?> getTimerFuture() {
            return this.timerFuture;
        }

        @Override
        protected void loop() {
            if (!this.isCancelled()) {
                Duration renewInterval = RenewLockLoop.getNextRenewInterval(this.session.getLockedUntilUtc(), this.sessionIdentifier);
                if (renewInterval != null && !renewInterval.isNegative()) {
                    this.timerFuture = Timer.schedule(() -> {
                        this.session.renewSessionLockAsync().handleAsync((v, renewLockEx) ->
                        {
                            if (renewLockEx != null) {
                                renewLockEx = ExceptionUtil.extractAsyncCompletionCause(renewLockEx);
                                this.messageAndSessionPump.notifyExceptionToSessionHandler(renewLockEx, ExceptionPhase.RENEWSESSIONLOCK);
                                if (!(renewLockEx instanceof SessionLockLostException || renewLockEx instanceof OperationCancelledException)) {
                                    this.loop();
                                }
                            } else {
                                this.loop();
                            }

                            return null;
                        }, MessagingFactory.INTERNAL_THREAD_POOL);
                    }, renewInterval, TimerType.OneTimeRun);
                }
            }
        }
    }

    @Override
    public void abandon(UUID lockToken) throws InterruptedException, ServiceBusException {
        this.checkInnerReceiveCreated();
        this.innerReceiver.abandon(lockToken);
    }

    @Override
    public void abandon(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        this.checkInnerReceiveCreated();
        this.innerReceiver.abandon(lockToken, propertiesToModify);
    }

    @Override
    public CompletableFuture<Void> abandonAsync(UUID lockToken) {
        this.checkInnerReceiveCreated();
        return this.innerReceiver.abandonAsync(lockToken);
    }

    @Override
    public CompletableFuture<Void> abandonAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
        this.checkInnerReceiveCreated();
        return this.innerReceiver.abandonAsync(lockToken, propertiesToModify);
    }

    @Override
    public void complete(UUID lockToken) throws InterruptedException, ServiceBusException {
        this.checkInnerReceiveCreated();
        this.innerReceiver.complete(lockToken);
    }

    @Override
    public CompletableFuture<Void> completeAsync(UUID lockToken) {
        this.checkInnerReceiveCreated();
        return this.innerReceiver.completeAsync(lockToken);
    }

    //	@Override
    void defer(UUID lockToken) throws InterruptedException, ServiceBusException {
        this.checkInnerReceiveCreated();
        this.innerReceiver.defer(lockToken);
    }

    //	@Override
    void defer(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        this.checkInnerReceiveCreated();
        this.innerReceiver.defer(lockToken, propertiesToModify);
    }

//    @Override
//    public CompletableFuture<Void> deferAsync(UUID lockToken) {
//        this.checkInnerReceiveCreated();
//        return this.innerReceiver.abandonAsync(lockToken);
//    }
//
//    @Override
//    public CompletableFuture<Void> deferAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
//        this.checkInnerReceiveCreated();
//        return this.innerReceiver.abandonAsync(lockToken, propertiesToModify);
//    }

    @Override
    public void deadLetter(UUID lockToken) throws InterruptedException, ServiceBusException {
        this.innerReceiver.deadLetter(lockToken);
    }

    @Override
    public void deadLetter(UUID lockToken, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        this.innerReceiver.deadLetter(lockToken, propertiesToModify);
    }

    @Override
    public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription) throws InterruptedException, ServiceBusException {
        this.checkInnerReceiveCreated();
        this.innerReceiver.deadLetter(lockToken, deadLetterReason, deadLetterErrorDescription);
    }

    @Override
    public void deadLetter(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription, Map<String, Object> propertiesToModify) throws InterruptedException, ServiceBusException {
        this.checkInnerReceiveCreated();
        this.innerReceiver.deadLetter(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify);
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken) {
        this.checkInnerReceiveCreated();
        return this.innerReceiver.deadLetterAsync(lockToken);
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, Map<String, Object> propertiesToModify) {
        this.checkInnerReceiveCreated();
        return this.innerReceiver.deadLetterAsync(lockToken, propertiesToModify);
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription) {
        this.checkInnerReceiveCreated();
        return this.innerReceiver.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription);
    }

    @Override
    public CompletableFuture<Void> deadLetterAsync(UUID lockToken, String deadLetterReason, String deadLetterErrorDescription, Map<String, Object> propertiesToModify) {
        this.checkInnerReceiveCreated();
        return this.innerReceiver.deadLetterAsync(lockToken, deadLetterReason, deadLetterErrorDescription, propertiesToModify);
    }

    private void checkInnerReceiveCreated() {
        if (this.innerReceiver == null) {
            throw new UnsupportedOperationException("Receiver not created. Registering a MessageHandler creates a receiver.");
        }
    }

    // Don't notify handler if the pump is already closed
    private void notifyExceptionToSessionHandler(Throwable ex, ExceptionPhase phase) {
        if (!(ex instanceof IllegalStateException && this.getIsClosingOrClosed())) {
            this.customCodeExecutor.execute(() -> {this.sessionHandler.notifyException(ex, phase);});
        }
    }

    private void notifyExceptionToMessageHandler(Throwable ex, ExceptionPhase phase) {
        if (!(ex instanceof IllegalStateException && this.getIsClosingOrClosed())) {
        	this.customCodeExecutor.execute(() -> {this.messageHandler.notifyException(ex, phase);});            
        }
    }

    @Override
    public int getPrefetchCount() {
        return this.prefetchCount;
    }

    @Override
    public void setPrefetchCount(int prefetchCount) throws ServiceBusException {
        if(prefetchCount < 0)
        {
            throw new IllegalArgumentException("Prefetch count cannot be negative.");
        }
        
        this.prefetchCount = prefetchCount;
        if(this.innerReceiver != null)
        {
            this.innerReceiver.setPrefetchCount(prefetchCount);
        }

        // For accepted session receivers also
        IMessageSession[] currentAcceptedSessions = this.openSessions.values().toArray(new IMessageSession[0]);
        for(IMessageSession session : currentAcceptedSessions)
        {
            try
            {
                session.setPrefetchCount(prefetchCount);
            }
            catch(IllegalStateException ise)
            {
                // Session might have been closed.. Ignore the exception as this is a best effort setter on already accepted sessions
            }
        }
    }
}
