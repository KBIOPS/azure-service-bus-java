// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package com.microsoft.azure.servicebus;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.primitives.CoreMessageSender;
import com.microsoft.azure.servicebus.primitives.ExceptionUtil;
import com.microsoft.azure.servicebus.primitives.MessagingEntityType;
import com.microsoft.azure.servicebus.primitives.MessagingFactory;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import com.microsoft.azure.servicebus.primitives.StringUtil;

final class MessageSender extends InitializableEntity implements IMessageSender {
    private boolean ownsMessagingFactory;
    private String entityPath = null;
    private MessagingEntityType entityType = null;
    private MessagingFactory messagingFactory = null;
    private CoreMessageSender internalSender = null;
    private boolean isInitialized = false;
    private URI namespaceEndpointURI;
    private ClientSettings clientSettings;    

    private MessageSender() {
        super(StringUtil.getShortRandomString());
    }

    MessageSender(URI namespaceEndpointURI, String entityPath, MessagingEntityType entityType, ClientSettings clientSettings) {
        this();

        this.namespaceEndpointURI = namespaceEndpointURI;
        this.entityPath = entityPath;
        this.clientSettings = clientSettings;
        this.ownsMessagingFactory = true;
        this.entityType = entityType;
    }

    MessageSender(MessagingFactory messagingFactory, String entityPath, MessagingEntityType entityType) {
        this(messagingFactory, entityPath, entityType, false);
    }

    private MessageSender(MessagingFactory messagingFactory, String entityPath, MessagingEntityType entityType, boolean ownsMessagingFactory) {
        this();

        this.messagingFactory = messagingFactory;
        this.entityPath = entityPath;
        this.ownsMessagingFactory = ownsMessagingFactory;
        this.entityType = entityType;
    }

    @Override
    synchronized CompletableFuture<Void> initializeAsync() {
        if (this.isInitialized) {
            return CompletableFuture.completedFuture(null);
        } else {
            CompletableFuture<Void> factoryFuture;
            if (this.messagingFactory == null) {
                factoryFuture = MessagingFactory.createFromNamespaceEndpointURIAsyc(this.namespaceEndpointURI, this.clientSettings).thenAcceptAsync((f) ->
                {
                    this.messagingFactory = f;
                });
            } else {
                factoryFuture = CompletableFuture.completedFuture(null);
            }

            return factoryFuture.thenComposeAsync((v) ->
            {
                CompletableFuture<CoreMessageSender> senderFuture = CoreMessageSender.create(this.messagingFactory, StringUtil.getShortRandomString(), this.entityPath, this.entityType);
                CompletableFuture<Void> postSenderCreationFuture = new CompletableFuture<Void>();
                senderFuture.handleAsync((s, coreSenderCreationEx) -> {
                    if (coreSenderCreationEx == null) {
                        this.internalSender = s;
                        this.isInitialized = true;
                        postSenderCreationFuture.complete(null);
                    } else {
                        Throwable cause = ExceptionUtil.extractAsyncCompletionCause(coreSenderCreationEx);
                        if (this.ownsMessagingFactory) {
                            // Close factory
                            this.messagingFactory.closeAsync();
                        }
                        postSenderCreationFuture.completeExceptionally(cause);
                    }
                    return null;
                }, MessagingFactory.INTERNAL_THREAD_POOL);
                return postSenderCreationFuture;
            }, MessagingFactory.INTERNAL_THREAD_POOL);
        }
    }

    final CoreMessageSender getInternalSender() {
        return this.internalSender;
    }

    @Override
    public void send(IMessage message) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(this.sendAsync(message));
    }

    @Override
    public void sendBatch(Collection<? extends IMessage> message) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(this.sendBatchAsync(message));
    }

    @Override
    public CompletableFuture<Void> sendAsync(IMessage message) {
        org.apache.qpid.proton.message.Message amqpMessage = MessageConverter.convertBrokeredMessageToAmqpMessage((Message) message);
        return this.internalSender.sendAsync(amqpMessage);
    }

    @Override
    public CompletableFuture<Void> sendBatchAsync(Collection<? extends IMessage> messages) {
        ArrayList<org.apache.qpid.proton.message.Message> convertedMessages = new ArrayList<org.apache.qpid.proton.message.Message>();
        for (IMessage message : messages) {
            convertedMessages.add(MessageConverter.convertBrokeredMessageToAmqpMessage((Message) message));
        }

        return this.internalSender.sendAsync(convertedMessages);
    }

    @Override
    protected CompletableFuture<Void> onClose() {
        if (this.isInitialized) {
            return this.internalSender.closeAsync().thenComposeAsync((v) ->
            {
                if (MessageSender.this.ownsMessagingFactory) {

                    return MessageSender.this.messagingFactory.closeAsync();
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            }, MessagingFactory.INTERNAL_THREAD_POOL);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public String getEntityPath() {
        return this.entityPath;
    }

    @Override
    public CompletableFuture<Long> scheduleMessageAsync(IMessage message, Instant scheduledEnqueueTimeUtc) {
        message.setScheduledEnqueueTimeUtc(scheduledEnqueueTimeUtc);
        org.apache.qpid.proton.message.Message amqpMessage = MessageConverter.convertBrokeredMessageToAmqpMessage((Message) message);
        return this.internalSender.scheduleMessageAsync(new org.apache.qpid.proton.message.Message[]{amqpMessage}, this.messagingFactory.getClientSetttings().getOperationTimeout()).thenApply(sequenceNumbers -> sequenceNumbers[0]);
    }

    @Override
    public CompletableFuture<Void> cancelScheduledMessageAsync(long sequenceNumber) {
        return this.internalSender.cancelScheduledMessageAsync(new Long[]{sequenceNumber}, this.messagingFactory.getClientSetttings().getOperationTimeout());
    }

    @Override
    public long scheduleMessage(IMessage message, Instant scheduledEnqueueTimeUtc) throws InterruptedException, ServiceBusException {
        return Utils.completeFuture(this.scheduleMessageAsync(message, scheduledEnqueueTimeUtc));
    }

    @Override
    public void cancelScheduledMessage(long sequenceNumber) throws InterruptedException, ServiceBusException {
        Utils.completeFuture(this.cancelScheduledMessageAsync(sequenceNumber));
    }

    MessagingFactory getMessagingFactory() {
        return this.messagingFactory;
    }
}
