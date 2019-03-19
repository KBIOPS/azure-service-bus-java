package com.microsoft.azure.servicebus.primitives;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.amqp.transport.ReceiverSettleMode;
import org.apache.qpid.proton.amqp.transport.SenderSettleMode;
import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;
import org.apache.qpid.proton.engine.impl.DeliveryImpl;
import org.apache.qpid.proton.message.Message;

import com.microsoft.azure.servicebus.amqp.AmqpConstants;
import com.microsoft.azure.servicebus.amqp.DispatchHandler;
import com.microsoft.azure.servicebus.amqp.IAmqpReceiver;
import com.microsoft.azure.servicebus.amqp.IAmqpSender;
import com.microsoft.azure.servicebus.amqp.ReceiveLinkHandler;
import com.microsoft.azure.servicebus.amqp.SendLinkHandler;
import com.microsoft.azure.servicebus.amqp.SessionHandler;

class RequestResponseLink extends ClientEntity{
	
	private final Object recreateLinksLock;
	private final MessagingFactory underlyingFactory;
	private final String linkPath;
	private final String sasTokenAudienceURI;
	private final CompletableFuture<RequestResponseLink> createFuture;
	private final ConcurrentHashMap<String, RequestResponseWorkItem> pendingRequests;
    private final AtomicInteger requestCounter;
    private final String replyTo;
    
    private ScheduledFuture<?> sasTokenRenewTimerFuture;
	private InternalReceiver amqpReceiver;
	private InternalSender amqpSender;
	private boolean isRecreateLinksInProgress;
	private MessagingEntityType entityType;
	private boolean isInnerLinksCloseHandled;
	private int internalLinkGeneration;
	
	public static CompletableFuture<RequestResponseLink> createAsync(MessagingFactory messagingFactory, String linkName, String linkPath, String sasTokenAudienceURI, MessagingEntityType entityType)
	{
		final RequestResponseLink requestReponseLink = new RequestResponseLink(messagingFactory, linkName, linkPath, sasTokenAudienceURI, entityType);
		
		Timer.schedule(
				new Runnable()
				{
					public void run()
					{
						if (!requestReponseLink.createFuture.isDone())
						{
						    requestReponseLink.amqpSender.closeInternals(false);
						    requestReponseLink.amqpReceiver.closeInternals(false);
						    requestReponseLink.cancelSASTokenRenewTimer();
						    
							Exception operationTimedout = new TimeoutException(
									String.format(Locale.US, "Open operation on RequestResponseLink(%s) on Entity(%s) timed out at %s.", requestReponseLink.getClientId(), requestReponseLink.linkPath, ZonedDateTime.now().toString()));
							AsyncUtil.completeFutureExceptionally(requestReponseLink.createFuture, operationTimedout);
						}
					}
				}
				, messagingFactory.getOperationTimeout()
				, TimerType.OneTimeRun);
		
		requestReponseLink.sendTokenAndSetRenewTimer(false).handleAsync((v, sasTokenEx) -> {
            if(sasTokenEx != null)
            {
                Throwable cause = ExceptionUtil.extractAsyncCompletionCause(sasTokenEx);
                requestReponseLink.createFuture.completeExceptionally(cause);
            }
            else
            {
                try
                {
                    messagingFactory.scheduleOnReactorThread(new DispatchHandler()
                    {
                        @Override
                        public void onEvent()
                        {
                            requestReponseLink.createInternalLinks();
                        }
                    });
                }
                catch (IOException ioException)
                {
                    requestReponseLink.cancelSASTokenRenewTimer();
                    requestReponseLink.createFuture.completeExceptionally(new ServiceBusException(false, "Failed to create internal links, see cause for more details.", ioException));
                }
            }
            
            return null;
        }, MessagingFactory.INTERNAL_THREAD_POOL);
		
		CompletableFuture.allOf(requestReponseLink.amqpSender.openFuture, requestReponseLink.amqpReceiver.openFuture).handleAsync((v, ex) -> 
        {
            if(ex == null)
            {
                requestReponseLink.createFuture.complete(requestReponseLink);
            }
            else
            {
                requestReponseLink.cancelSASTokenRenewTimer();
                requestReponseLink.createFuture.completeExceptionally(ex);
            }
            
            return null;
        }, MessagingFactory.INTERNAL_THREAD_POOL);
		
		return requestReponseLink.createFuture;
	}
	
	public static String getManagementNodeLinkPath(String entityPath)
	{
		return String.format("%s/%s", entityPath,  AmqpConstants.MANAGEMENT_NODE_ADDRESS_SEGMENT);
	}
	
	public static String getCBSNodeLinkPath()
    {
        return AmqpConstants.CBS_NODE_ADDRESS_SEGMENT;
    }
	
	private RequestResponseLink(MessagingFactory messagingFactory, String linkName, String linkPath, String sasTokenAudienceURI, MessagingEntityType entityType)
	{
		super(linkName);
		
		this.internalLinkGeneration = 1;
		this.recreateLinksLock = new Object();
		this.isRecreateLinksInProgress = false;
		this.underlyingFactory = messagingFactory;
		this.linkPath = linkPath;
		this.sasTokenAudienceURI = sasTokenAudienceURI;
		this.amqpSender = new InternalSender(linkName + ":internalSender", this, null);
		this.amqpReceiver = new InternalReceiver(linkName + ":interalReceiver", this);
		this.pendingRequests = new ConcurrentHashMap<>();
		this.requestCounter = new AtomicInteger();
		this.replyTo = UUID.randomUUID().toString();
		this.createFuture = new CompletableFuture<RequestResponseLink>();
		this.entityType = entityType;
		this.isInnerLinksCloseHandled = false;
	}
	
	public String getLinkPath()
	{
	    return this.linkPath;
	}
	
	private CompletableFuture<Void> sendTokenAndSetRenewTimer(boolean retryOnFailure)
    {
        if(this.getIsClosingOrClosed() || this.sasTokenAudienceURI == null)
        {
            return CompletableFuture.completedFuture(null);
        }
        else
        {
            CompletableFuture<ScheduledFuture<?>> sendTokenFuture = this.underlyingFactory.sendSecurityTokenAndSetRenewTimer(this.sasTokenAudienceURI, retryOnFailure, () -> this.sendTokenAndSetRenewTimer(true));
            return sendTokenFuture.thenAccept((f) -> {this.sasTokenRenewTimerFuture = f;});
        }
    }
	
	private void onInnerLinksClosed(int linkGeneration, Exception exception)
	{
		// Ignore exceptions from last generation links
		if(this.internalLinkGeneration == linkGeneration)
		{
			// This method is called twice once when inner receive link is closed and again when inner send link is closed.
			// Both of them happen in succession anyway, not concurrently as all these happen on the reactor thread.
			if(!this.isInnerLinksCloseHandled)
			{
				// Set it here and Reset the flag only before recreating inner links
				this.isInnerLinksCloseHandled = true;
				this.cancelSASTokenRenewTimer();
				if(this.pendingRequests.size() > 0)
				{
					if(exception != null && exception instanceof ServiceBusException && ((ServiceBusException) exception).getIsTransient())
					{
						Duration nextRetryInterval = this.underlyingFactory.getRetryPolicy().getNextRetryInterval(this.getClientId(), exception, this.underlyingFactory.getOperationTimeout());
						if (nextRetryInterval != null)
						{
							Timer.schedule(() -> {RequestResponseLink.this.ensureUniqueLinkRecreation();}, nextRetryInterval, TimerType.OneTimeRun);
						}
						else
						{
							this.completeAllPendingRequestsWithException(exception);
						}
					}
					else
					{
						this.completeAllPendingRequestsWithException(exception);
					}
				}
			}
		}
	}
	
	private void cancelSASTokenRenewTimer()
    {
        if(this.sasTokenRenewTimerFuture != null && !this.sasTokenRenewTimerFuture.isDone())
        {
            this.sasTokenRenewTimerFuture.cancel(true);
        }
    }
	
	private void createInternalLinks()
	{
		this.isInnerLinksCloseHandled = false;
		Map<Symbol, Object> commonLinkProperties = new HashMap<>();
		// ServiceBus expects timeout to be of type unsignedint
		commonLinkProperties.put(ClientConstants.LINK_TIMEOUT_PROPERTY, UnsignedInteger.valueOf(Util.adjustServerTimeout(this.underlyingFactory.getOperationTimeout()).toMillis()));
		if(this.entityType != null)
		{
			commonLinkProperties.put(ClientConstants.ENTITY_TYPE_PROPERTY, this.entityType.getIntValue());
		}
		
		// Create send link
		final Connection connection = this.underlyingFactory.getConnection();

		Session session = connection.session();
		session.setOutgoingWindow(Integer.MAX_VALUE);
		session.open();
		BaseHandler.setHandler(session, new SessionHandler(this.linkPath));

		String sendLinkNamePrefix = "RequestResponseLink-Sender".concat(TrackingUtil.TRACKING_ID_TOKEN_SEPARATOR).concat(StringUtil.getShortRandomString());
		String sendLinkName = !StringUtil.isNullOrEmpty(connection.getRemoteContainer()) ?
				sendLinkNamePrefix.concat(TrackingUtil.TRACKING_ID_TOKEN_SEPARATOR).concat(connection.getRemoteContainer()) :
				sendLinkNamePrefix;
		
		Sender sender = session.sender(sendLinkName);
		Target sednerTarget = new Target();
		sednerTarget.setAddress(this.linkPath);
		sender.setTarget(sednerTarget);
		Source senderSource = new Source();
		senderSource.setAddress(this.replyTo);
		sender.setSource(senderSource);
		sender.setSenderSettleMode(SenderSettleMode.SETTLED);
		sender.setProperties(commonLinkProperties);
		SendLinkHandler sendLinkHandler = new SendLinkHandler(this.amqpSender);
		BaseHandler.setHandler(sender, sendLinkHandler);
		
		// Create receive link
		session = connection.session();
        session.setOutgoingWindow(Integer.MAX_VALUE);
        session.open();
        BaseHandler.setHandler(session, new SessionHandler(this.linkPath));
        
		String receiveLinkNamePrefix = "RequestResponseLink-Receiver".concat(TrackingUtil.TRACKING_ID_TOKEN_SEPARATOR).concat(StringUtil.getShortRandomString());
		String receiveLinkName = !StringUtil.isNullOrEmpty(connection.getRemoteContainer()) ? 
				receiveLinkNamePrefix.concat(TrackingUtil.TRACKING_ID_TOKEN_SEPARATOR).concat(connection.getRemoteContainer()) :
				receiveLinkNamePrefix;
		Receiver receiver = session.receiver(receiveLinkName);
		Source receiverSource = new Source();
		receiverSource.setAddress(this.linkPath);
		receiver.setSource(receiverSource);
		Target receiverTarget = new Target();
		receiverTarget.setAddress(this.replyTo);
		receiver.setTarget(receiverTarget);

		// Set settle modes
		receiver.setSenderSettleMode(SenderSettleMode.SETTLED);
		receiver.setReceiverSettleMode(ReceiverSettleMode.FIRST);
		receiver.setProperties(commonLinkProperties);

		final ReceiveLinkHandler receiveLinkHandler = new ReceiveLinkHandler(this.amqpReceiver);
		BaseHandler.setHandler(receiver, receiveLinkHandler);
		
		this.amqpSender.setLinks(sender, receiver);
		this.amqpReceiver.setLinks(sender, receiver);
		
		sender.open();
		this.underlyingFactory.registerForConnectionError(sender);
		receiver.open();
		this.underlyingFactory.registerForConnectionError(receiver);
	}
	
	private void ensureUniqueLinkRecreation()
	{
		synchronized (this.recreateLinksLock) {
            if(!this.isRecreateLinksInProgress)
            {
                this.isRecreateLinksInProgress = true;
                this.recreateInternalLinks().handleAsync((v, recreationEx) ->
                {
                    if(recreationEx != null)
                    {
                    }
                    
                    synchronized (this.recreateLinksLock)
                    {
                        this.isRecreateLinksInProgress = false;
                    }
                    
                    return null;
                }, MessagingFactory.INTERNAL_THREAD_POOL);
            }
        }
	}
	
	private CompletableFuture<Void> recreateInternalLinks()
	{
		this.amqpSender.closeInternals(false);
		this.amqpReceiver.closeInternals(false);
		this.cancelSASTokenRenewTimer();
		
		// Create new internal sender and receiver objects, as old ones are closed
		this.internalLinkGeneration++;
        this.amqpSender = new InternalSender(this.getClientId() + ":internalSender", this, this.amqpSender);
        this.amqpReceiver = new InternalReceiver(this.getClientId() + ":interalReceiver", this);
        CompletableFuture<Void> recreateInternalLinksFuture = new CompletableFuture<Void>();
        this.sendTokenAndSetRenewTimer(false).handleAsync((v, sasTokenEx) -> {
            if(sasTokenEx != null)
            {
                Throwable cause = ExceptionUtil.extractAsyncCompletionCause(sasTokenEx);
                recreateInternalLinksFuture.completeExceptionally(cause);
                this.completeAllPendingRequestsWithException(cause);
            }
            else
            {
                try
                {
                    this.underlyingFactory.scheduleOnReactorThread(new DispatchHandler()
                    {
                        @Override
                        public void onEvent()
                        {
                            RequestResponseLink.this.createInternalLinks();
                        }
                    });
                }
                catch (IOException ioException)
                {
                    this.cancelSASTokenRenewTimer();
                    recreateInternalLinksFuture.completeExceptionally(new ServiceBusException(false, "Failed to create internal links, see cause for more details.", ioException));
                }
            }
            
            return null;
        }, MessagingFactory.INTERNAL_THREAD_POOL);
        
        CompletableFuture.allOf(this.amqpSender.openFuture, this.amqpReceiver.openFuture).handleAsync((v, ex) -> 
        {
            if(ex == null)
            {
                this.underlyingFactory.getRetryPolicy().resetRetryCount(this.getClientId());
                recreateInternalLinksFuture.complete(null);
            }
            else
            {
                this.cancelSASTokenRenewTimer();
                recreateInternalLinksFuture.completeExceptionally(ex);
            }
            
            return null;
        }, MessagingFactory.INTERNAL_THREAD_POOL);
		
		Timer.schedule(
            new Runnable()
            {
                public void run()
                {
                    if (!recreateInternalLinksFuture.isDone())
                    {
                        Exception operationTimedout = new TimeoutException(
                                String.format(Locale.US, "Recreating internal links of requestresponselink to %s timed out.", RequestResponseLink.this.linkPath));
                        RequestResponseLink.this.cancelSASTokenRenewTimer();
                        AsyncUtil.completeFutureExceptionally(recreateInternalLinksFuture, operationTimedout);
                    }
                }
            }
            , this.underlyingFactory.getOperationTimeout()
            , TimerType.OneTimeRun);
		
		return recreateInternalLinksFuture;
	}
	
	private void completeAllPendingRequestsWithException(Throwable exception)
	{
		for(RequestResponseWorkItem workItem : this.pendingRequests.values())
		{
			AsyncUtil.completeFutureExceptionally(workItem.getWork(), exception);
			workItem.cancelTimeoutTask(true);
		}
		
		this.pendingRequests.clear();
	}
	
	public CompletableFuture<Message> requestAysnc(Message requestMessage, Duration timeout)
	{	    
		this.throwIfClosed(null);
		
		CompletableFuture<Message> responseFuture = new CompletableFuture<Message>();
		RequestResponseWorkItem workItem = new RequestResponseWorkItem(requestMessage, responseFuture, timeout);
		String requestId = "request:" +  this.requestCounter.incrementAndGet();
		requestMessage.setMessageId(requestId);
		requestMessage.setReplyTo(this.replyTo);
		this.pendingRequests.put(requestId, workItem);
		workItem.setTimeoutTask(this.scheduleRequestTimeout(requestId, timeout));
		this.amqpSender.sendRequest(requestId, false);
		
		// Check and recreate links if necessary
        if(!((this.amqpSender.sendLink.getLocalState() == EndpointState.ACTIVE && this.amqpSender.sendLink.getRemoteState() == EndpointState.ACTIVE)
                && (this.amqpReceiver.receiveLink.getLocalState() == EndpointState.ACTIVE && this.amqpReceiver.receiveLink.getRemoteState() == EndpointState.ACTIVE)))
        {
            this.ensureUniqueLinkRecreation();
        }
        
		return responseFuture;
	}
	
	private ScheduledFuture<?> scheduleRequestTimeout(String requestId, Duration timeout)
	{
		return Timer.schedule(new Runnable() {
				public void run()
				{
					RequestResponseWorkItem completedWorkItem = RequestResponseLink.this.exceptionallyCompleteRequest(requestId, new TimeoutException("Request timed out."), true);
					boolean isRetriedWorkItem = completedWorkItem.getLastKnownException() != null;
					RequestResponseLink.this.amqpSender.removeEnqueuedRequest(requestId, isRetriedWorkItem);
				}
			}, timeout, TimerType.OneTimeRun);
	}
	
	
	private RequestResponseWorkItem exceptionallyCompleteRequest(String requestId, Exception exception, boolean useLastKnownException)
	{
		RequestResponseWorkItem workItem = this.pendingRequests.remove(requestId);
		if(workItem != null)
		{
			Exception exceptionToReport = exception;
			if(useLastKnownException && workItem.getLastKnownException() != null)
			{
				exceptionToReport = workItem.getLastKnownException();
			}
			
			workItem.getWork().completeExceptionally(exceptionToReport);
			AsyncUtil.completeFutureExceptionally(workItem.getWork(), exceptionToReport);
			workItem.cancelTimeoutTask(true);
		}
		
		return workItem;
	}
	
	private RequestResponseWorkItem completeRequestWithResponse(String requestId, Message responseMessage)
	{
		RequestResponseWorkItem workItem = this.pendingRequests.get(requestId);
		if(workItem != null)
		{
			int statusCode = RequestResponseUtils.getResponseStatusCode(responseMessage);
			// Retry on server busy and other retry-able status codes (what are other codes??)
			if(statusCode == ClientConstants.REQUEST_RESPONSE_SERVER_BUSY_STATUS_CODE)
			{
				// error response
				Exception responseException = RequestResponseUtils.genereateExceptionFromResponse(responseMessage);
				this.underlyingFactory.getRetryPolicy().incrementRetryCount(this.getClientId());
				Duration retryInterval = this.underlyingFactory.getRetryPolicy().getNextRetryInterval(this.getClientId(), responseException, workItem.getTimeoutTracker().remaining());
				if (retryInterval == null)
				{
					// Either not retry-able or not enough time to retry
					this.exceptionallyCompleteRequest(requestId, responseException, false);
				}
				else
				{
					// Retry
					workItem.setLastKnownException(responseException);
					try {
						this.underlyingFactory.scheduleOnReactorThread((int) retryInterval.toMillis(),
								new DispatchHandler()
								{
									@Override
									public void onEvent()
									{
										RequestResponseLink.this.amqpSender.sendRequest(requestId, true);
									}
								});
					} catch (IOException e) {
						this.exceptionallyCompleteRequest(requestId, responseException, false);
					}
				}
			}
			else
			{
				this.underlyingFactory.getRetryPolicy().resetRetryCount(this.getClientId());
				this.pendingRequests.remove(requestId);
				workItem.getWork().complete(responseMessage);
				workItem.cancelTimeoutTask(true);
			}
		}
		else
		{
		}		
		
		return workItem;
	}

	@Override
	protected CompletableFuture<Void> onClose() {
	    this.cancelSASTokenRenewTimer();
		return this.amqpSender.closeAsync().thenComposeAsync((v) -> this.amqpReceiver.closeAsync(), MessagingFactory.INTERNAL_THREAD_POOL);
	}
	
	private static void scheduleLinkCloseTimeout(CompletableFuture<Void> closeFuture, Duration timeout, String linkName)
	{		
		Timer.schedule(
				new Runnable()
				{
					public void run()
					{
						if (!closeFuture.isDone())
						{
							Exception operationTimedout = new TimeoutException(String.format(Locale.US, "%s operation on Link(%s) timed out at %s", "Close", linkName, ZonedDateTime.now()));
							
							AsyncUtil.completeFutureExceptionally(closeFuture, operationTimedout);
						}
					}
				}
				, timeout
				, TimerType.OneTimeRun);
	}
	
	private class InternalReceiver extends ClientEntity implements IAmqpReceiver
	{
		private RequestResponseLink parent;
		private Receiver receiveLink;
		private Sender matchingSendLink;
		private CompletableFuture<Void> openFuture;
		private CompletableFuture<Void> closeFuture;
		private int linkGeneration;
		
		protected InternalReceiver(String clientId, RequestResponseLink parent) {
			super(clientId);
			this.parent = parent;
			this.linkGeneration = parent.internalLinkGeneration;// Read it in the constructor as it may change later
			this.openFuture = new CompletableFuture<Void>();
			this.closeFuture = new CompletableFuture<Void>();
		}		

		@Override		
		protected CompletableFuture<Void> onClose() {
			this.closeInternals(true);
			return this.closeFuture;
		}
		
		void closeInternals(boolean waitForCloseCompletion)
		{
		    if (!this.getIsClosed())
            {
                if (this.receiveLink != null && this.receiveLink.getLocalState() != EndpointState.CLOSED)
                {
                    try {
                        this.parent.underlyingFactory.scheduleOnReactorThread(new DispatchHandler() {
                            
                            @Override
                            public void onEvent() {
                                if (InternalReceiver.this.receiveLink != null && InternalReceiver.this.receiveLink.getLocalState() != EndpointState.CLOSED)
                                {
                                    InternalReceiver.this.receiveLink.close();
                                    InternalReceiver.this.parent.underlyingFactory.deregisterForConnectionError(InternalReceiver.this.receiveLink);
                                    if(waitForCloseCompletion)
                                    {
                                        RequestResponseLink.scheduleLinkCloseTimeout(InternalReceiver.this.closeFuture, InternalReceiver.this.parent.underlyingFactory.getOperationTimeout(), InternalReceiver.this.receiveLink.getName());
                                    }
                                    else
                                    {
                                        AsyncUtil.completeFuture(InternalReceiver.this.closeFuture, null);
                                    }
                                }
                            }
                        });
                    } catch (IOException e) {
                        AsyncUtil.completeFutureExceptionally(this.closeFuture, e);
                    }
                }
                else
                {
                    AsyncUtil.completeFuture(this.closeFuture, null);
                }
            }
		}

		@Override
		public void onOpenComplete(Exception completionException) {
			if(completionException == null)
			{
				AsyncUtil.completeFuture(this.openFuture, null);
				
				// Send unlimited credit
				this.receiveLink.flow(Integer.MAX_VALUE);
			}
			else
			{
			    this.setClosed();
			    AsyncUtil.completeFuture(this.closeFuture, null);
				AsyncUtil.completeFutureExceptionally(this.openFuture, completionException);
			}
		}

		@Override
		public void onError(Exception exception) {
			if(!this.openFuture.isDone())
			{
			    this.onOpenComplete(exception);
			}
			
			if(this.getIsClosingOrClosed())
			{
				if(!this.closeFuture.isDone())
				{
					AsyncUtil.completeFutureExceptionally(this.closeFuture, exception);
				}
			}
			else
			{
				this.parent.underlyingFactory.deregisterForConnectionError(this.receiveLink);
				this.matchingSendLink.close();
                this.parent.underlyingFactory.deregisterForConnectionError(this.matchingSendLink);
				this.parent.onInnerLinksClosed(this.linkGeneration, exception);
			}
		}

		@Override
		public void onClose(ErrorCondition condition) {
			if(condition == null || condition.getCondition() == null)
			{
				if(this.getIsClosingOrClosed() && !this.closeFuture.isDone())
				{
					AsyncUtil.completeFuture(this.closeFuture, null);
				}
			}
			else
			{
				Exception exception = ExceptionUtil.toException(condition);
				this.onError(exception);
			}
		}

		@Override
		public void onReceiveComplete(Delivery delivery)
		{
		    Message responseMessage = null;
		    try
		    {
		        responseMessage = Util.readMessageFromDelivery(this.receiveLink, delivery);
		        delivery.disposition(Accepted.getInstance());
		        delivery.settle();
		    }
			catch(Exception e)
		    {			    
			    
			    // release the delivery ??
			    delivery.disposition(Released.getInstance());
                delivery.settle();
			    return;
		    }
			
		    // Return response in a separate thread so reactor thread is free to handle reactor events
		    final Message finalResponseMessage = responseMessage;
		    MessagingFactory.INTERNAL_THREAD_POOL.submit(() -> {
		        String requestMessageId = (String)finalResponseMessage.getCorrelationId();
		        if(requestMessageId != null)
	            {
	                this.parent.completeRequestWithResponse(requestMessageId, finalResponseMessage);
	            }
	            else
	            {
	            }
		    });
		}
		
		public void setLinks(Sender sendLink, Receiver receiveLink) {			
			this.receiveLink = receiveLink;
			this.matchingSendLink = sendLink;
		}
	}
	
	private class InternalSender extends ClientEntity implements IAmqpSender
	{
		private Sender sendLink;
		private Receiver matchingReceiveLink;
		private RequestResponseLink parent;
		private CompletableFuture<Void> openFuture;
		private CompletableFuture<Void> closeFuture;
		private AtomicInteger availableCredit;
		private LinkedList<String> pendingFreshSends;
		private LinkedList<String> pendingRetrySends;
		private Object pendingSendsSyncLock;
		private boolean isSendLoopRunning;
		private int maxMessageSize;
		private int linkGeneration;

		protected InternalSender(String clientId, RequestResponseLink parent, InternalSender senderToBeCopied) {
			super(clientId);			
			this.parent = parent;
			this.linkGeneration = parent.internalLinkGeneration;// Read it in the constructor as it may change later
			this.availableCredit = new AtomicInteger(0);
			this.pendingSendsSyncLock = new Object();
			this.isSendLoopRunning = false;
			this.openFuture = new CompletableFuture<Void>();
			this.closeFuture = new CompletableFuture<Void>();
			
			if(senderToBeCopied == null)
			{
			    this.pendingFreshSends = new LinkedList<>();
	            this.pendingRetrySends = new LinkedList<>();
			}
			else
			{
			    this.pendingFreshSends = senderToBeCopied.pendingFreshSends;
                this.pendingRetrySends = senderToBeCopied.pendingRetrySends;
			}
		}		

		@Override
		protected CompletableFuture<Void> onClose() {
			this.closeInternals(true);
			return this.closeFuture;
		}
		
		void closeInternals(boolean waitForCloseCompletion)
		{
		    if (!this.getIsClosed())
            {
                if (this.sendLink != null && this.sendLink.getLocalState() != EndpointState.CLOSED)
                {
                    try {
                        this.parent.underlyingFactory.scheduleOnReactorThread(new DispatchHandler() {
                            
                            @Override
                            public void onEvent() {
                                if (InternalSender.this.sendLink != null && InternalSender.this.sendLink.getLocalState() != EndpointState.CLOSED)
                                {
                                    InternalSender.this.sendLink.close();
                                    InternalSender.this.parent.underlyingFactory.deregisterForConnectionError(InternalSender.this.sendLink);
                                    if(waitForCloseCompletion)
                                    {
                                        RequestResponseLink.scheduleLinkCloseTimeout(InternalSender.this.closeFuture, InternalSender.this.parent.underlyingFactory.getOperationTimeout(), InternalSender.this.sendLink.getName());
                                    }
                                    else
                                    {
                                        AsyncUtil.completeFuture(InternalSender.this.closeFuture, null);
                                    }
                                }
                            }
                        });
                    } catch (IOException e) {
                        AsyncUtil.completeFutureExceptionally(this.closeFuture, e);
                    }
                }
                else
                {
                    AsyncUtil.completeFuture(this.closeFuture, null);
                }
            }
		}

		@Override
		public void onOpenComplete(Exception completionException) {
			if(completionException == null)
			{
			    this.maxMessageSize = Util.getMaxMessageSizeFromLink(this.sendLink);
				AsyncUtil.completeFuture(this.openFuture, null);
				this.runSendLoop();
			}
			else
			{
			    this.setClosed();
				AsyncUtil.completeFuture(this.closeFuture, null);
				AsyncUtil.completeFutureExceptionally(this.openFuture, completionException);
			}
		}

		@Override
		public void onError(Exception exception) {
			if(!this.openFuture.isDone())
			{
			    this.onOpenComplete(exception);
			}
			
			if(this.getIsClosingOrClosed())
			{
				if(!this.closeFuture.isDone())
				{
					AsyncUtil.completeFutureExceptionally(this.closeFuture, exception);
				}
			}
			else
			{
				this.parent.underlyingFactory.deregisterForConnectionError(this.sendLink);
				this.matchingReceiveLink.close();
                this.parent.underlyingFactory.deregisterForConnectionError(this.matchingReceiveLink);
	            this.parent.onInnerLinksClosed(this.linkGeneration, exception);
			}
		}

		@Override
		public void onClose(ErrorCondition condition) {
			if(condition == null || condition.getCondition() == null)
			{
				if(!this.closeFuture.isDone() && !this.closeFuture.isDone())
				{
					AsyncUtil.completeFuture(this.closeFuture, null);
				}
			}
			else
			{
				Exception exception = ExceptionUtil.toException(condition);
				this.onError(exception);
			}
		}
		
		public void sendRequest(String requestId, boolean isRetry)
		{
			synchronized(this.pendingSendsSyncLock)
			{
				if(isRetry)
				{
					this.pendingRetrySends.add(requestId);
				}
				else
				{
					this.pendingFreshSends.add(requestId);
				}
				
				// This check must be done inside lock
				if(this.isSendLoopRunning)
	            {
	                return;
	            }
			}
			
			try {
                this.parent.underlyingFactory.scheduleOnReactorThread(new DispatchHandler() {
                    @Override
                    public void onEvent() {
                        InternalSender.this.runSendLoop();
                    }
                });
            } catch (IOException e) {
                this.parent.exceptionallyCompleteRequest(requestId, e, true);
            }
		}
		
		public void removeEnqueuedRequest(String requestId, boolean isRetry)
		{
			synchronized(this.pendingSendsSyncLock)
			{
				// Collections are more likely to be very small. So remove() shouldn't be a problem.
				if(isRetry)
				{
					this.pendingRetrySends.remove(requestId);
				}
				else
				{
					this.pendingFreshSends.remove(requestId);
				}				
			}
		}

		@Override
		public void onFlow(int creditIssued) {
			this.availableCredit.addAndGet(creditIssued);
			this.runSendLoop();
		}

		@Override
		public void onSendComplete(Delivery delivery) {
			// Doesn't happen as sends are settled on send
		}
		
		public void setLinks(Sender sendLink, Receiver receiveLink) {			
			this.sendLink = sendLink;
			this.matchingReceiveLink = receiveLink;
			this.availableCredit = new AtomicInteger(0);
		}
		
		private void runSendLoop()
		{
		    synchronized(this.pendingSendsSyncLock)
		    {
		        if(this.isSendLoopRunning)
		        {
		            return;
		        }
		        else
		        {
		            this.isSendLoopRunning = true;
		        }
		    }
		    
		    
		    try
            {
                while(this.sendLink != null && this.sendLink.getLocalState() == EndpointState.ACTIVE && this.sendLink.getRemoteState() == EndpointState.ACTIVE && this.availableCredit.get() > 0)
                {
                	String requestIdToBeSent = null;					
                    synchronized(pendingSendsSyncLock)
                    {
                        // First send retries and then fresh ones
                    	requestIdToBeSent = this.pendingRetrySends.poll();
                        if(requestIdToBeSent == null)
                        {
                        	requestIdToBeSent = this.pendingFreshSends.poll();
                            if(requestIdToBeSent == null)
                            {
                                // Set to false inside the synchronized block to avoid race condition
                                this.isSendLoopRunning = false;
                                break;
                            }
                        }
                    }
                    
                    RequestResponseWorkItem requestToBeSent = this.parent.pendingRequests.get(requestIdToBeSent);
                    if(requestToBeSent != null)
                    {
                    	Delivery delivery = this.sendLink.delivery(UUID.randomUUID().toString().getBytes());
                        delivery.setMessageFormat(DeliveryImpl.DEFAULT_MESSAGE_FORMAT);
                    
                        Pair<byte[], Integer> encodedPair = null;
                        try
                        {
                        	encodedPair = Util.encodeMessageToOptimalSizeArray(requestToBeSent.getRequest(), this.maxMessageSize);
                        }
                        catch(PayloadSizeExceededException exception)
                        {
                        	this.parent.exceptionallyCompleteRequest(requestIdToBeSent, new PayloadSizeExceededException(String.format("Size of the payload exceeded Maximum message size: %s kb", this.maxMessageSize / 1024), exception), false);
                        }
                        
                        try
                        {
                            int sentMsgSize = this.sendLink.send(encodedPair.getFirstItem(), 0, encodedPair.getSecondItem());
                            assert sentMsgSize == encodedPair.getSecondItem() : "Contract of the ProtonJ library for Sender.Send API changed";
                            delivery.settle();
                            this.availableCredit.decrementAndGet();
                        }
                        catch(Exception e)
                        {
                            this.parent.exceptionallyCompleteRequest(requestIdToBeSent, e, false);
                        }
                    }
                    else
                    {
                    }
                }
            }
            finally
            {
                synchronized (this.pendingSendsSyncLock) {
                    if(this.isSendLoopRunning)
                    {
                        this.isSendLoopRunning = false;
                    }
                }
                
            }
		}
	}
}
