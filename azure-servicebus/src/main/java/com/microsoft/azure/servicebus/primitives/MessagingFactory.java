/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus.primitives;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.UnresolvedAddressException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Handler;
import org.apache.qpid.proton.engine.HandlerException;
import org.apache.qpid.proton.engine.Link;
import org.apache.qpid.proton.reactor.Reactor;

import com.microsoft.azure.servicebus.ClientSettings;
import com.microsoft.azure.servicebus.amqp.BaseLinkHandler;
import com.microsoft.azure.servicebus.amqp.ConnectionHandler;
import com.microsoft.azure.servicebus.amqp.DispatchHandler;
import com.microsoft.azure.servicebus.amqp.IAmqpConnection;
import com.microsoft.azure.servicebus.amqp.ProtonUtil;
import com.microsoft.azure.servicebus.amqp.ReactorDispatcher;
import com.microsoft.azure.servicebus.amqp.ReactorHandler;
import com.microsoft.azure.servicebus.security.SecurityToken;

/**
 * Abstracts all AMQP related details and encapsulates an AMQP connection and manages its life cycle. Each instance of this class represent one AMQP connection to the namespace.
 * If an application creates multiple senders, receivers or clients using the same MessagingFacotry instance, all those senders, receivers or clients will share the same connection to the namespace.
 * @since 1.0
 */
public class MessagingFactory extends ClientEntity implements IAmqpConnection
{
    public static final ExecutorService INTERNAL_THREAD_POOL = Executors.newCachedThreadPool();
	
    private static final String REACTOR_THREAD_NAME_PREFIX = "ReactorThread";
	private static final int MAX_CBS_LINK_CREATION_ATTEMPTS = 3;
	private final String hostName;
	private final CompletableFuture<Void> connetionCloseFuture;
	private final ConnectionHandler connectionHandler;
	private final ReactorHandler reactorHandler;
	private final LinkedList<Link> registeredLinks;
	private final Object reactorLock;
	private final RequestResponseLinkcache managementLinksCache;
	
	private Reactor reactor;
	private ReactorDispatcher reactorScheduler;
	private Connection connection;

	private CompletableFuture<MessagingFactory> factoryOpenFuture;
	private CompletableFuture<Void> cbsLinkCreationFuture;
	private RequestResponseLink cbsLink;
	private int cbsLinkCreationAttempts = 0;
	private Throwable lastCBSLinkCreationException = null;
	
	private final ClientSettings clientSettings;
	
	private MessagingFactory(URI namespaceEndpointUri, ClientSettings clientSettings)
	{
	    super("MessagingFactory".concat(StringUtil.getShortRandomString()));
	    this.clientSettings = clientSettings;
	    
	    this.hostName = namespaceEndpointUri.getHost();
	    this.registeredLinks = new LinkedList<Link>();
        this.connetionCloseFuture = new CompletableFuture<Void>();
        this.reactorLock = new Object();
        this.connectionHandler = new ConnectionHandler(this);
        this.factoryOpenFuture = new CompletableFuture<MessagingFactory>();
        this.cbsLinkCreationFuture = new CompletableFuture<Void>();
        this.managementLinksCache = new RequestResponseLinkcache(this);
        this.reactorHandler = new ReactorHandler()
        {
            @Override
            public void onReactorInit(Event e)
            {
                super.onReactorInit(e);

                final Reactor r = e.getReactor();
                connection = r.connectionToHost(hostName, MessagingFactory.this.connectionHandler.getPort(), connectionHandler);
            }
        };
        Timer.register(this.getClientId());
	}

	@Override
	public String getHostName()
	{
		return this.hostName;
	}
	
	private Reactor getReactor()
	{
		synchronized (this.reactorLock)
		{
			return this.reactor;
		}
	}
	
	private ReactorDispatcher getReactorScheduler()
	{
		synchronized (this.reactorLock)
		{
			return this.reactorScheduler;
		}
	}

	private void startReactor(ReactorHandler reactorHandler) throws IOException
	{
		Reactor newReactor = ProtonUtil.reactor(reactorHandler);
		synchronized (this.reactorLock)
		{
			this.reactor = newReactor;
			this.reactorScheduler = new ReactorDispatcher(newReactor);
		}
		
		String reactorThreadName = REACTOR_THREAD_NAME_PREFIX + UUID.randomUUID().toString();
		Thread reactorThread = new Thread(new RunReactor(), reactorThreadName);
		reactorThread.start();
	}
	
	Connection getConnection()
	{
		if (this.connection == null || this.connection.getLocalState() == EndpointState.CLOSED || this.connection.getRemoteState() == EndpointState.CLOSED)
		{
			this.connection = this.getReactor().connectionToHost(this.hostName, ClientConstants.AMQPS_PORT, this.connectionHandler);
		}

		return this.connection;
	}

	/**
	 * Gets the operation timeout from the connections string.
	 * @return operation timeout specified in the connection string
	 */
	public Duration getOperationTimeout()
	{
		return this.clientSettings.getOperationTimeout();
	}

	/**
	 * Gets the retry policy from the connection string.
	 * @return retry policy specified in the connection string
	 */
	public RetryPolicy getRetryPolicy()
	{
		return this.clientSettings.getRetryPolicy();
	}
	
	public ClientSettings getClientSetttings()
	{
	    return this.clientSettings;
	}
	
	public static CompletableFuture<MessagingFactory> createFromNamespaceNameAsyc(String sbNamespaceName, ClientSettings clientSettings)
	{
	    return createFromNamespaceEndpointURIAsyc(Util.convertNamespaceToEndPointURI(sbNamespaceName), clientSettings);
	}
	
	public static CompletableFuture<MessagingFactory> createFromNamespaceEndpointURIAsyc(URI namespaceEndpointURI, ClientSettings clientSettings)
    {
        
        MessagingFactory messagingFactory = new MessagingFactory(namespaceEndpointURI, clientSettings);
        try {
            messagingFactory.startReactor(messagingFactory.reactorHandler);
        } catch (IOException e) {
            messagingFactory.factoryOpenFuture.completeExceptionally(e);
        }
        return messagingFactory.factoryOpenFuture;
    }
	
	public static MessagingFactory createFromNamespaceName(String sbNamespaceName, ClientSettings clientSettings) throws InterruptedException, ServiceBusException
    {
	    return completeFuture(createFromNamespaceNameAsyc(sbNamespaceName, clientSettings));
    }
    
    public static MessagingFactory createFromNamespaceEndpointURI(URI namespaceEndpointURI, ClientSettings clientSettings) throws InterruptedException, ServiceBusException
    {
        return completeFuture(createFromNamespaceEndpointURIAsyc(namespaceEndpointURI, clientSettings));
    }

	/**	 
	 * Creates an instance of MessagingFactory from the given connection string builder. This is a non-blocking method.
	 * @param builder connection string builder to the  bus namespace or entity
	 * @return a <code>CompletableFuture</code> which completes when a connection is established to the namespace or when a connection couldn't be established.
	 * @see java.util.concurrent.CompletableFuture
	 */    
	public static CompletableFuture<MessagingFactory> createFromConnectionStringBuilderAsync(final ConnectionStringBuilder builder)
	{	
	    
	    return createFromNamespaceEndpointURIAsyc(builder.getEndpoint(), Util.getClientSettingsFromConnectionStringBuilder(builder));
	}
	
	/**
	 * Creates an instance of MessagingFactory from the given connection string. This is a non-blocking method.
	 * @param connectionString connection string to the  bus namespace or entity
	 * @return a <code>CompletableFuture</code> which completes when a connection is established to the namespace or when a connection couldn't be established.
	 * @see java.util.concurrent.CompletableFuture
	 */
	public static CompletableFuture<MessagingFactory> createFromConnectionStringAsync(final String connectionString)
	{
		ConnectionStringBuilder builder = new ConnectionStringBuilder(connectionString);
		return createFromConnectionStringBuilderAsync(builder);
	}
	
	/**
	 * Creates an instance of MessagingFactory from the given connection string builder. This method blocks for a connection to the namespace to be established.
	 * @param builder connection string builder to the  bus namespace or entity
	 * @return an instance of MessagingFactory
	 * @throws InterruptedException if blocking thread is interrupted
	 * @throws ExecutionException if a connection couldn't be established to the namespace. Cause of the failure can be found by calling {@link Exception#getCause()}
	 */
	public static MessagingFactory createFromConnectionStringBuilder(final ConnectionStringBuilder builder) throws InterruptedException, ExecutionException
	{		
		return createFromConnectionStringBuilderAsync(builder).get();
	}
	
	/**
	 * Creates an instance of MessagingFactory from the given connection string. This method blocks for a connection to the namespace to be established.
	 * @param connectionString connection string to the  bus namespace or entity
	 * @return an instance of MessagingFactory
	 * @throws InterruptedException if blocking thread is interrupted
	 * @throws ExecutionException if a connection couldn't be established to the namespace. Cause of the failure can be found by calling {@link Exception#getCause()}
	 */
	public static MessagingFactory createFromConnectionString(final String connectionString) throws InterruptedException, ExecutionException
	{		
		return createFromConnectionStringAsync(connectionString).get();
	}

	/**
     * Internal method.&nbsp;Clients should not use this method.
     */
	@Override
	public void onConnectionOpen()
	{
	    if(!factoryOpenFuture.isDone())
	    {
	        AsyncUtil.completeFuture(this.factoryOpenFuture, this);
	    }
	    
	    // Connection opened. Initiate new cbs link creation
	    if(this.cbsLink == null)
	    {
	        this.createCBSLinkAsync();
	    }
	}

	/**
	 * Internal method.&nbsp;Clients should not use this method.
	 */
	@Override
	public void onConnectionError(ErrorCondition error)
	{
	    if(error != null && error.getCondition() != null)
	    {
	    }
	    
		if (!this.factoryOpenFuture.isDone())
		{		    
		    AsyncUtil.completeFutureExceptionally(this.factoryOpenFuture, ExceptionUtil.toException(error));
		    this.setClosed();
		}
		else
		{
		    this.closeConnection(error, null);
		}

		if (this.getIsClosingOrClosed() && !this.connetionCloseFuture.isDone())
		{
		    AsyncUtil.completeFuture(this.connetionCloseFuture, null);
			Timer.unregister(this.getClientId());
		} 
	}

	private void onReactorError(Exception cause)
	{
		if (!this.factoryOpenFuture.isDone())
		{
		    AsyncUtil.completeFutureExceptionally(this.factoryOpenFuture, cause);
		    this.setClosed();
		}
		else
		{
		    if(this.getIsClosingOrClosed())
            {
                return;
            }
		    
			
			try
			{
				this.startReactor(this.reactorHandler);
			}
			catch (IOException e)
			{
				this.onReactorError(cause);
			}
			
			this.closeConnection(null, cause);
		}
	}
	
	// One of the parameters must be null
	private void closeConnection(ErrorCondition error, Exception cause)
	{
	    // Important to copy the reference of the connection as a call to getConnection might create a new connection while we are still in this method
	    Connection currentConnection = this.connection;
	    if(currentConnection != null)
	    {
	        Link[] links = this.registeredLinks.toArray(new Link[0]);
	        this.registeredLinks.clear();
	        
	        for(Link link : links)
	        {
	            link.close();
	        }
	        

	        if (currentConnection.getLocalState() != EndpointState.CLOSED)
	        {
	            currentConnection.close();
	        }
	        
	        for(Link link : links)
	        {
	            Handler handler = BaseHandler.getHandler(link);
	            if (handler != null && handler instanceof BaseLinkHandler)
	            {
	                BaseLinkHandler linkHandler = (BaseLinkHandler) handler;
	                if(error != null)
	                {
	                    linkHandler.processOnClose(link, error);
	                }
	                else
	                {
	                    linkHandler.processOnClose(link, cause);
	                }
	            }
	        }
	    }
	}

	@Override
	protected CompletableFuture<Void> onClose()
	{
		if (!this.getIsClosed())
		{
		    CompletableFuture<Void> cbsLinkCloseFuture;
		    if(this.cbsLink == null)
		    {
		        cbsLinkCloseFuture = CompletableFuture.completedFuture(null);
		    }
		    else
		    {
		        cbsLinkCloseFuture = this.cbsLink.closeAsync();
		    }
		    
		    cbsLinkCloseFuture.thenRun(() -> this.managementLinksCache.freeAsync()).thenRun(() -> {
		        if(this.cbsLinkCreationFuture != null && !this.cbsLinkCreationFuture.isDone())
	            {
	                this.cbsLinkCreationFuture.completeExceptionally(new Exception("Connection closed."));
	            }
		        
		        if (this.connection != null && this.connection.getRemoteState() != EndpointState.CLOSED)
	            {
	                try {
	                    this.scheduleOnReactorThread(new DispatchHandler()
	                    {
	                        @Override
	                        public void onEvent()
	                        {
	                            if (MessagingFactory.this.connection != null && MessagingFactory.this.connection.getLocalState() != EndpointState.CLOSED)
	                            {
	                                MessagingFactory.this.connection.close();
	                            }
	                        }
	                    });
	                } catch (IOException e) {
	                    this.connetionCloseFuture.completeExceptionally(e);
	                }
	                
	                Timer.schedule(new Runnable()
	                {
	                    @Override
	                    public void run()
	                    {
	                        if (!MessagingFactory.this.connetionCloseFuture.isDone())
	                        {
	                            String errorMessage = "Closing MessagingFactory timed out.";
	                            AsyncUtil.completeFutureExceptionally(MessagingFactory.this.connetionCloseFuture, new TimeoutException(errorMessage));
	                        }
	                    }
	                },
	                this.clientSettings.getOperationTimeout(), TimerType.OneTimeRun);
	            }
	            else
	            {
	                this.connetionCloseFuture.complete(null);
	                Timer.unregister(this.getClientId());
	            }
		    });
			
			return this.connetionCloseFuture;
		}
		else
		{
		    return CompletableFuture.completedFuture(null);
		}
	}

	private class RunReactor implements Runnable
	{
		final private Reactor rctr;

		public RunReactor()
		{
			this.rctr = MessagingFactory.this.getReactor();
		}

		public void run()
		{
			try
			{
				this.rctr.setTimeout(3141);
				this.rctr.start();
				boolean continuteProcessing = true;
                while(!Thread.interrupted() && continuteProcessing)
                {
                    // If factory is closed, stop reactor too
                    if(MessagingFactory.this.getIsClosed())
                    {
                        break;
                    }
                    continuteProcessing = this.rctr.process();
                }				
				this.rctr.stop();
			}
			catch (HandlerException handlerException)
			{
				Throwable cause = handlerException.getCause();
				if (cause == null)
				{
					cause = handlerException;
				}
				

				String message = !StringUtil.isNullOrEmpty(cause.getMessage()) ? 
						cause.getMessage():
						!StringUtil.isNullOrEmpty(handlerException.getMessage()) ? 
							handlerException.getMessage() :
							"Reactor encountered unrecoverable error";
				ServiceBusException sbException = new ServiceBusException(
						true,
						String.format(Locale.US, "%s, %s", message, ExceptionUtil.getTrackingIDAndTimeToLog()),
						cause);
				
				if (cause instanceof UnresolvedAddressException)
				{
					sbException = new CommunicationException(
							String.format(Locale.US, "%s. This is usually caused by incorrect hostname or network configuration. Please check to see if namespace information is correct. %s", message, ExceptionUtil.getTrackingIDAndTimeToLog()),
							cause);
				}
				
				MessagingFactory.this.onReactorError(sbException);
			}
			finally
			{
				this.rctr.free();
			}
		}
	}

	/**
     * Internal method.&nbsp;Clients should not use this method.
     */
	@Override
	public void registerForConnectionError(Link link)
	{
	    if(link != null)
	    {
	        this.registeredLinks.add(link);
	    }
	}

	/**
     * Internal method.&nbsp;Clients should not use this method.
     */
	@Override
	public void deregisterForConnectionError(Link link)
	{
	    if(link != null)
	    {
	        this.registeredLinks.remove(link);
	    }
	}
	
	void scheduleOnReactorThread(final DispatchHandler handler) throws IOException
	{
		this.getReactorScheduler().invoke(handler);
	}

	void scheduleOnReactorThread(final int delay, final DispatchHandler handler) throws IOException
	{
		this.getReactorScheduler().invoke(delay, handler);
	}
	
	CompletableFuture<ScheduledFuture<?>> sendSecurityTokenAndSetRenewTimer(String sasTokenAudienceURI, boolean retryOnFailure, Runnable validityRenewer)
    {
		CompletableFuture<ScheduledFuture<?>> result = new CompletableFuture<ScheduledFuture<?>>();
	    CompletableFuture<Instant> sendTokenFuture = this.generateAndSendSecurityToken(sasTokenAudienceURI);
	    sendTokenFuture.handleAsync((validUntil, sendTokenEx) -> {
            if(sendTokenEx == null)
            {
                ScheduledFuture<?> renewalFuture = MessagingFactory.scheduleRenewTimer(validUntil, validityRenewer);
                result.complete(renewalFuture);
            }
            else
            {
            	Throwable sendFailureCause = ExceptionUtil.extractAsyncCompletionCause(sendTokenEx);
                if(retryOnFailure)
                {
                	// Just schedule another attempt
                	ScheduledFuture<?> renewalFuture = Timer.schedule(validityRenewer, Duration.ofSeconds(ClientConstants.DEFAULT_SAS_TOKEN_SEND_RETRY_INTERVAL_IN_SECONDS), TimerType.OneTimeRun);
                	result.complete(renewalFuture);
                }
                else
                {
                	if(sendFailureCause instanceof TimeoutException)
                	{
                		// Retry immediately on timeout. This is a special case as CBSLink may be disconnected right after the token is sent, but before it reaches the service
                		CompletableFuture<Instant> resendTokenFuture = this.generateAndSendSecurityToken(sasTokenAudienceURI);
                		resendTokenFuture.handleAsync((resendValidUntil, resendTokenEx) -> {
                			if(resendTokenEx == null)
                			{
                                ScheduledFuture<?> renewalFuture = MessagingFactory.scheduleRenewTimer(resendValidUntil, validityRenewer);
                                result.complete(renewalFuture);
                			}
                			else
                			{
                				Throwable resendFailureCause = ExceptionUtil.extractAsyncCompletionCause(resendTokenEx);
                				result.completeExceptionally(resendFailureCause);
                			}
                            return null;
                		}, MessagingFactory.INTERNAL_THREAD_POOL);
                	}
                	else
                	{
                		result.completeExceptionally(sendFailureCause);
                	}
                }
            }
            return null;
        }, MessagingFactory.INTERNAL_THREAD_POOL);
	    
	    return result;
    }
	
	private CompletableFuture<Instant> generateAndSendSecurityToken(String sasTokenAudienceURI)
	{
		CompletableFuture<SecurityToken> tokenFuture = this.clientSettings.getTokenProvider().getSecurityTokenAsync(sasTokenAudienceURI);
		return tokenFuture.thenComposeAsync((t) ->
	    {
	        SecurityToken generatedSecurityToken = t;
	        return this.cbsLinkCreationFuture.thenComposeAsync((v) -> {
	                return CommonRequestResponseOperations.sendCBSTokenAsync(this.cbsLink, ClientConstants.SAS_TOKEN_SEND_TIMEOUT, generatedSecurityToken).thenApply((u) -> generatedSecurityToken.getValidUntil());
	            }, MessagingFactory.INTERNAL_THREAD_POOL);
	    }, MessagingFactory.INTERNAL_THREAD_POOL);
	}
	
	private static ScheduledFuture<?> scheduleRenewTimer(Instant currentTokenValidUntil, Runnable validityRenewer)
	{
		if(currentTokenValidUntil == Instant.MAX)
		{
			// User provided token or will never expire
			return null;
		}
		else
		{
			// It will eventually expire. Renew it
	        int renewInterval = Util.getTokenRenewIntervalInSeconds((int)Duration.between(Instant.now(), currentTokenValidUntil).getSeconds());
	        return Timer.schedule(validityRenewer, Duration.ofSeconds(renewInterval), TimerType.OneTimeRun);
		}
	}
	
	CompletableFuture<RequestResponseLink> obtainRequestResponseLinkAsync(String entityPath, MessagingEntityType entityType)
	{
	    this.throwIfClosed(null);
	    return this.managementLinksCache.obtainRequestResponseLinkAsync(entityPath, entityType);
	}
	
	void releaseRequestResponseLink(String entityPath)
	{
	    if(!this.getIsClosed())
	    {
	        this.managementLinksCache.releaseRequestResponseLink(entityPath);
	    }	    
	}
	
	private void createCBSLinkAsync()
    {
		if(this.getIsClosingOrClosed())
		{
			return;
		}
		
	    if(++this.cbsLinkCreationAttempts > MAX_CBS_LINK_CREATION_ATTEMPTS )
	    {
	        Throwable completionEx = this.lastCBSLinkCreationException == null ? new Exception("CBS link creation failed multiple times.") : this.lastCBSLinkCreationException;
	        this.cbsLinkCreationFuture.completeExceptionally(completionEx);
	    }
	    else
	    {	        
	        String requestResponseLinkPath = RequestResponseLink.getCBSNodeLinkPath();
	        RequestResponseLink.createAsync(this, this.getClientId() + "-cbs", requestResponseLinkPath, null, null).handleAsync((cbsLink, ex) ->
            {
                if(ex == null)
                {
                    if(this.getIsClosingOrClosed())
                    {
                    	// Factory is closed before CBSLink could be created. Close the created CBS link too
                    	cbsLink.closeAsync();
                    }
                    else
                    {
                    	this.cbsLink = cbsLink;	
                        this.cbsLinkCreationFuture.complete(null);
                    }                    
                }
                else
                {
                    this.lastCBSLinkCreationException = ExceptionUtil.extractAsyncCompletionCause(ex);
                    this.createCBSLinkAsync();
                }
                return null;
            }, MessagingFactory.INTERNAL_THREAD_POOL);
	                        
	    }	    
    }
	
	private static <T> T completeFuture(CompletableFuture<T> future) throws InterruptedException, ServiceBusException {
        try {
            return future.get();
        } catch (InterruptedException ie) {
            // Rare instance
            throw ie;
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof ServiceBusException) {
                throw (ServiceBusException) cause;
            } else {
                throw new ServiceBusException(true, cause);
            }
        }
    }
}
