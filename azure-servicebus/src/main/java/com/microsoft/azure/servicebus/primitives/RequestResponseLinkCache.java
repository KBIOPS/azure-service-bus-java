package com.microsoft.azure.servicebus.primitives;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;


class RequestResponseLinkcache 
{
    
    private Object lock = new Object();
    private final MessagingFactory underlyingFactory;    
    private HashMap<String, RequestResponseLinkWrapper> pathToRRLinkMap;
    
    public RequestResponseLinkcache(MessagingFactory underlyingFactory)
    {
        this.underlyingFactory = underlyingFactory;
        this.pathToRRLinkMap = new HashMap<>();
    }
    
    public CompletableFuture<RequestResponseLink> obtainRequestResponseLinkAsync(String entityPath, MessagingEntityType entityType)
    {
        RequestResponseLinkWrapper wrapper;
        synchronized (lock)
        {
            wrapper = this.pathToRRLinkMap.get(entityPath);
            if(wrapper == null)
            {
                wrapper = new RequestResponseLinkWrapper(this.underlyingFactory, entityPath, entityType);
                this.pathToRRLinkMap.put(entityPath, wrapper);
            }
        }
        return wrapper.acquireReferenceAsync();
    }
    
    public void releaseRequestResponseLink(String entityPath)
    {
        RequestResponseLinkWrapper wrapper;
        synchronized (lock)
        {
            wrapper = this.pathToRRLinkMap.get(entityPath);
        }
        if(wrapper != null)
        {
            wrapper.releaseReference();
        }
    }
    
    public CompletableFuture<Void> freeAsync()
    {
        ArrayList<CompletableFuture<Void>> closeFutures = new ArrayList<>();
        for(RequestResponseLinkWrapper wrapper : this.pathToRRLinkMap.values())
        {
            closeFutures.add(wrapper.forceCloseAsync());
        }
        
        this.pathToRRLinkMap.clear();
        return CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture[0]));
    }
    
    private void removeWrapperFromCache(String entityPath)
    {
        synchronized (lock)
        {
            this.pathToRRLinkMap.remove(entityPath);
        }
    }
    
    private class RequestResponseLinkWrapper
    {
        private Object lock = new Object();
        private final MessagingFactory underlyingFactory;
        private final String entityPath;
        private final MessagingEntityType entityType;
        private RequestResponseLink requestResponseLink;
        private int referenceCount;
        private ArrayList<CompletableFuture<RequestResponseLink>> waiters;
        private boolean isClosed;
        
        public RequestResponseLinkWrapper(MessagingFactory underlyingFactory, String entityPath, MessagingEntityType entityType)
        {
            this.underlyingFactory = underlyingFactory;
            this.entityPath = entityPath;
            this.entityType = entityType;
            this.requestResponseLink = null;
            this.referenceCount = 0;
            this.waiters = new ArrayList<>();
            this.isClosed = false;
            this.createRequestResponseLinkAsync();
        }
        
        private void createRequestResponseLinkAsync()
        {
            String requestResponseLinkPath = RequestResponseLink.getManagementNodeLinkPath(this.entityPath);
            String sasTokenAudienceURI = String.format(ClientConstants.SAS_TOKEN_AUDIENCE_FORMAT, this.underlyingFactory.getHostName(), this.entityPath);
            RequestResponseLink.createAsync(this.underlyingFactory, StringUtil.getShortRandomString() + "-RequestResponse", requestResponseLinkPath, sasTokenAudienceURI, this.entityType).handleAsync((rrlink, ex) ->
            {
                synchronized (this.lock)
                {
                    if(ex == null)
                    {
                        if(this.isClosed)
                        {
                        	// Factory is likely closed. Close the link too
                        	rrlink.closeAsync();
                        }
                        else
                        {
                        	this.requestResponseLink = rrlink;
                            this.completeWaiters(null);
                        }
                    }
                    else
                    {
                        Throwable cause = ExceptionUtil.extractAsyncCompletionCause(ex);
                        RequestResponseLinkcache.this.removeWrapperFromCache(this.entityPath);
                        this.completeWaiters(cause);
                    }
                }
                
                return null;
            }, MessagingFactory.INTERNAL_THREAD_POOL);
        }
        
        private void completeWaiters(Throwable exception)
        {
        	for(CompletableFuture<RequestResponseLink> waiter : this.waiters)
            {
        		if(exception == null)
        		{
        			this.referenceCount++;
        			AsyncUtil.completeFuture(waiter, this.requestResponseLink);
        		}
        		else
        		{
        			AsyncUtil.completeFutureExceptionally(waiter, exception);
        		}
            }
        	
        	this.waiters.clear();
        }
        
        public CompletableFuture<RequestResponseLink> acquireReferenceAsync()
        {
            synchronized (this.lock)
            {
                if(this.requestResponseLink == null)
                {
                    CompletableFuture<RequestResponseLink> waiter = new CompletableFuture<>();
                    this.waiters.add(waiter);
                    return waiter;
                }
                else
                {
                    this.referenceCount++;
                    return CompletableFuture.completedFuture(this.requestResponseLink);
                }
            }
        }
        
        public void releaseReference()
        {            
            synchronized (this.lock)
            {
                if(--this.referenceCount == 0)
                {
                    RequestResponseLinkcache.this.removeWrapperFromCache(this.entityPath);
                    this.requestResponseLink.closeAsync();
                }
            }
        }
        
        public CompletableFuture<Void> forceCloseAsync()
        {
            this.isClosed = true;
            if(this.waiters.size() > 0)
            {
            	this.completeWaiters(new ServiceBusException(false, "MessagingFactory closed."));
            }
            
            if(this.requestResponseLink != null)
            {
            	return this.requestResponseLink.closeAsync();
            }
            else
            {
            	return CompletableFuture.completedFuture(null);
            }
        }
    }
}
