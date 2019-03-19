/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.servicebus.amqp;

import org.apache.qpid.proton.amqp.transport.*;
import org.apache.qpid.proton.engine.*;

public class BaseLinkHandler extends BaseHandler {

    private final IAmqpLink underlyingEntity;

    public BaseLinkHandler(final IAmqpLink amqpLink) {
        this.underlyingEntity = amqpLink;
    }

    @Override
    public void onLinkLocalClose(Event event) {
        Link link = event.getLink();

        closeSession(link);
    }

    @Override
    public void onLinkRemoteClose(Event event) {
        final Link link = event.getLink();
        if(link != null)
        {
            if (link.getLocalState() != EndpointState.CLOSED) {
                link.close();
            }

            ErrorCondition condition = link.getRemoteCondition();
            this.processOnClose(link, condition);
            closeSession(link);
        }        
    }

    @Override
    public void onLinkRemoteDetach(Event event) {
        final Link link = event.getLink();
        if(link != null)
        {
            if (link.getLocalState() != EndpointState.CLOSED) {
                link.close();
            }
            
            this.processOnClose(link, link.getRemoteCondition());
            closeSession(link);
        }
        
    }

    public void processOnClose(Link link, ErrorCondition condition) {
        if (condition != null) {
        }

        this.underlyingEntity.onClose(condition);
    }

    public void processOnClose(Link link, Exception exception) {
        this.underlyingEntity.onError(exception);
    }

    private void closeSession(Link link) {
        if (link.getSession() != null && link.getSession().getLocalState() != EndpointState.CLOSED)
            link.getSession().close();
    }
}
