package com.microsoft.azure.servicebus.amqp;

import org.apache.qpid.proton.engine.BaseHandler;
import org.apache.qpid.proton.engine.Event;
import org.apache.qpid.proton.engine.Event.Type;

public class LoggingHandler extends BaseHandler {
    
    @Override
    public void onUnhandled(Event event)
    {
    }   
}
