package com.chibao.edu.receiver;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractReceiver implements EventReceiver {
    private final Set<EventListener> listeners = new HashSet<>();

    public final void addListener(EventListener listener) {
        listeners.add(listener);
    }

    protected final void fire(ReceivedEvent event) {
        for (var listener: listeners){
            listener.onEvent(event);
        }
    }
}
