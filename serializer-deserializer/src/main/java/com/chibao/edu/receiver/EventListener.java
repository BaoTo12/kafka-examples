package com.chibao.edu.receiver;

@FunctionalInterface
public interface EventListener {
    void onEvent(ReceivedEvent event);
}
