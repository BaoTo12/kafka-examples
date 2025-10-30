package com.chibao.edu.receiver;

public class ConsumerBusinessLogic {
    public ConsumerBusinessLogic(EventReceiver eventReceiver){
        eventReceiver.addListener(this::onEvent);
    }
    private void onEvent(ReceivedEvent event) {
        if (! event.isError()) {
            System.out.format("Received %s%n", event.getPayload());
        } else {
            System.err.format("Error in record %s: %s%n", event.getRecord(), event.getError());
        }
    }
}
