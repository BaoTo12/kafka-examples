package com.chibao.edu.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public final class SuspendCustomer extends CustomerPayload {
    static final String TYPE = "SUSPEND_CUSTOMER";

    public SuspendCustomer(@JsonProperty("id") UUID id) {
        super(id);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        return SuspendCustomer.class.getSimpleName() + " [" + baseToString() + "]";
    }
}