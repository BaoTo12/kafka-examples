package com.chibao.edu.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public final class ReinstateCustomer extends CustomerPayload {
    static final String TYPE = "REINSTATE_CUSTOMER";

    public ReinstateCustomer(@JsonProperty("id") UUID id) {
        super(id);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String toString() {
        return ReinstateCustomer.class.getSimpleName() + " [" + baseToString() + "]";
    }
}