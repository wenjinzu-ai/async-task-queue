package com.z.asynctaskqueue.service;

import com.z.asynctaskqueue.domain.exception.BusinessException;
import com.z.asynctaskqueue.domain.exception.ErrorCode;
import org.springframework.stereotype.Component;

@Component
public class AcceptanceGate {

    private volatile boolean accepting = true;

    public boolean isAccepting() {
        return accepting;
    }

    public void shutdown() {
        this.accepting = false;
    }

    public void checkAccepting() {
        if (!accepting) {
            throw new BusinessException(ErrorCode.SERVICE_SHUTTING_DOWN, "service is shutting down");
        }
    }
}