package com.z.asynctaskqueue.api;

import com.z.asynctaskqueue.api.dto.ErrorResponse;
import com.z.asynctaskqueue.domain.exception.BusinessException;
import com.z.asynctaskqueue.domain.exception.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(BusinessException.class)
    public ResponseEntity<ErrorResponse> handleBusiness(BusinessException ex) {
        ErrorResponse body = ErrorResponse.of(ex.getCode(), ex.getMessage(), ex.getTaskId());
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(ex.getCode().getHttpStatus());
        if (ex.getCode() == ErrorCode.QUEUE_FULL) {
            int retryAfterSeconds = 1 + ThreadLocalRandom.current().nextInt(2);
            builder.header("Retry-After", String.valueOf(retryAfterSeconds));
        }
        return builder.body(body);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex) {
        String message = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + " " + fe.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(ErrorResponse.of(ErrorCode.PARAM_INVALID, message, null));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleAll(Exception ex) {
        log.error("unhandled exception", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ErrorResponse.of(ErrorCode.INTERNAL_ERROR, "internal error", null));
    }
}