package com.benchmark.distributed.security;

import io.grpc.*;

public class JwtServerInterceptor implements ServerInterceptor {

    // Metadata key for the Authorization header
    public static final Metadata.Key<String> AUTH_HEADER = Metadata.Key.of("authorization",
            Metadata.ASCII_STRING_MARSHALLER);

    // The authenticate RPC does not require a token (it's how you GET one)
    private static final String AUTH_METHOD_DB = "com.benchmark.distributed.grpc.DatabaseService/Authenticate";
    private static final String AUTH_METHOD_API = "com.benchmark.distributed.grpc.ApiService/Authenticate";

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata headers,
            ServerCallHandler<ReqT, RespT> next) {

        String fullMethodName = call.getMethodDescriptor().getFullMethodName();

        // Skip JWT check for the authenticate RPC itself
        if (AUTH_METHOD_DB.equals(fullMethodName) || AUTH_METHOD_API.equals(fullMethodName)) {
            return next.startCall(call, headers);
        }

        // Extract Authorization header
        String authHeader = headers.get(AUTH_HEADER);
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            call.close(Status.UNAUTHENTICATED.withDescription("Missing or malformed Authorization header"),
                    new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }

        String token = authHeader.substring(7); // Strip "Bearer "
        try {
            var claims = JwtUtil.validateToken(token);
            System.out.println("JWT validated for: " + claims.getSubject());
            return next.startCall(call, headers);
        } catch (Exception e) {
            call.close(Status.UNAUTHENTICATED.withDescription("Invalid or expired JWT token: " + e.getMessage()),
                    new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }
    }
}
