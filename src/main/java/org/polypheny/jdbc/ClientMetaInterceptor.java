package org.polypheny.jdbc;

import io.grpc.*;

public class ClientMetaInterceptor implements ClientInterceptor {
    private final String clientUUID;

    public ClientMetaInterceptor(String clientUUID) {
        this.clientUUID = clientUUID;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                headers.put(Metadata.Key.of("clientUUID", Metadata.ASCII_STRING_MARSHALLER), clientUUID);
                super.start(responseListener, headers);
            }
        };
    }
}
