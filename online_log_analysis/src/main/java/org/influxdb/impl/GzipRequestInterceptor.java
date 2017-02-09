package org.influxdb.impl;

import okhttp3.*;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of a intercepter to compress http's body using GZIP.
 *
 * @author fujian1115 [at] gmail.com
 */
final class GzipRequestInterceptor implements Interceptor {

    private AtomicBoolean enabled = new AtomicBoolean(false);

    GzipRequestInterceptor() {
    }

    public void enable() {
        enabled.set(true);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void disable() {
        enabled.set(false);
    }

    @Override
    public Response intercept(final Interceptor.Chain chain) throws IOException {
        if (!enabled.get()) {
            return chain.proceed(chain.request());
        }

        Request originalRequest = chain.request();
        RequestBody body = originalRequest.body();
        if (body == null || originalRequest.header("Content-Encoding") != null) {
            return chain.proceed(originalRequest);
        }

        Request compressedRequest = originalRequest.newBuilder().header("Content-Encoding", "gzip")
                .method(originalRequest.method(), gzip(body)).build();
        return chain.proceed(compressedRequest);
    }

    private RequestBody gzip(final RequestBody body) {
        return new RequestBody() {
            @Override
            public MediaType contentType() {
                return body.contentType();
            }

            @Override
            public long contentLength() {
                return -1; // We don't know the compressed length in advance!
            }

            @Override
            public void writeTo(final BufferedSink sink) throws IOException {
                BufferedSink gzipSink = Okio.buffer(new GzipSink(sink));
                body.writeTo(gzipSink);
                gzipSink.close();
            }
        };
    }
}
