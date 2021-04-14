/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.zipkin;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Optional;

import brave.Span;
import brave.propagation.TraceContextOrSamplingFlags;
import org.apache.camel.Exchange;

/**
 * The state of the zipkin trace which we store on the {@link Exchange}
 * <p/>
 * This is needed to keep track of of correlating when an existing span is calling downstream service(s) and therefore
 * must be able to correlate those service calls with the parent span.
 */
public final class ZipkinState {

    public static final String KEY = "CamelZipkinState";

    private final Deque<Span> clientSpans = new ArrayDeque<>();
    private final Deque<Span> serverSpans = new ArrayDeque<>();

    public synchronized void pushClientSpan(Span span) {
        clientSpans.push(span);
    }

    public synchronized Span popClientSpan(Exchange exchange) {
        Span span = popClientSpan(exchange, clientSpans);
        updateExchangeZipkinHeaders(exchange, span, clientSpans);
        return span;
    }

    private void updateExchangeZipkinHeaders(Exchange exchange, Span span, Deque<Span> spans) {
        if (span.context().parentId() != null) {
            Optional<Span> parent = findParent(spans, span);
            parent.ifPresent(parentSpan -> {
                exchange.getIn().setHeader(ZipkinConstants.SPAN_ID, parentSpan.context().spanIdString());
                exchange.getIn().setHeader(ZipkinConstants.PARENT_SPAN_ID, parentSpan.context().parentIdString());
            });

        }
    }

    private Span popClientSpan(Exchange exchange, Deque<Span> spans) {
        if (!spans.isEmpty()) {
            String spanId = (String) exchange.getIn().getHeader(ZipkinConstants.SPAN_ID);
            Span lastSpan = peekSpan(spans);
            if (spanId == null) {
                return spans.pop();
            }

            TraceContextOrSamplingFlags traceContext
                    = ZipkinTracer.EXTRACTOR.extract(new CamelRequest(exchange.getIn(), Span.Kind.CLIENT));
            if (traceContext.context().spanId() == lastSpan.context().spanId()) {
                return spans.pop();
            }
            Iterator<Span> spanItr = spans.iterator();
            while (spanItr.hasNext()) {
                Span span = spanItr.next();
                if (span.context().spanId() == traceContext.context().spanId()) {
                    spanItr.remove();
                    return span;
                }
            }
            return spans.pop();

        } else {
            return null;
        }
    }

    private Optional<Span> findParent(Deque<Span> spans, Span child) {
        return spans.stream().filter(span -> span.context().spanId() == child.context().parentIdAsLong()).findFirst();

    }

    public synchronized void pushServerSpan(Span span) {
        serverSpans.push(span);
    }

    public synchronized Span popServerSpan(Exchange exchange) {
        Span span = popServerSpan(exchange, serverSpans);
        updateExchangeZipkinHeaders(exchange, span, serverSpans);
        return span;
    }

    private Span popServerSpan(Exchange exchange, Deque<Span> spans) {
        if (!spans.isEmpty()) {
            String parentSpanId = (String) exchange.getIn().getHeader(ZipkinConstants.PARENT_SPAN_ID);
            Span lastSpan = peekSpan(spans);
            if (parentSpanId == null) {
                return spans.pop();
            }

            TraceContextOrSamplingFlags traceContext
                    = ZipkinTracer.EXTRACTOR.extract(new CamelRequest(exchange.getIn(), Span.Kind.SERVER));
            if (traceContext.context().parentId() == null) {
                return spans.pop();
            }
            if (traceContext.context().parentId() == lastSpan.context().spanId()) {
                return spans.pop();
            }
            Iterator<Span> spanItr = spans.iterator();
            while (spanItr.hasNext()) {
                Span span = spanItr.next();
                if (span.context().spanId() == traceContext.context().parentId()) {
                    spanItr.remove();
                    return span;
                }
            }
            return spans.pop();

        } else {
            return null;
        }
    }

    private Span peekSpan(Deque<Span> spans) {
        if (!spans.isEmpty()) {
            return spans.peek();
        } else {
            return null;
        }
    }

    public synchronized Span findMatchingServerSpan(Exchange exchange) {
        String spanId = (String) exchange.getIn().getHeader(ZipkinConstants.SPAN_ID);
        Span lastSpan = peekSpan(serverSpans);
        if (spanId == null) {
            return lastSpan;
        }
        TraceContextOrSamplingFlags traceContext
                = ZipkinTracer.EXTRACTOR.extract(new CamelRequest(exchange.getIn(), Span.Kind.SERVER));
        if (traceContext.context().spanId() == lastSpan.context().spanId()) {
            return lastSpan;
        }

        Optional<Span> matchingSpan
                = serverSpans.stream().filter(span -> span.context().spanId() == traceContext.context().spanId()).findFirst();
        return matchingSpan.orElse(lastSpan);
    }

}
