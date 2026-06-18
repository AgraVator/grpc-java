/*
 * Copyright 2026 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.udpa.udpa.type.v1.TypedStruct;
import com.github.xds.type.matcher.v3.Matcher;
import com.github.xds.type.matcher.v3.StringMatcher;
import com.google.protobuf.Any;
import io.envoyproxy.envoy.config.core.v3.RuntimeFractionalPercent;
import io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.FilterChainConfiguration;
import io.envoyproxy.envoy.extensions.matching.common_inputs.network.v3.ServerNameInput;
import io.envoyproxy.envoy.type.matcher.v3.HttpRequestHeaderMatchInput;
import io.envoyproxy.envoy.type.v3.FractionalPercent;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.MetricRecorder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.xds.Filter.FilterConfig;
import io.grpc.xds.Filter.FilterConfigParseContext;
import io.grpc.xds.Filter.FilterContext;
import io.grpc.xds.client.Bootstrapper;
import io.grpc.xds.client.EnvoyProtoData;
import io.grpc.xds.internal.matcher.UnifiedMatcher;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class CompositeFilterTest {

  private static final String FAKE_TYPE_URL = "type.googleapis.com/fake";
  private static final String FAKE_UNSUPPORTED_TYPE_URL = "type.googleapis.com/fake.unsupported";

  private CompositeFilter.Provider provider;

  @Mock
  private Filter.Provider fakeProvider;
  @Mock
  private Filter.Provider fakeUnsupportedProvider;
  @Mock
  private Filter fakeFilter;
  @Mock
  private ClientInterceptor fakeClientInterceptor;
  @Mock
  private ServerInterceptor fakeServerInterceptor;
  @Mock
  private FilterConfig fakeConfig;

  @Before
  @SuppressWarnings("deprecation")
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");

    when(fakeProvider.typeUrls()).thenReturn(new String[]{FAKE_TYPE_URL});
    when(fakeProvider.isClientFilter()).thenReturn(true);
    when(fakeProvider.isServerFilter()).thenReturn(true);
    ConfigOrError<? extends FilterConfig> configRes = ConfigOrError.fromConfig(fakeConfig);
    when(fakeProvider.parseFilterConfig(any(com.google.protobuf.Message.class), any()))
        .thenReturn((ConfigOrError) configRes);
    when(fakeProvider.newInstance(any(FilterContext.class))).thenReturn(fakeFilter);
    when(fakeFilter.buildClientInterceptor(any(), any(), any())).thenReturn(fakeClientInterceptor);
    when(fakeFilter.buildServerInterceptor(any(), any())).thenReturn(fakeServerInterceptor);

    when(fakeUnsupportedProvider.typeUrls()).thenReturn(new String[]{FAKE_UNSUPPORTED_TYPE_URL});
    when(fakeUnsupportedProvider.isClientFilter()).thenReturn(false);
    when(fakeUnsupportedProvider.isServerFilter()).thenReturn(false);
    when(fakeUnsupportedProvider.parseFilterConfig(
        any(com.google.protobuf.Message.class), any()))
        .thenReturn((ConfigOrError) configRes);

    provider = new CompositeFilter.Provider(typeUrl -> {
      if (FAKE_TYPE_URL.equals(typeUrl)) {
        return fakeProvider;
      }
      if (FAKE_UNSUPPORTED_TYPE_URL.equals(typeUrl)) {
        return fakeUnsupportedProvider;
      }
      return FilterRegistry.getDefaultRegistry().get(typeUrl);
    });
  }

  @After
  public void tearDown() {
    System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
  }

  private FilterConfigParseContext getFilterContext() {
    return FilterConfigParseContext.builder()
        .bootstrapInfo(Bootstrapper.BootstrapInfo.builder()
            .servers(Collections.singletonList(
                Bootstrapper.ServerInfo.create("test_target", Collections.emptyMap())))
            .node(EnvoyProtoData.Node.newBuilder().build())
            .build())
        .serverInfo(Bootstrapper.ServerInfo.create(
            "test_target", Collections.emptyMap(), false, true, false, false))
        .build();
  }

  private CompositeFilter newFilter(String name) {
    return (CompositeFilter) provider.newInstance(
        FilterContext.create(name, mock(MetricRecorder.class)));
  }

  private static ExtensionWithMatcher createExtensionWithMatcher(Matcher matcher) {
    return ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(TypedExtensionConfig.newBuilder()
            .setName("composite")
            .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
            .build())
        .setXdsMatcher(matcher)
        .build();
  }

  private MethodDescriptor<Void, Void> createMockMethod() {
    MethodDescriptor.Marshaller<Void> marshaller = mock(MethodDescriptor.Marshaller.class);
    return MethodDescriptor.<Void, Void>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName("service/method")
        .setRequestMarshaller(marshaller)
        .setResponseMarshaller(marshaller)
        .build();
  }

  private static Matcher.OnMatch createExecuteAction(String childName, String typeUrl) {
    return Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_" + childName)
            .setTypedConfig(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                    + ".ExecuteFilterAction")
                .setValue(ExecuteFilterAction.newBuilder()
                    .setTypedConfig(TypedExtensionConfig.newBuilder()
                        .setName(childName)
                        .setTypedConfig(Any.newBuilder()
                            .setTypeUrl(typeUrl)
                            .setValue(Composite.getDefaultInstance().toByteString())
                            .build())
                        .build())
                    .build().toByteString())
                .build())
            .build())
        .build();
  }

  private static Matcher.MatcherList.FieldMatcher createHeaderFieldMatcher(
      String headerName, String headerValue, Matcher.OnMatch onMatch) {
    return Matcher.MatcherList.FieldMatcher.newBuilder()
        .setPredicate(Matcher.MatcherList.Predicate.newBuilder()
            .setSinglePredicate(Matcher.MatcherList.Predicate.SinglePredicate.newBuilder()
                .setInput(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                    .setName("request_headers")
                    .setTypedConfig(Any.pack(HttpRequestHeaderMatchInput.newBuilder()
                        .setHeaderName(headerName)
                        .build()))
                    .build())
                .setValueMatch(StringMatcher.newBuilder().setExact(headerValue).build())
                .build())
            .build())
        .setOnMatch(onMatch)
        .build();
  }

  @Test
  public void providerMethodsCovered() {
    assertThat(provider.typeUrls()).asList().containsExactly(
        CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER,
        CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE
    );
    assertThat(provider.isClientFilter()).isTrue();
    assertThat(provider.isServerFilter()).isTrue();
  }

  @Test
  public void parseFilterConfig_success() {
    Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    assertThat(result.errorDetail).isNull();
    assertThat(result.config).isNotNull();
    assertThat(result.config.matcher).isNotNull();
    assertThat(result.config.delegates).containsKey("action_child");
  }

  @Test
  public void parseFilterConfig_bareCompositeFails() {
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(Composite.getDefaultInstance()), getFilterContext());

    assertThat(result.errorDetail).contains("Expected ExtensionWithMatcher but got");
  }

  @Test
  public void parseFilterConfig_missingCompositeInExtensionConfigFails() {
    ExtensionWithMatcher proto = ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(TypedExtensionConfig.newBuilder().setName("composite").build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    assertThat(result.errorDetail).contains(
        "ExtensionWithMatcher.extension_config must contain an empty Composite proto");
  }

  @Test
  public void parseFilterConfig_emptyConfigWithExtensionWithMatcherSucceeds() {
    ExtensionWithMatcher protoNoMatcher = ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(TypedExtensionConfig.newBuilder()
            .setName("composite")
            .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
            .build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result1 =
        provider.parseFilterConfig(Any.pack(protoNoMatcher), getFilterContext());

    assertThat(result1.errorDetail).isNull();
    assertThat(result1.config).isNotNull();
    assertThat(result1.config.matcher).isNull();

    ExtensionWithMatcher protoEmptyMatcher =
        createExtensionWithMatcher(Matcher.getDefaultInstance());

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result2 =
        provider.parseFilterConfig(Any.pack(protoEmptyMatcher), getFilterContext());

    assertThat(result2.errorDetail).isNull();
    assertThat(result2.config).isNotNull();
    assertThat(result2.config.matcher).isNotNull();
  }

  @Test
  public void parseFilterConfig_failsWhenDisabled() {
    System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    try {
      ExtensionWithMatcher proto = createExtensionWithMatcher(Matcher.getDefaultInstance());
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfig(Any.pack(proto), getFilterContext());
      assertThat(result.errorDetail).contains("Composite Filter is experimental");
    } finally {
      System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
    }
  }

  @Test
  public void parseFilterConfig_invalidMessageType() {
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(com.google.protobuf.Empty.getDefaultInstance(),
            getFilterContext());

    assertThat(result.errorDetail).contains("Invalid message type");
  }

  @Test
  public void parseFilterConfig_invalidProtoBytes() {
    Any invalidAny = Any.newBuilder()
        .setTypeUrl(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER)
        .setValue(com.google.protobuf.ByteString.copyFrom(new byte[]{(byte) 0x80}))
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(invalidAny, getFilterContext());

    assertThat(result.errorDetail).contains("Invalid proto:");
  }

  @Test
  public void parseFilterConfig_exceedsRecursionLimit() {
    Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(Matcher.MatcherList.FieldMatcher.newBuilder()
                .setOnMatch(matchAction)
                .build())
            .build())
        .build();

    final Any configAny = Any.pack(createExtensionWithMatcher(matcher));

    when(fakeProvider.parseFilterConfig(any(), any()))
        .thenAnswer(invocation -> {
          FilterConfigParseContext context = invocation.getArgument(1);
          int depth = context.recursionDepth() != null ? context.recursionDepth() : 0;
          FilterConfigParseContext childContext = context.toBuilder()
              .recursionDepth(depth + 1)
              .build();
          return provider.parseFilterConfig(configAny, childContext);
        });

    ConfigOrError<? extends FilterConfig> result =
        provider.parseFilterConfig(configAny, getFilterContext());

    assertThat(result.errorDetail).contains("Maximum recursion depth of 8 exceeded");
  }

  @Test
  public void parseFilterConfig_keepMatchingFailsValidation() {
    Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
        .setKeepMatching(true)
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action")
            .setTypedConfig(Any.newBuilder().setTypeUrl(FAKE_TYPE_URL).build())
            .build())
        .build();

    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    assertThat(result.errorDetail).contains(
        "keep_matching is not permitted anywhere in the composite filter matcher tree");
  }

  @Test
  public void parseFilterConfig_nestedTerminalFilterFailsValidation() {
    Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("terminal_action")
            .setTypedConfig(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                    + ".ExecuteFilterAction")
                .setValue(ExecuteFilterAction.newBuilder()
                    .setTypedConfig(TypedExtensionConfig.newBuilder()
                        .setName("router")
                        .setTypedConfig(Any.newBuilder()
                            .setTypeUrl(RouterFilter.TYPE_URL)
                            .build())
                        .build())
                    .build().toByteString())
                .build())
            .build())
        .build();

    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    assertThat(result.errorDetail).contains(
        "Nested filter cannot be a terminal filter (RouterFilter)");
  }

  @Test
  public void parseFilterConfig_withUdpaTypedStruct() {
    com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder()
        .putFields("foo", com.google.protobuf.Value.newBuilder().setStringValue("bar").build())
        .build();

    TypedStruct udpaTypedStruct = TypedStruct.newBuilder()
        .setTypeUrl(FAKE_TYPE_URL)
        .setValue(struct)
        .build();

    ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
        .setTypedConfig(TypedExtensionConfig.newBuilder()
            .setName("child")
            .setTypedConfig(Any.pack(udpaTypedStruct))
            .build())
        .build();

    Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action")
            .setTypedConfig(Any.pack(action))
            .build())
        .build();

    Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();
    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    assertThat(result.errorDetail).isNull();
    verify(fakeProvider).parseFilterConfig(eq(struct), any());
  }

  @Test
  public void parseFilterConfig_withXdsTypedStruct() {
    com.google.protobuf.Struct struct = com.google.protobuf.Struct.newBuilder()
        .putFields("foo", com.google.protobuf.Value.newBuilder().setStringValue("bar").build())
        .build();

    com.github.xds.type.v3.TypedStruct xdsTypedStruct =
        com.github.xds.type.v3.TypedStruct.newBuilder()
            .setTypeUrl(FAKE_TYPE_URL)
            .setValue(struct)
            .build();

    ExecuteFilterAction action = ExecuteFilterAction.newBuilder()
        .setTypedConfig(TypedExtensionConfig.newBuilder()
            .setName("child")
            .setTypedConfig(Any.pack(xdsTypedStruct))
            .build())
        .build();

    Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action")
            .setTypedConfig(Any.pack(action))
            .build())
        .build();

    Matcher matcher = Matcher.newBuilder().setOnNoMatch(matchAction).build();
    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    assertThat(result.errorDetail).isNull();
    verify(fakeProvider).parseFilterConfig(eq(struct), any());
  }

  @Test
  public void parseFilterConfig_rejectsOverrideMessage() {
    ExtensionWithMatcherPerRoute overrideProto = ExtensionWithMatcherPerRoute.newBuilder()
        .setXdsMatcher(Matcher.newBuilder().build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(overrideProto), getFilterContext());

    assertThat(result.errorDetail).contains("Expected ExtensionWithMatcher but got");
  }

  @Test
  public void parseFilterConfigOverride_rejectsConfigMessage() {
    ExtensionWithMatcher configProto = createExtensionWithMatcher(Matcher.newBuilder().build());

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfigOverride(Any.pack(configProto), getFilterContext());

    assertThat(result.errorDetail).contains("Expected ExtensionWithMatcherPerRoute but got");
  }

  @Test
  public void parseFilterConfigOverride_invalidMessageType() {
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfigOverride(
            com.google.protobuf.Empty.getDefaultInstance(), getFilterContext());

    assertThat(result.errorDetail).contains("Invalid message type");
  }

  @Test
  public void parseFilterConfigOverride_invalidProtoBytes() {
    Any invalidAny = Any.newBuilder()
        .setTypeUrl(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE)
        .setValue(com.google.protobuf.ByteString.copyFrom(new byte[]{(byte) 0x80}))
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfigOverride(invalidAny, getFilterContext());

    assertThat(result.errorDetail).contains("Invalid proto:");
  }

  @Test
  public void parseFilterConfigOverride_failsWhenDisabled() {
    System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    try {
      ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
          provider.parseFilterConfigOverride(
              Any.pack(ExtensionWithMatcherPerRoute.getDefaultInstance()), getFilterContext());
      assertThat(result.errorDetail).contains("Composite Filter is experimental");
    } finally {
      System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
    }
  }

  @Test
  public void clientInterceptor_delegatesToMatchedChild() {
    Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    ClientCall childCall = mock(ClientCall.class);
    when(fakeClientInterceptor.interceptCall(any(), any(), any())).thenReturn(childCall);

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    call.start(mock(ClientCall.Listener.class), headers);

    verify(fakeClientInterceptor).interceptCall(any(), any(), any());
    verify(childCall).start(any(), eq(headers));
  }

  @Test
  public void clientInterceptor_noMatchFailsWithUnavailable() {
    Matcher matcher = Matcher.newBuilder().build();
    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
    Metadata headers = new Metadata();
    call.start(listener, headers);

    verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
    verify(next, never()).newCall(any(), any());

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(statusCaptor.getValue().getDescription())
        .contains("no match found in matcher tree");
  }

  @Test
  public void clientInterceptor_delegatesChain() {
    TypedExtensionConfig child1 = TypedExtensionConfig.newBuilder()
        .setName("child1")
        .setTypedConfig(Any.newBuilder()
            .setTypeUrl(FAKE_TYPE_URL)
            .setValue(Composite.getDefaultInstance().toByteString())
            .build())
        .build();
    TypedExtensionConfig child2 = TypedExtensionConfig.newBuilder()
        .setName("child2")
        .setTypedConfig(Any.newBuilder()
            .setTypeUrl(FAKE_TYPE_URL)
            .setValue(Composite.getDefaultInstance().toByteString())
            .build())
        .build();

    FilterChainConfiguration filterChain = FilterChainConfiguration.newBuilder()
        .addTypedConfig(child1)
        .addTypedConfig(child2)
        .build();

    Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_chain")
            .setTypedConfig(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                    + ".ExecuteFilterAction")
                .setValue(ExecuteFilterAction.newBuilder()
                    .setFilterChain(filterChain)
                    .build().toByteString())
                .build())
            .build())
        .build();

    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    ClientCall nextCall = mock(ClientCall.class);
    when(next.newCall(any(), any())).thenReturn(nextCall);

    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    fakeClientInterceptor = mock(ClientInterceptor.class);
    when(fakeFilter.buildClientInterceptor(any(), any(), any())).thenReturn(fakeClientInterceptor);

    org.mockito.Mockito.doAnswer(invocation -> {
      Channel nextArg = (Channel) invocation.getArguments()[2];
      return nextArg.newCall(
          (MethodDescriptor<?, ?>) invocation.getArguments()[0],
          (CallOptions) invocation.getArguments()[1]);
    }).when(fakeClientInterceptor).interceptCall(any(), any(), any());

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    call.start(mock(ClientCall.Listener.class), headers);

    verify(fakeFilter, times(2)).buildClientInterceptor(any(), any(), any());
    verify(fakeClientInterceptor, times(2)).interceptCall(any(), any(), any());
  }

  @Test
  public void clientInterceptor_serverNameInputMatch() {
    Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);

    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(Matcher.MatcherList.FieldMatcher.newBuilder()
                .setPredicate(Matcher.MatcherList.Predicate.newBuilder()
                    .setSinglePredicate(Matcher.MatcherList.Predicate.SinglePredicate.newBuilder()
                        .setInput(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                            .setName("server_name")
                            .setTypedConfig(Any.pack(ServerNameInput.getDefaultInstance()))
                            .build())
                        .setValueMatch(StringMatcher.newBuilder().setExact("foo.com").build())
                        .build())
                    .build())
                .setOnMatch(matchAction)
                .build())
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    MethodDescriptor<Void, Void> method = createMockMethod();
    CallOptions callOptions = CallOptions.DEFAULT.withAuthority("foo.com");

    ClientCall<Void, Void> call = interceptor.interceptCall(method, callOptions, next);

    ClientCall childCall = mock(ClientCall.class);
    when(fakeClientInterceptor.interceptCall(any(), any(), any())).thenReturn(childCall);

    Metadata headers = new Metadata();
    call.start(mock(ClientCall.Listener.class), headers);

    verify(fakeClientInterceptor).interceptCall(any(), any(), any());
    verify(childCall).start(any(), eq(headers));
  }

  @Test
  public void clientInterceptor_samplePercentAlwaysFalse_skipsFilter() {
    Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_sampled")
            .setTypedConfig(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                    + ".ExecuteFilterAction")
                .setValue(ExecuteFilterAction.newBuilder()
                    .setTypedConfig(TypedExtensionConfig.newBuilder()
                        .setName("child")
                        .setTypedConfig(Any.newBuilder()
                            .setTypeUrl(FAKE_TYPE_URL)
                            .setValue(Composite.getDefaultInstance().toByteString())
                            .build())
                        .build())
                    .setSamplePercent(RuntimeFractionalPercent.newBuilder()
                        .setDefaultValue(FractionalPercent.newBuilder()
                            .setNumerator(0)
                            .setDenominator(FractionalPercent.DenominatorType.HUNDRED)
                            .build())
                        .build())
                    .build().toByteString())
                .build())
            .build())
        .build();

    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    ClientCall nextCall = mock(ClientCall.class);
    when(next.newCall(any(), any())).thenReturn(nextCall);

    MethodDescriptor<Void, Void> method = createMockMethod();
    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);
    call.start(mock(ClientCall.Listener.class), headers);

    verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
    verify(next).newCall(any(), any());
    verify(nextCall).start(any(), eq(headers));
  }

  @Test
  public void clientInterceptor_skipsOnSkipFilter() {
    Any skipActionAny = Any.newBuilder()
        .setTypeUrl("type.googleapis.com/envoy.extensions.filters.common.matcher.action.v3"
            + ".SkipFilter")
        .setValue(com.google.protobuf.ByteString.EMPTY)
        .build();

    Matcher.OnMatch matchAction = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_skip")
            .setTypedConfig(skipActionAny)
            .build())
        .build();

    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    ClientCall nextCall = mock(ClientCall.class);
    when(next.newCall(any(), any())).thenReturn(nextCall);

    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    call.start(mock(ClientCall.Listener.class), headers);

    verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
    verify(next).newCall(any(), any());
    verify(nextCall).start(any(), eq(headers));
  }

  @Test
  public void clientInterceptor_closesFiltersOnClose() {
    Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    ClientCall childCall = mock(ClientCall.class);
    when(fakeClientInterceptor.interceptCall(any(), any(), any())).thenReturn(childCall);

    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    ClientCall.Listener responseListener = mock(ClientCall.Listener.class);
    call.start(responseListener, headers);

    ArgumentCaptor<ClientCall.Listener> listenerCaptor =
        ArgumentCaptor.forClass(ClientCall.Listener.class);
    verify(childCall).start(listenerCaptor.capture(), eq(headers));

    ClientCall.Listener capturedListener = listenerCaptor.getValue();
    capturedListener.onClose(Status.OK, new Metadata());

    verify(fakeFilter).close();
  }

  @Test
  public void clientInterceptor_unsupportedSideChildFilterFailsWithUnavailable() {
    Matcher.OnMatch matchAction = createExecuteAction("unsupported", FAKE_UNSUPPORTED_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    call.start(listener, headers);

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(statusCaptor.getValue().getDescription())
        .contains("not supported on client side");
  }

  @Test
  public void clientInterceptor_callMethodsThrowBeforeStart() {
    CompositeFilter filter = newFilter("composite");
    UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
    ClientInterceptor interceptor = filter.buildClientInterceptor(
        new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap()), null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    try {
      call.request(1);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).isEqualTo("Not started");
    }

    try {
      call.halfClose();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).isEqualTo("Not started");
    }

    try {
      call.sendMessage(null);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).isEqualTo("Not started");
    }

    try {
      call.setMessageCompression(true);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).isEqualTo("Not started");
    }

    // Cancel before start is allowed and records cancel status
    call.cancel("cancelled before start", null);

    ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
    call.start(listener, new Metadata());

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.CANCELLED);
    assertThat(statusCaptor.getValue().getDescription()).contains("cancelled before start");
  }

  @Test
  public void clientInterceptor_usesOverrideMatcher_overrideMatches() {
    Matcher.OnMatch baseAction = createExecuteAction("child", FAKE_TYPE_URL);
    Matcher baseMatcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", baseAction))
            .build())
        .build();
    ExtensionWithMatcher baseProto = createExtensionWithMatcher(baseMatcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> baseResult =
        provider.parseFilterConfig(Any.pack(baseProto), getFilterContext());

    Any skipActionAny = Any.newBuilder()
        .setTypeUrl("type.googleapis.com/envoy.extensions.filters.common.matcher.action.v3"
            + ".SkipFilter")
        .setValue(com.google.protobuf.ByteString.EMPTY)
        .build();
    Matcher.OnMatch overrideAction = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("override_skip")
            .setTypedConfig(skipActionAny)
            .build())
        .build();
    Matcher overrideMatcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", overrideAction))
            .build())
        .build();
    ExtensionWithMatcherPerRoute overrideProto = ExtensionWithMatcherPerRoute.newBuilder()
        .setXdsMatcher(overrideMatcher)
        .build();
    ConfigOrError<CompositeFilter.CompositeFilterConfig> overrideResult =
        provider.parseFilterConfigOverride(Any.pack(overrideProto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(
        baseResult.config, overrideResult.config, mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    ClientCall nextCall = mock(ClientCall.class);
    when(next.newCall(any(), any())).thenReturn(nextCall);

    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");
    call.start(mock(ClientCall.Listener.class), headers);

    verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
    verify(next).newCall(any(), any());
    verify(nextCall).start(any(), eq(headers));
  }

  @Test
  public void clientInterceptor_usesOverrideMatcher_overrideNoMatchFails() {
    Matcher.OnMatch baseAction = createExecuteAction("child", FAKE_TYPE_URL);
    Matcher baseMatcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", baseAction))
            .build())
        .build();
    ExtensionWithMatcher baseProto = createExtensionWithMatcher(baseMatcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> baseResult =
        provider.parseFilterConfig(Any.pack(baseProto), getFilterContext());

    // Override matcher has no match rules
    Matcher overrideMatcher = Matcher.newBuilder().build();
    ExtensionWithMatcherPerRoute overrideProto = ExtensionWithMatcherPerRoute.newBuilder()
        .setXdsMatcher(overrideMatcher)
        .build();
    ConfigOrError<CompositeFilter.CompositeFilterConfig> overrideResult =
        provider.parseFilterConfigOverride(Any.pack(overrideProto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ClientInterceptor interceptor = filter.buildClientInterceptor(
        baseResult.config, overrideResult.config, mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");
    call.start(listener, headers);

    verify(fakeClientInterceptor, never()).interceptCall(any(), any(), any());
    verify(next, never()).newCall(any(), any());
    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }

  @Test
  public void serverInterceptor_delegatesToMatchedChild() {
    Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

    ServerCall call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
    when(call.getMethodDescriptor()).thenReturn(createMockMethod());

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    ServerCallHandler next = mock(ServerCallHandler.class);
    ServerCall.Listener listener = mock(ServerCall.Listener.class);
    when(next.startCall(any(), any())).thenReturn(listener);

    interceptor.interceptCall(call, headers, next);

    verify(fakeServerInterceptor).interceptCall(eq(call), eq(headers), any());
  }

  @Test
  public void serverInterceptor_noMatchFailsWithUnavailable() {
    Matcher matcher = Matcher.newBuilder().build();
    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

    ServerCall call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
    when(call.getMethodDescriptor()).thenReturn(createMockMethod());

    ServerCallHandler next = mock(ServerCallHandler.class);
    Metadata headers = new Metadata();

    interceptor.interceptCall(call, headers, next);

    verify(fakeServerInterceptor, never()).interceptCall(any(), any(), any());
    verify(next, never()).startCall(any(), any());

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(call).close(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(statusCaptor.getValue().getDescription())
        .contains("no match found in matcher tree");
  }

  @Test
  public void serverInterceptor_unsupportedSideChildFilterFailsWithUnavailable() {
    Matcher.OnMatch matchAction = createExecuteAction("unsupported", FAKE_UNSUPPORTED_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

    ServerCall call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
    when(call.getMethodDescriptor()).thenReturn(createMockMethod());

    ServerCallHandler next = mock(ServerCallHandler.class);
    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    interceptor.interceptCall(call, headers, next);

    verify(fakeServerInterceptor, never()).interceptCall(any(), any(), any());
    verify(next, never()).startCall(any(), any());

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(call).close(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
    assertThat(statusCaptor.getValue().getDescription())
        .contains("not supported on server side");
  }

  @Test
  public void serverInterceptor_closesFiltersOnCancelAndComplete() {
    Matcher.OnMatch matchAction = createExecuteAction("child", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    ServerInterceptor interceptor = filter.buildServerInterceptor(result.config, null);

    ServerCall call = mock(ServerCall.class);
    when(call.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
    when(call.getMethodDescriptor()).thenReturn(createMockMethod());

    ServerCall.Listener innerListener = mock(ServerCall.Listener.class);
    when(fakeServerInterceptor.interceptCall(any(), any(), any())).thenAnswer(invocation -> {
      ServerCallHandler handler = invocation.getArgument(2);
      return handler.startCall((ServerCall) invocation.getArgument(0),
          (Metadata) invocation.getArgument(1));
    });

    ServerCallHandler next = mock(ServerCallHandler.class);
    when(next.startCall(any(), any())).thenReturn(innerListener);

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    ServerCall.Listener listener = interceptor.interceptCall(call, headers, next);

    listener.onComplete();
    verify(fakeFilter).close();

    listener.onCancel();
    verify(fakeFilter, times(2)).close();
  }

  @Test
  public void samplePercentDeterministic() {
    FractionalPercent percent = FractionalPercent.newBuilder()
        .setNumerator(50)
        .setDenominator(FractionalPercent.DenominatorType.HUNDRED)
        .build();

    ThreadSafeRandom mockRandom = mock(ThreadSafeRandom.class);
    CompositeFilter.FilterDelegate delegate = new CompositeFilter.FilterDelegate(
        Collections.emptyList(), percent, mockRandom);

    when(mockRandom.nextDouble()).thenReturn(0.4);
    assertThat(delegate.shouldExecute()).isTrue();

    when(mockRandom.nextDouble()).thenReturn(0.6);
    assertThat(delegate.shouldExecute()).isFalse();
  }

  @Test
  public void samplePercentTenThousand() {
    FractionalPercent percent = FractionalPercent.newBuilder()
        .setNumerator(5000)
        .setDenominator(FractionalPercent.DenominatorType.TEN_THOUSAND)
        .build();

    ThreadSafeRandom mockRandom = mock(ThreadSafeRandom.class);
    CompositeFilter.FilterDelegate delegate = new CompositeFilter.FilterDelegate(
        Collections.emptyList(), percent, mockRandom);

    when(mockRandom.nextDouble()).thenReturn(0.4);
    assertThat(delegate.shouldExecute()).isTrue();

    when(mockRandom.nextDouble()).thenReturn(0.6);
    assertThat(delegate.shouldExecute()).isFalse();
  }

  @Test
  public void samplePercentMillion() {
    FractionalPercent percent = FractionalPercent.newBuilder()
        .setNumerator(500000)
        .setDenominator(FractionalPercent.DenominatorType.MILLION)
        .build();

    ThreadSafeRandom mockRandom = mock(ThreadSafeRandom.class);
    CompositeFilter.FilterDelegate delegate = new CompositeFilter.FilterDelegate(
        Collections.emptyList(), percent, mockRandom);

    when(mockRandom.nextDouble()).thenReturn(0.4);
    assertThat(delegate.shouldExecute()).isTrue();

    when(mockRandom.nextDouble()).thenReturn(0.6);
    assertThat(delegate.shouldExecute()).isFalse();
  }
}
