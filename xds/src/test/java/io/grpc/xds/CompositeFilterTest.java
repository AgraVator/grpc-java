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
import static org.junit.Assert.assertThrows;
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
import com.google.common.collect.Iterables;
import com.google.protobuf.Any;
import io.envoyproxy.envoy.config.core.v3.RuntimeFractionalPercent;
import io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.FilterChainConfiguration;
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

  /**
   * Builds an ExecuteFilterAction whose action name is chosen independently of the child filter
   * name, so that tests can construct two distinct actions that share a name.
   */
  private static Matcher.OnMatch createExecuteActionNamed(
      String actionName, String childName, String typeUrl) {
    return Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName(actionName)
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
    assertThat(result.config.delegates).hasSize(1);
    assertThat(Iterables.getOnlyElement(result.config.delegates.keySet()).getName())
        .isEqualTo("action_child");
  }

  @Test
  public void parseFilterConfig_actionsSharingANameAreBothRetained() {
    // An action's `name` is documentation only -- the proto says it "is not used to select the
    // extension" -- and nothing requires it to be unique or even set. Keying delegates by name
    // silently dropped the second action, so a matcher branch could resolve to the wrong child
    // filter, or to none at all.
    Matcher.OnMatch first = createExecuteActionNamed("dup", "childA", FAKE_TYPE_URL);
    Matcher.OnMatch second = createExecuteActionNamed("dup", "childB", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "a", first))
            .addMatchers(createHeaderFieldMatcher("foo", "b", second))
            .build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(
            Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

    assertThat(result.errorDetail).isNull();
    assertThat(result.config.delegates).hasSize(2);
    assertThat(result.config.delegates).containsKey(first.getAction());
    assertThat(result.config.delegates).containsKey(second.getAction());
    for (com.github.xds.core.v3.TypedExtensionConfig key : result.config.delegates.keySet()) {
      assertThat(key.getName()).isEqualTo("dup");
    }
  }

  @Test
  public void parseFilterConfig_identicalActionsCollapseToOneDelegate() {
    // Two byte-identical actions describe the same work, so sharing a delegate is intended and
    // keeps the map from growing with every duplicated matcher branch.
    Matcher.OnMatch action = createExecuteActionNamed("same", "child", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "a", action))
            .addMatchers(createHeaderFieldMatcher("foo", "b", action))
            .build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(
            Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

    assertThat(result.errorDetail).isNull();
    assertThat(result.config.delegates).hasSize(1);
  }

  @Test
  public void parseFilterConfig_actionsWithNoNameAreBothRetained() {
    // gRPC-Java does not enforce min_len:1 on the action name, so an unset name is legal and
    // would previously have collapsed every anonymous action onto the empty-string key.
    Matcher.OnMatch first = createExecuteActionNamed("", "childA", FAKE_TYPE_URL);
    Matcher.OnMatch second = createExecuteActionNamed("", "childB", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "a", first))
            .addMatchers(createHeaderFieldMatcher("foo", "b", second))
            .build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(
            Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

    assertThat(result.errorDetail).isNull();
    assertThat(result.config.delegates).hasSize(2);
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
  public void whenDisabled_reportedAsUnsupportedRatherThanFailingToParse() {
    System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
    try {
      // The resource layer honours the http_filter's is_optional flag when a filter is
      // unsupported, but treats a config parse error as a fatal NACK. Reporting "disabled" as
      // unsupported therefore lets an optional composite filter be skipped instead of rejecting
      // the entire listener.
      assertThat(provider.isClientFilter()).isFalse();
      assertThat(provider.isServerFilter()).isFalse();
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
          int depth = context.recursionDepth();
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
  public void parseFilterConfigOverride_missingXdsMatcherIsRejected() {
    // A per-route override consists of nothing but the matcher, so an absent one is an error.
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfigOverride(
            Any.pack(ExtensionWithMatcherPerRoute.getDefaultInstance()), getFilterContext());

    assertThat(result.config).isNull();
    assertThat(result.errorDetail)
        .contains("ExtensionWithMatcherPerRoute.xds_matcher: field not set");
  }

  @Test
  public void parseFilterConfig_missingXdsMatcherIsAllowedAndActsAsNoOp() {
    // Unlike the override, a top-level config without a matcher is valid; the filter becomes a
    // passthrough and a per-route override may still supply a matcher.
    ExtensionWithMatcher proto = ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig.newBuilder()
            .setName("composite")
            .setTypedConfig(Any.pack(Composite.getDefaultInstance()))
            .build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    assertThat(result.errorDetail).isNull();
    assertThat(result.config).isNotNull();
    assertThat(result.config.matcher).isNull();

    CompositeFilter filter = newFilter("composite");
    assertThat(filter.buildClientInterceptor(
        result.config, null, mock(ScheduledExecutorService.class))).isNull();
    assertThat(filter.buildServerInterceptor(result.config, null)).isNull();
  }

  @Test
  public void whenEnabled_reportedAsSupportedOnBothSides() {
    assertThat(provider.isClientFilter()).isTrue();
    assertThat(provider.isServerFilter()).isTrue();
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
        .contains("no match found in composite filter");
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

    // Nested interceptors are built when the composite interceptor is built, so stub before that.
    fakeClientInterceptor = mock(ClientInterceptor.class);
    when(fakeFilter.buildClientInterceptor(any(), any(), any())).thenReturn(fakeClientInterceptor);

    org.mockito.Mockito.doAnswer(invocation -> {
      Channel nextArg = (Channel) invocation.getArguments()[2];
      return nextArg.newCall(
          (MethodDescriptor<?, ?>) invocation.getArguments()[0],
          (CallOptions) invocation.getArguments()[1]);
    }).when(fakeClientInterceptor).interceptCall(any(), any(), any());

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

    verify(fakeFilter, times(2)).buildClientInterceptor(any(), any(), any());
    verify(fakeClientInterceptor, times(2)).interceptCall(any(), any(), any());
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
  public void clientInterceptor_nestedFilterOutlivesRpcAndClosesWithComposite() {
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

    // Closing the RPC must NOT close the nested filter; it is shared across RPCs.
    verify(fakeFilter, never()).close();

    // It is released only when the composite filter itself is closed.
    filter.close();
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
        .contains("no match found in composite filter");
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
  public void serverInterceptor_nestedFilterOutlivesRpcsAndClosesWithComposite() {
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
    verify(fakeFilter, never()).close();

    listener.onCancel();
    verify(fakeFilter, never()).close();

    // A second RPC reuses the same nested filter instance rather than creating a new one.
    ServerCall call2 = mock(ServerCall.class);
    when(call2.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
    when(call2.getMethodDescriptor()).thenReturn(createMockMethod());
    ServerCall.Listener listener2 = interceptor.interceptCall(call2, headers, next);
    listener2.onCancel();
    verify(fakeFilter, never()).close();
    verify(fakeProvider, times(1)).newInstance(any());

    // Closing the composite filter releases the nested filter exactly once.
    filter.close();
    verify(fakeFilter, times(1)).close();

    // close() is idempotent.
    filter.close();
    verify(fakeFilter, times(1)).close();
  }

  /** Parses a top-level composite config whose single action runs the named nested filter. */
  private CompositeFilter.CompositeFilterConfig configWithChild(String childName) {
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar",
                createExecuteAction(childName, FAKE_TYPE_URL)))
            .build())
        .build();
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(
            Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());
    assertThat(result.errorDetail).isNull();
    return result.config;
  }

  @Test
  public void nestedFilter_oneInstancePerConfigGenerationAcrossRoutes() {
    CompositeFilter filter = newFilter("composite");
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    // The resolver hands the same config object to every route of a single LDS update.
    CompositeFilter.CompositeFilterConfig config = configWithChild("child");

    filter.buildClientInterceptor(config, null, scheduler);
    filter.buildClientInterceptor(config, null, scheduler);
    filter.buildClientInterceptor(config, null, scheduler);

    verify(fakeProvider, times(1)).newInstance(any());
    verify(fakeFilter, never()).close();
  }

  @Test
  public void nestedFilter_routesOfOneGenerationDoNotEvictEachOther() {
    CompositeFilter filter = newFilter("composite");
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    // A single LDS update: the resolver hands the same top-level config object to every route,
    // but individual routes may carry ExtensionWithMatcherPerRoute overrides that select
    // different nested filters.
    CompositeFilter.CompositeFilterConfig topLevel = configWithChild("child_a");
    CompositeFilter.CompositeFilterConfig routeOverride = configWithChild("child_b");

    filter.buildClientInterceptor(topLevel, null, scheduler);           // route 1 -> child_a
    filter.buildClientInterceptor(topLevel, routeOverride, scheduler);  // route 2 -> child_b
    filter.buildClientInterceptor(topLevel, null, scheduler);           // route 3 -> child_a

    // Generations are keyed on the top-level config's identity, so all three routes belong to
    // one generation and nothing rotates between them. Were a route to start a new generation,
    // route 3 would close child_a while route 1's interceptor still holds an interceptor built
    // from it, and would then hand route 3 a different instance.
    verify(fakeProvider, times(2)).newInstance(any());
    verify(fakeFilter, never()).close();
  }

  @Test
  public void nestedFilter_carriedForwardAcrossConfigGenerations() {
    CompositeFilter filter = newFilter("composite");
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    // Two separate LDS updates that both still configure "child".
    filter.buildClientInterceptor(configWithChild("child"), null, scheduler);
    filter.buildClientInterceptor(configWithChild("child"), null, scheduler);

    // The instance is reused, so the state it owns (connection pools, credential caches)
    // survives the update instead of being rebuilt.
    verify(fakeProvider, times(1)).newInstance(any());
    verify(fakeFilter, never()).close();
  }

  @Test
  public void nestedFilter_droppedFromConfigIsReleasedOnFollowingGeneration() {
    CompositeFilter filter = newFilter("composite");
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    filter.buildClientInterceptor(configWithChild("child"), null, scheduler);
    filter.buildClientInterceptor(configWithChild("other"), null, scheduler);

    // Generation 2 may still have routes left to process, any of which could reclaim "child",
    // so it is only retired here, not closed.
    verify(fakeFilter, never()).close();

    // Once generation 3 starts, generation 1's leftovers are provably unused.
    filter.buildClientInterceptor(configWithChild("third"), null, scheduler);
    verify(fakeFilter, times(1)).close();
  }

  @Test
  public void nestedFilter_closeReleasesActiveAndRetiredGenerations() {
    CompositeFilter filter = newFilter("composite");
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    filter.buildClientInterceptor(configWithChild("child"), null, scheduler);
    filter.buildClientInterceptor(configWithChild("other"), null, scheduler);
    verify(fakeProvider, times(2)).newInstance(any());
    verify(fakeFilter, never()).close();

    // "other" is active and "child" is retired; both must be released.
    filter.close();
    verify(fakeFilter, times(2)).close();
  }

  @Test
  public void nestedFilter_buildAfterCloseIsRejected() {
    CompositeFilter filter = newFilter("composite");
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    filter.close();

    try {
      filter.buildClientInterceptor(configWithChild("child"), null, scheduler);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected).hasMessageThat().contains("CompositeFilter is closed");
    }
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

    when(mockRandom.nextInt(1_000_000)).thenReturn(400_000);
    assertThat(delegate.shouldExecute()).isTrue();

    when(mockRandom.nextInt(1_000_000)).thenReturn(600_000);
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

    when(mockRandom.nextInt(1_000_000)).thenReturn(400_000);
    assertThat(delegate.shouldExecute()).isTrue();

    when(mockRandom.nextInt(1_000_000)).thenReturn(600_000);
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

    when(mockRandom.nextInt(1_000_000)).thenReturn(400_000);
    assertThat(delegate.shouldExecute()).isTrue();

    when(mockRandom.nextInt(1_000_000)).thenReturn(600_000);
    assertThat(delegate.shouldExecute()).isFalse();
  }

  @Test
  public void samplePercentUnknownDenominator_rejected() {
    FractionalPercent percent = FractionalPercent.newBuilder()
        .setNumerator(50)
        .setDenominatorValue(9999) // a denominator this build does not know
        .build();

    // Falling back to HUNDRED would read a numerator meant as a fraction of some much larger
    // denominator as a percentage, running the action far more often than configured.
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new CompositeFilter.FilterDelegate(
            Collections.emptyList(), percent, mock(ThreadSafeRandom.class)));
    assertThat(e).hasMessageThat().contains("Unknown denominator type");
  }

  @Test
  public void parseFilterConfig_unknownSamplePercentDenominator_nacks() {
    Matcher.OnMatch action = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_sampled")
            .setTypedConfig(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                    + ".ExecuteFilterAction")
                .setValue(ExecuteFilterAction.newBuilder()
                    .setSamplePercent(RuntimeFractionalPercent.newBuilder()
                        .setDefaultValue(FractionalPercent.newBuilder()
                            .setNumerator(50)
                            .setDenominatorValue(9999)
                            .build())
                        .build())
                    .setTypedConfig(TypedExtensionConfig.newBuilder()
                        .setName("child")
                        .setTypedConfig(Any.newBuilder()
                            .setTypeUrl(FAKE_TYPE_URL)
                            .setValue(Composite.getDefaultInstance().toByteString())
                            .build())
                        .build())
                    .build().toByteString())
                .build())
            .build())
        .build();
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", action))
            .build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(
            Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

    assertThat(result.errorDetail).contains("Unknown denominator type");
  }

  @Test
  public void parseFilterConfig_samplePercentWithoutDefaultValue_nacks() {
    // A103 requires default_value whenever sample_percent is set. An unset message would read
    // as 0%, silently disabling the action, so it has to be rejected instead.
    Matcher.OnMatch action = Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName("action_sampled")
            .setTypedConfig(Any.newBuilder()
                .setTypeUrl("type.googleapis.com/envoy.extensions.filters.http.composite.v3"
                    + ".ExecuteFilterAction")
                .setValue(ExecuteFilterAction.newBuilder()
                    .setSamplePercent(RuntimeFractionalPercent.newBuilder()
                        .setRuntimeKey("ignored_by_grpc")
                        .build())
                    .setTypedConfig(TypedExtensionConfig.newBuilder()
                        .setName("child")
                        .setTypedConfig(Any.newBuilder()
                            .setTypeUrl(FAKE_TYPE_URL)
                            .setValue(Composite.getDefaultInstance().toByteString())
                            .build())
                        .build())
                    .build().toByteString())
                .build())
            .build())
        .build();
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", action))
            .build())
        .build();

    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(
            Any.pack(createExtensionWithMatcher(matcher)), getFilterContext());

    assertThat(result.errorDetail)
        .contains("ExecuteFilterAction.sample_percent.default_value: field not set");
  }

  @Test
  public void filterConfigParseContext_recursionDepthDefaultsToZero() {
    // Callers outside the composite filter never set a depth, so the default has to be the
    // top-level value rather than an absent one every reader has to translate.
    assertThat(getFilterContext().recursionDepth()).isEqualTo(0);
  }

  @Test
  public void filterRegistry_compositeFilterRegistered() {
    FilterRegistry registry = FilterRegistry.getDefaultRegistry();
    Filter.Provider provider1 = registry.get(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER);
    assertThat(provider1).isInstanceOf(CompositeFilter.Provider.class);
    Filter.Provider provider2 =
        registry.get(CompositeFilter.TYPE_URL_EXTENSION_WITH_MATCHER_PER_ROUTE);
    assertThat(provider2).isInstanceOf(CompositeFilter.Provider.class);
  }

  @Test
  public void clientInterceptor_streamingCall_delegatesAllMethods() {
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

    // Test streaming call methods delegation
    call.request(5);
    verify(childCall).request(5);

    call.sendMessage(null);
    verify(childCall).sendMessage(null);

    call.setMessageCompression(true);
    verify(childCall).setMessageCompression(true);

    call.halfClose();
    verify(childCall).halfClose();

    // Test response listener delegation
    ClientCall.Listener capturedListener = listenerCaptor.getValue();
    Metadata respHeaders = new Metadata();
    capturedListener.onHeaders(respHeaders);
    verify(responseListener).onHeaders(respHeaders);

    capturedListener.onMessage(null);
    verify(responseListener).onMessage(null);

    Metadata trailers = new Metadata();
    capturedListener.onClose(Status.OK, trailers);
    verify(responseListener).onClose(Status.OK, trailers);

    // The nested filter must survive the RPC; it is shared across RPCs.
    verify(fakeFilter, never()).close();

    // It is released only when the composite filter itself is closed.
    filter.close();
    verify(fakeFilter).close();
  }

  @Test
  public void clientInterceptor_unaryCall_methodsSafeAfterNoMatch() {
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

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.UNAVAILABLE);

    // Stub call sequence after start() must not throw IllegalStateException("Not started")
    call.request(1);
    call.sendMessage(null);
    call.halfClose();
    call.cancel("cancel", null);
  }

  @Test
  public void clientInterceptor_unaryCall_methodsSafeAfterUnsupportedSideChildFilter() {
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

    // Subsequent calls from stubs must be safe no-ops
    call.request(1);
    call.sendMessage(null);
    call.halfClose();
    call.cancel("cancel", null);
  }

  @Test
  public void clientInterceptor_unaryCall_methodsSafeAfterCancelledBeforeStart() {
    CompositeFilter filter = newFilter("composite");
    UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
    ClientInterceptor interceptor = filter.buildClientInterceptor(
        new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap()), null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    call.cancel("cancelled before start", null);

    ClientCall.Listener<Void> listener = mock(ClientCall.Listener.class);
    call.start(listener, new Metadata());

    ArgumentCaptor<Status> statusCaptor = ArgumentCaptor.forClass(Status.class);
    verify(listener).onClose(statusCaptor.capture(), any(Metadata.class));
    assertThat(statusCaptor.getValue().getCode()).isEqualTo(Status.Code.CANCELLED);

    // Subsequent calls from stubs must be safe no-ops
    call.request(1);
    call.sendMessage(null);
    call.halfClose();
  }

  @Test
  public void filterContext_passesFilterNameAndMetricsRecorderToChild() {
    Matcher.OnMatch matchAction = createExecuteAction("my_child_filter", FAKE_TYPE_URL);
    Matcher matcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("foo", "bar", matchAction))
            .build())
        .build();

    ExtensionWithMatcher proto = createExtensionWithMatcher(matcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> result =
        provider.parseFilterConfig(Any.pack(proto), getFilterContext());

    MetricRecorder expectedRecorder = mock(MetricRecorder.class);
    CompositeFilter filter = new CompositeFilter(expectedRecorder);
    ClientInterceptor interceptor = filter.buildClientInterceptor(result.config, null,
        mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    when(fakeClientInterceptor.interceptCall(any(), any(), any()))
        .thenReturn(mock(ClientCall.class));

    MethodDescriptor<Void, Void> method = createMockMethod();
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");
    call.start(mock(ClientCall.Listener.class), headers);

    ArgumentCaptor<FilterContext> contextCaptor = ArgumentCaptor.forClass(FilterContext.class);
    verify(fakeProvider).newInstance(contextCaptor.capture());
    assertThat(contextCaptor.getValue().filterName()).isEqualTo("my_child_filter");
    assertThat(contextCaptor.getValue().metricsRecorder()).isSameInstanceAs(expectedRecorder);
  }

  @Test
  public void serverInterceptor_streamingCall_delegatesAllMethods() {
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

    ServerCall.Listener childListener = mock(ServerCall.Listener.class);
    when(fakeServerInterceptor.interceptCall(any(), any(), any())).thenReturn(childListener);

    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("foo", Metadata.ASCII_STRING_MARSHALLER), "bar");

    ServerCallHandler next = mock(ServerCallHandler.class);
    ServerCall.Listener listener = interceptor.interceptCall(call, headers, next);

    // Verify streaming message delegation
    listener.onMessage(null);
    verify(childListener).onMessage(null);

    listener.onHalfClose();
    verify(childListener).onHalfClose();

    listener.onReady();
    verify(childListener).onReady();

    // Completing the RPC must NOT tear down the nested filter: nested filters are long-lived
    // and shared across RPCs, just like top-level filters.
    listener.onComplete();
    verify(childListener).onComplete();
    verify(fakeFilter, never()).close();

    // They are released only when the composite filter itself is closed.
    filter.close();
    verify(fakeFilter).close();
  }

  @Test
  public void matchContext_pathAndAuthorityCapturedOnClientAndServer() {
    UnifiedMatcher mockMatcher = mock(UnifiedMatcher.class);
    when(mockMatcher.match(any())).thenReturn(
        io.grpc.xds.internal.matcher.MatchResult.noMatch(Collections.emptyList()));

    CompositeFilter.CompositeFilterConfig config =
        new CompositeFilter.CompositeFilterConfig(mockMatcher, Collections.emptyMap());

    CompositeFilter filter = newFilter("composite");

    // Client side verification
    ClientInterceptor clientInterceptor = filter.buildClientInterceptor(
        config, null, mock(ScheduledExecutorService.class));
    Channel next = mock(Channel.class);
    when(next.authority()).thenReturn("my-channel-authority:443");

    MethodDescriptor<Void, Void> method = createMockMethod(); // fullMethodName is "service/method"
    ClientCall<Void, Void> clientCall =
        clientInterceptor.interceptCall(method, CallOptions.DEFAULT, next);

    Metadata clientHeaders = new Metadata();
    clientCall.start(mock(ClientCall.Listener.class), clientHeaders);

    ArgumentCaptor<io.grpc.xds.internal.matcher.MatchContext> clientContextCaptor =
        ArgumentCaptor.forClass(io.grpc.xds.internal.matcher.MatchContext.class);
    verify(mockMatcher).match(clientContextCaptor.capture());

    io.grpc.xds.internal.matcher.MatchContext clientContext = clientContextCaptor.getValue();
    assertThat(clientContext.getPath()).isEqualTo("/service/method");
    assertThat(clientContext.getHost()).isEqualTo("my-channel-authority:443");
    assertThat(clientContext.getMethod()).isEqualTo("service/method");

    // Server side verification
    ServerInterceptor serverInterceptor = filter.buildServerInterceptor(config, null);
    ServerCall serverCall = mock(ServerCall.class);
    when(serverCall.getAttributes()).thenReturn(io.grpc.Attributes.EMPTY);
    when(serverCall.getMethodDescriptor()).thenReturn(createMockMethod());
    when(serverCall.getAuthority()).thenReturn("my-server-authority:50051");

    Metadata serverHeaders = new Metadata();
    serverInterceptor.interceptCall(serverCall, serverHeaders, mock(ServerCallHandler.class));

    ArgumentCaptor<io.grpc.xds.internal.matcher.MatchContext> serverContextCaptor =
        ArgumentCaptor.forClass(io.grpc.xds.internal.matcher.MatchContext.class);
    verify(mockMatcher, times(2)).match(serverContextCaptor.capture());

    io.grpc.xds.internal.matcher.MatchContext serverContext = serverContextCaptor.getValue();
    assertThat(serverContext.getPath()).isEqualTo("/service/method");
    assertThat(serverContext.getHost()).isEqualTo("my-server-authority:50051");
    assertThat(serverContext.getMethod()).isEqualTo("service/method");
  }

  @Test
  public void endToEnd_routeOverride_withExtensionWithMatcherPerRoute() {
    // Base composite filter matches "header_env=prod" -> executes child filter
    Matcher.OnMatch baseAction = createExecuteAction("prod_child", FAKE_TYPE_URL);
    Matcher baseMatcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("header_env", "prod", baseAction))
            .build())
        .build();
    ExtensionWithMatcher baseProto = createExtensionWithMatcher(baseMatcher);
    ConfigOrError<CompositeFilter.CompositeFilterConfig> baseResult =
        provider.parseFilterConfig(Any.pack(baseProto), getFilterContext());

    // Override composite filter matches "header_env=staging" -> executes child filter
    Matcher.OnMatch overrideAction = createExecuteAction("staging_child", FAKE_TYPE_URL);
    Matcher overrideMatcher = Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(createHeaderFieldMatcher("header_env", "staging", overrideAction))
            .build())
        .build();
    ExtensionWithMatcherPerRoute overrideProto = ExtensionWithMatcherPerRoute.newBuilder()
        .setXdsMatcher(overrideMatcher)
        .build();
    ConfigOrError<CompositeFilter.CompositeFilterConfig> overrideResult =
        provider.parseFilterConfigOverride(Any.pack(overrideProto), getFilterContext());

    CompositeFilter filter = newFilter("composite");
    // Build interceptor with base config overridden by route config
    ClientInterceptor interceptor = filter.buildClientInterceptor(
        baseResult.config, overrideResult.config, mock(ScheduledExecutorService.class));

    Channel next = mock(Channel.class);
    ClientCall childCall = mock(ClientCall.class);
    when(fakeClientInterceptor.interceptCall(any(), any(), any())).thenReturn(childCall);

    MethodDescriptor<Void, Void> method = createMockMethod();

    // Call with header_env=staging should match the override matcher
    ClientCall<Void, Void> call = interceptor.interceptCall(method, CallOptions.DEFAULT, next);
    Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("header_env", Metadata.ASCII_STRING_MARSHALLER), "staging");
    call.start(mock(ClientCall.Listener.class), headers);

    verify(fakeClientInterceptor).interceptCall(any(), any(), any());
    verify(childCall).start(any(), eq(headers));
  }
}
