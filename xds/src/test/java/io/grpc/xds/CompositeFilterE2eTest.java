/*
 * Copyright 2025 The gRPC Authors
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

import com.github.xds.type.matcher.v3.Matcher;
import com.github.xds.type.matcher.v3.StringMatcher;
import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import io.envoyproxy.envoy.config.core.v3.AggregatedConfigSource;
import io.envoyproxy.envoy.config.core.v3.ConfigSource;
import io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig;
import io.envoyproxy.envoy.config.listener.v3.ApiListener;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteAction;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.RouteMatch;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcherPerRoute;
import io.envoyproxy.envoy.extensions.filters.common.matcher.action.v3.SkipFilter;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.FilterChainConfiguration;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.FaultAbort;
import io.envoyproxy.envoy.extensions.filters.http.fault.v3.HTTPFault;
import io.envoyproxy.envoy.extensions.filters.http.rbac.v3.RBAC;
import io.envoyproxy.envoy.extensions.filters.http.router.v3.Router;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpFilter;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.Rds;
import io.envoyproxy.envoy.type.matcher.v3.HttpRequestHeaderMatchInput;
import io.envoyproxy.envoy.type.v3.FractionalPercent;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import io.grpc.testing.protobuf.SimpleRequest;
import io.grpc.testing.protobuf.SimpleResponse;
import io.grpc.testing.protobuf.SimpleServiceGrpc;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end tests for the composite filter (gRFC A103), driven by a real control plane.
 *
 * <p>Unlike {@code CompositeFilterTest}, nothing here calls {@code CompositeFilter} directly. A
 * real xDS config is pushed to a real ADS server, a real channel resolves it, and a real RPC is
 * sent. The only thing asserted is the RPC's observable outcome, so these tests describe the
 * behaviour A103 promises rather than the implementation that provides it.
 *
 * <p>Every assertion below is derived from A103's "Filter Behavior" section:
 *
 * <ul>
 *   <li>no match in the matcher tree -> fail the RPC with UNAVAILABLE
 *   <li>a {@code SkipFilter} match -> pass the RPC through, running no nested filters
 *   <li>an {@code ExecuteFilterAction} match, not sampled -> pass the RPC through
 *   <li>an {@code ExecuteFilterAction} match, sampled -> run the nested filter chain
 *   <li>{@code filter_chain} -> "a chain of filters to call, in order"
 *   <li>a per-route override -> "replace the value of the field in the top-level config"
 *   <li>a nested filter unsupported on this side -> fail the RPC with UNAVAILABLE
 * </ul>
 *
 * <p>The nested filter used throughout is the fault-injection filter configured to abort 100% of
 * RPCs with a distinctive status code. That makes "the nested filter ran" observable from the
 * client as a status code, with no test hooks in the filter path. Three outcomes are therefore
 * distinguishable from one RPC:
 *
 * <ul>
 *   <li>{@code PERMISSION_DENIED} / {@code RESOURCE_EXHAUSTED} - a nested fault filter ran
 *   <li>{@code UNAVAILABLE} - the composite filter refused the RPC
 *   <li>a successful response - the RPC passed through untouched
 * </ul>
 */
@RunWith(JUnit4.class)
public class CompositeFilterE2eTest {

  private static final String SERVER_HOST_NAME = "test-server";
  private static final String RDS_NAME = "route-config.googleapis.com";
  private static final String CLUSTER_NAME = "cluster0";
  private static final String COMPOSITE_FILTER_NAME = "composite-filter";
  private static final String HTTP_CONNECTION_MANAGER_TYPE_URL =
      "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3"
          + ".HttpConnectionManager";

  private static final Metadata.Key<String> MATCH_HEADER =
      Metadata.Key.of("x-composite-match", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> OTHER_HEADER =
      Metadata.Key.of("x-composite-other", Metadata.ASCII_STRING_MARSHALLER);

  /** The response {@link DataPlaneRule}'s echo server sends when an RPC gets through. */
  private static final SimpleResponse EXPECTED_RESPONSE = SimpleResponse.newBuilder()
      .setResponseMessage("Hi, xDS! Authority= " + SERVER_HOST_NAME)
      .build();

  @Rule(order = 0)
  public ControlPlaneRule controlPlane = new ControlPlaneRule();

  @Rule(order = 1)
  public DataPlaneRule dataPlane = new DataPlaneRule(controlPlane);

  /**
   * A103: "Support for the composite filter will be guarded by the
   * GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER environment variable."
   *
   * <p>This must be set before the JUnit rules run, because {@link DataPlaneRule} starts the xDS
   * server - and parses its LDS - during {@code starting()}.
   */
  @BeforeClass
  public static void enableCompositeFilter() {
    System.setProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER", "true");
  }

  @AfterClass
  public static void disableCompositeFilter() {
    System.clearProperty("GRPC_EXPERIMENTAL_XDS_COMPOSITE_FILTER");
  }

  // ---------------------------------------------------------------------------------------------
  // A103: "If the matcher tree does not find a match, the RPC will be failed with UNAVAILABLE
  // status."
  // ---------------------------------------------------------------------------------------------

  @Test
  public void noMatch_rpcFailsWithUnavailable() {
    pushCompositeConfig(matcherOnHeader("run", executeAction("fault", faultAbort(Status.Code
        .PERMISSION_DENIED))));

    // The matcher keys off MATCH_HEADER; this RPC does not carry it, so nothing matches.
    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> callWithoutHeaders());

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }

  // ---------------------------------------------------------------------------------------------
  // A103: "If the matcher tree finds a SkipFilter match, the filter will simply pass the RPC
  // through to the next filter (the one after the composite filter), without delegating to any
  // nested filters."
  // ---------------------------------------------------------------------------------------------

  @Test
  public void skipFilterAction_rpcPassesThrough() {
    pushCompositeConfig(matcherOnHeader("run", skipAction()));

    assertThat(callWithMatchHeader()).isEqualTo(EXPECTED_RESPONSE);
  }

  // ---------------------------------------------------------------------------------------------
  // A103: "if the RPC *is* sampled, the RPC will be passed to the nested filter chain before being
  // sent to the next filter".  sample_percent is unset here, and A103 says of it: "Optional; if
  // unset, the specified filter(s) are always executed."
  // ---------------------------------------------------------------------------------------------

  @Test
  public void executeFilterAction_matched_nestedFilterRuns() {
    pushCompositeConfig(matcherOnHeader("run",
        executeAction("fault", faultAbort(Status.Code.PERMISSION_DENIED))));

    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> callWithMatchHeader());

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // ---------------------------------------------------------------------------------------------
  // A103: "for each RPC, a random number will be generated between 0 and 100, and if that number
  // is less than the specified threshold, the specified filter(s) will be executed."  At 0% no
  // random number can be below the threshold, so the filter must never run; A103: "If the RPC is
  // not sampled, then the filter will pass the RPC through to the next filter ... without
  // delegating to any nested filters."
  // ---------------------------------------------------------------------------------------------

  @Test
  public void samplePercentZero_nestedFilterNeverRuns() {
    pushCompositeConfig(matcherOnHeader("run",
        executeActionSampled("fault", 0, faultAbort(Status.Code.PERMISSION_DENIED))));

    assertThat(callWithMatchHeader()).isEqualTo(EXPECTED_RESPONSE);
  }

  @Test
  public void samplePercentHundred_nestedFilterAlwaysRuns() {
    pushCompositeConfig(matcherOnHeader("run",
        executeActionSampled("fault", 100, faultAbort(Status.Code.PERMISSION_DENIED))));

    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> callWithMatchHeader());

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // ---------------------------------------------------------------------------------------------
  // A103 on filter_chain: "This specifies a chain of filters to call, in order."
  //
  // Both filters in the chain abort, with different status codes. Whichever runs first terminates
  // the RPC, so the resulting status code names the filter that ran first. "In order" means that
  // must be the one listed first.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void filterChain_callsFiltersInConfiguredOrder() {
    pushCompositeConfig(matcherOnHeader("run", executeActionChain("chain",
        faultAbort(Status.Code.PERMISSION_DENIED),   // listed first -> must run first
        faultAbort(Status.Code.RESOURCE_EXHAUSTED))));

    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> callWithMatchHeader());

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // ---------------------------------------------------------------------------------------------
  // A103 on ExtensionWithMatcherPerRoute: "The value of this field will replace the value of the
  // field in the top-level config."
  //
  // Replacement, not merge: after the override is applied the top-level matcher must have no
  // effect at all, so an RPC that the top-level matcher would have matched now finds no match.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void perRouteOverride_replacesTopLevelMatcher() {
    // Top level matches MATCH_HEADER; the override matches OTHER_HEADER instead.
    Matcher topLevel = matcherOnHeader("run",
        executeAction("fault", faultAbort(Status.Code.PERMISSION_DENIED)));
    Matcher override = matcherOnOtherHeader("run",
        executeAction("fault", faultAbort(Status.Code.RESOURCE_EXHAUSTED)));

    pushCompositeConfig(topLevel);
    pushRouteOverride(override);

    // The override's matcher is in force, so the override's nested filter runs.
    StatusRuntimeException overridden = assertThrows(StatusRuntimeException.class,
        () -> callWithHeader(OTHER_HEADER, "run"));
    assertThat(overridden.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);

    // The top-level matcher was replaced, not merged, so its header no longer matches anything.
    StatusRuntimeException replaced = assertThrows(StatusRuntimeException.class,
        () -> callWithMatchHeader());
    assertThat(replaced.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }

  // ---------------------------------------------------------------------------------------------
  // A103: "if a nested filter chain includes a filter that is not supported on the side that it is
  // running on, we will fail the RPC with status UNAVAILABLE."
  //
  // RBAC is a server-side-only filter, nested here inside a client-side composite filter.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void nestedFilterUnsupportedOnThisSide_rpcFailsWithUnavailable() {
    pushCompositeConfig(matcherOnHeader("run",
        executeAction("rbac", Any.pack(RBAC.getDefaultInstance()))));

    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> callWithMatchHeader());

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }

  // ---------------------------------------------------------------------------------------------
  // RPC helpers
  // ---------------------------------------------------------------------------------------------

  private SimpleResponse callWithMatchHeader() {
    return callWithHeader(MATCH_HEADER, "run");
  }

  private SimpleResponse callWithHeader(Metadata.Key<String> key, String value) {
    Metadata headers = new Metadata();
    headers.put(key, value);
    return stub().withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
        .unaryRpc(SimpleRequest.getDefaultInstance());
  }

  private SimpleResponse callWithoutHeaders() {
    return stub().unaryRpc(SimpleRequest.getDefaultInstance());
  }

  private SimpleServiceGrpc.SimpleServiceBlockingStub stub() {
    ManagedChannel channel = dataPlane.getManagedChannel();
    return SimpleServiceGrpc.newBlockingStub(channel)
        .withDeadlineAfter(10, TimeUnit.SECONDS);
  }

  // ---------------------------------------------------------------------------------------------
  // Config plumbing
  // ---------------------------------------------------------------------------------------------

  /** Replaces the client listener with one whose filter chain is [composite, router]. */
  private void pushCompositeConfig(Matcher matcher) {
    ExtensionWithMatcher compositeConfig = ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(TypedExtensionConfig.newBuilder()
            .setName(COMPOSITE_FILTER_NAME)
            .setTypedConfig(Any.pack(Composite.getDefaultInstance())))
        .setXdsMatcher(matcher)
        .build();

    HttpFilter composite = HttpFilter.newBuilder()
        .setName(COMPOSITE_FILTER_NAME)
        .setTypedConfig(Any.pack(compositeConfig))
        .build();
    HttpFilter router = HttpFilter.newBuilder()
        .setName("terminal-filter")
        .setTypedConfig(Any.pack(Router.newBuilder().build()))
        .setIsOptional(true)
        .build();

    ApiListener apiListener = ApiListener.newBuilder()
        .setApiListener(Any.pack(HttpConnectionManager.newBuilder()
            .setRds(Rds.newBuilder()
                .setRouteConfigName(RDS_NAME)
                .setConfigSource(ConfigSource.newBuilder()
                    .setAds(AggregatedConfigSource.getDefaultInstance())))
            .addHttpFilters(composite)
            .addHttpFilters(router)
            .build(), HTTP_CONNECTION_MANAGER_TYPE_URL))
        .build();

    controlPlane.setLdsConfig(
        ControlPlaneRule.buildServerListener(),
        Listener.newBuilder().setName(SERVER_HOST_NAME).setApiListener(apiListener).build());
  }

  /** Attaches an ExtensionWithMatcherPerRoute override to the one and only route. */
  private void pushRouteOverride(Matcher matcher) {
    ExtensionWithMatcherPerRoute perRoute = ExtensionWithMatcherPerRoute.newBuilder()
        .setXdsMatcher(matcher)
        .build();

    controlPlane.setRdsConfig(RouteConfiguration.newBuilder()
        .setName(RDS_NAME)
        .addVirtualHosts(VirtualHost.newBuilder()
            .setName(RDS_NAME)
            .addDomains(SERVER_HOST_NAME)
            .addRoutes(Route.newBuilder()
                .setMatch(RouteMatch.newBuilder().setPrefix("/"))
                .setRoute(RouteAction.newBuilder()
                    .setCluster(CLUSTER_NAME)
                    .setAutoHostRewrite(BoolValue.of(true)))
                .putTypedPerFilterConfig(COMPOSITE_FILTER_NAME, Any.pack(perRoute))))
        .build());
  }

  // ---------------------------------------------------------------------------------------------
  // Proto builders (plain xDS API usage, per A103 / A106)
  // ---------------------------------------------------------------------------------------------

  private static Matcher matcherOnHeader(String value, Matcher.OnMatch onMatch) {
    return matcherOnNamedHeader(MATCH_HEADER.name(), value, onMatch);
  }

  private static Matcher matcherOnOtherHeader(String value, Matcher.OnMatch onMatch) {
    return matcherOnNamedHeader(OTHER_HEADER.name(), value, onMatch);
  }

  /** A single-predicate matcher list, with no on_no_match, keyed on one request header. */
  private static Matcher matcherOnNamedHeader(
      String headerName, String headerValue, Matcher.OnMatch onMatch) {
    return Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(Matcher.MatcherList.FieldMatcher.newBuilder()
                .setPredicate(Matcher.MatcherList.Predicate.newBuilder()
                    .setSinglePredicate(Matcher.MatcherList.Predicate.SinglePredicate.newBuilder()
                        .setInput(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                            .setName("request-headers")
                            .setTypedConfig(Any.pack(HttpRequestHeaderMatchInput.newBuilder()
                                .setHeaderName(headerName)
                                .build())))
                        .setValueMatch(StringMatcher.newBuilder().setExact(headerValue))))
                .setOnMatch(onMatch)))
        .build();
  }

  /** A103: a SkipFilter action "indicates that no filter will be executed". */
  private static Matcher.OnMatch skipAction() {
    return action("skip", Any.pack(SkipFilter.getDefaultInstance()));
  }

  /** An ExecuteFilterAction naming a single nested filter via typed_config. */
  private static Matcher.OnMatch executeAction(String name, Any childConfig) {
    return action("execute-" + name, Any.pack(ExecuteFilterAction.newBuilder()
        .setTypedConfig(TypedExtensionConfig.newBuilder()
            .setName(name)
            .setTypedConfig(childConfig))
        .build()));
  }

  /** The same, but with sample_percent.default_value set to {@code percent}%. */
  private static Matcher.OnMatch executeActionSampled(String name, int percent, Any childConfig) {
    return action("execute-" + name, Any.pack(ExecuteFilterAction.newBuilder()
        .setTypedConfig(TypedExtensionConfig.newBuilder()
            .setName(name)
            .setTypedConfig(childConfig))
        .setSamplePercent(io.envoyproxy.envoy.config.core.v3.RuntimeFractionalPercent.newBuilder()
            .setDefaultValue(percentage(percent)))
        .build()));
  }

  /** An ExecuteFilterAction naming several nested filters via filter_chain, in order. */
  private static Matcher.OnMatch executeActionChain(String name, Any... childConfigs) {
    FilterChainConfiguration.Builder chain = FilterChainConfiguration.newBuilder();
    for (int i = 0; i < childConfigs.length; i++) {
      chain.addTypedConfig(TypedExtensionConfig.newBuilder()
          .setName(name + "-" + i)
          .setTypedConfig(childConfigs[i]));
    }
    return action("execute-" + name, Any.pack(ExecuteFilterAction.newBuilder()
        .setFilterChain(chain)
        .build()));
  }

  private static Matcher.OnMatch action(String actionName, Any actionConfig) {
    return Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName(actionName)
            .setTypedConfig(actionConfig))
        .build();
  }

  /** A fault filter config that aborts 100% of RPCs with {@code code}. */
  private static Any faultAbort(Status.Code code) {
    return Any.pack(HTTPFault.newBuilder()
        .setAbort(FaultAbort.newBuilder()
            .setGrpcStatus(code.value())
            .setPercentage(percentage(100)))
        .build());
  }

  private static FractionalPercent percentage(int percent) {
    return FractionalPercent.newBuilder()
        .setNumerator(percent)
        .setDenominator(FractionalPercent.DenominatorType.HUNDRED)
        .build();
  }
}
