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
import io.envoyproxy.envoy.config.core.v3.Address;
import io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.envoyproxy.envoy.config.core.v3.TrafficDirection;
import io.envoyproxy.envoy.config.core.v3.TypedExtensionConfig;
import io.envoyproxy.envoy.config.listener.v3.Filter;
import io.envoyproxy.envoy.config.listener.v3.FilterChain;
import io.envoyproxy.envoy.config.listener.v3.FilterChainMatch;
import io.envoyproxy.envoy.config.listener.v3.Listener;
import io.envoyproxy.envoy.config.route.v3.NonForwardingAction;
import io.envoyproxy.envoy.config.route.v3.Route;
import io.envoyproxy.envoy.config.route.v3.RouteConfiguration;
import io.envoyproxy.envoy.config.route.v3.RouteMatch;
import io.envoyproxy.envoy.config.route.v3.VirtualHost;
import io.envoyproxy.envoy.extensions.common.matching.v3.ExtensionWithMatcher;
import io.envoyproxy.envoy.extensions.filters.common.matcher.action.v3.SkipFilter;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.Composite;
import io.envoyproxy.envoy.extensions.filters.http.composite.v3.ExecuteFilterAction;
import io.envoyproxy.envoy.extensions.filters.http.rbac.v3.RBAC;
import io.envoyproxy.envoy.extensions.filters.http.router.v3.Router;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpFilter;
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
 * Server-side end-to-end tests for the composite filter (gRFC A103).
 *
 * <p>A103 opens with "We will support the composite filter in both the gRPC client and gRPC
 * server", and its "Filter Behavior" section is written without reference to a side. These tests
 * assert the same four behaviours as {@link CompositeFilterE2eTest}, but with the composite filter
 * installed in the socket listener rather than the API listener, so the filter runs inside the
 * xDS-configured server.
 *
 * <p>This side deserves independent coverage: gRPC C++ does not implement the composite filter on
 * the server at all, so there is no reference implementation to have been ported from.
 *
 * <p>The nested filter here is RBAC, which is server-side-only. It is configured with an ALLOW
 * action and no policies, so nothing can ever match and every RPC that reaches it is denied. As on
 * the client, that makes three outcomes distinguishable from one RPC:
 *
 * <ul>
 *   <li>{@code PERMISSION_DENIED} - the nested RBAC filter ran
 *   <li>{@code UNAVAILABLE} - the composite filter found no match
 *   <li>a successful response - the RPC passed through without the nested filter
 * </ul>
 */
@RunWith(JUnit4.class)
public class CompositeFilterServerE2eTest {

  private static final String SERVER_HOST_NAME = "test-server";
  private static final String SERVER_LISTENER_TEMPLATE_NO_REPLACEMENT =
      "grpc/server?udpa.resource.listening_address=";
  private static final String COMPOSITE_FILTER_NAME = "composite-filter";

  private static final Metadata.Key<String> MATCH_HEADER =
      Metadata.Key.of("x-composite-match", Metadata.ASCII_STRING_MARSHALLER);

  private static final SimpleResponse EXPECTED_RESPONSE = SimpleResponse.newBuilder()
      .setResponseMessage("Hi, xDS! Authority= " + SERVER_HOST_NAME)
      .build();

  /** How long to wait for a pushed server listener to reach the already-running xDS server. */
  private static final long CONFIG_PROPAGATION_TIMEOUT_SECONDS = 15;

  @Rule(order = 0)
  public ControlPlaneRule controlPlane = new ControlPlaneRule();

  @Rule(order = 1)
  public DataPlaneRule dataPlane = new DataPlaneRule(controlPlane);

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
    pushServerCompositeConfig(executeRbacAction());
    awaitCompositeFilterActive();

    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> stub().unaryRpc(SimpleRequest.getDefaultInstance()));

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
  }

  // ---------------------------------------------------------------------------------------------
  // A103: "If the matcher tree finds a SkipFilter match, the filter will simply pass the RPC
  // through to the next filter ... without delegating to any nested filters."
  // ---------------------------------------------------------------------------------------------

  @Test
  public void skipFilterAction_rpcPassesThrough() {
    pushServerCompositeConfig(skipAction());
    awaitCompositeFilterActive();

    assertThat(callWithMatchHeader()).isEqualTo(EXPECTED_RESPONSE);
  }

  // ---------------------------------------------------------------------------------------------
  // A103: "the RPC will be passed to the nested filter chain before being sent to the next filter".
  // ---------------------------------------------------------------------------------------------

  @Test
  public void executeFilterAction_matched_nestedFilterRuns() {
    pushServerCompositeConfig(executeRbacAction());
    awaitCompositeFilterActive();

    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> callWithMatchHeader());

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // ---------------------------------------------------------------------------------------------
  // A103: "If the RPC is not sampled, then the filter will pass the RPC through to the next filter
  // ... without delegating to any nested filters."
  // ---------------------------------------------------------------------------------------------

  @Test
  public void samplePercentZero_nestedFilterNeverRuns() {
    pushServerCompositeConfig(executeRbacActionSampled(0));
    awaitCompositeFilterActive();

    assertThat(callWithMatchHeader()).isEqualTo(EXPECTED_RESPONSE);
  }

  @Test
  public void samplePercentHundred_nestedFilterAlwaysRuns() {
    pushServerCompositeConfig(executeRbacActionSampled(100));
    awaitCompositeFilterActive();

    StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
        () -> callWithMatchHeader());

    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  /**
   * Blocks until the pushed server listener is actually in force.
   *
   * <p>Unlike the client tests, the xDS server is already running and serving the default listener
   * by the time a test body starts, so an assertion made too early can pass against the *old*
   * config. Every matcher in this class keys off {@link #MATCH_HEADER} and declares no
   * {@code on_no_match}, so a header-less RPC returns UNAVAILABLE once - and only once - the
   * composite filter is live. That makes it a reliable barrier.
   */
  private void awaitCompositeFilterActive() {
    long deadline = System.nanoTime()
        + TimeUnit.SECONDS.toNanos(CONFIG_PROPAGATION_TIMEOUT_SECONDS);
    Status.Code lastCode = null;
    while (System.nanoTime() < deadline) {
      try {
        stub().unaryRpc(SimpleRequest.getDefaultInstance());
        lastCode = Status.Code.OK;
      } catch (StatusRuntimeException e) {
        lastCode = e.getStatus().getCode();
        if (lastCode == Status.Code.UNAVAILABLE) {
          return;
        }
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted waiting for config propagation", e);
      }
    }
    throw new AssertionError(
        "composite filter never became active; last RPC status was " + lastCode);
  }

  private SimpleResponse callWithMatchHeader() {
    Metadata headers = new Metadata();
    headers.put(MATCH_HEADER, "run");
    return stub().withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers))
        .unaryRpc(SimpleRequest.getDefaultInstance());
  }

  private SimpleServiceGrpc.SimpleServiceBlockingStub stub() {
    ManagedChannel channel = dataPlane.getManagedChannel();
    return SimpleServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
  }

  /**
   * Replaces the server listener with one whose HTTP filter chain is [composite, router]. The
   * client listener is left at its default, so only the server side is under test.
   */
  private void pushServerCompositeConfig(Matcher.OnMatch onMatch) {
    ExtensionWithMatcher compositeConfig = ExtensionWithMatcher.newBuilder()
        .setExtensionConfig(TypedExtensionConfig.newBuilder()
            .setName(COMPOSITE_FILTER_NAME)
            .setTypedConfig(Any.pack(Composite.getDefaultInstance())))
        .setXdsMatcher(matcherOnMatchHeader(onMatch))
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

    RouteConfiguration routeConfig = RouteConfiguration.newBuilder()
        .addVirtualHosts(VirtualHost.newBuilder()
            .setName("virtual-host-0")
            .addDomains("*")
            .addRoutes(Route.newBuilder()
                .setMatch(RouteMatch.newBuilder().setPrefix("/"))
                .setNonForwardingAction(NonForwardingAction.getDefaultInstance())))
        .build();

    Filter networkFilter = Filter.newBuilder()
        .setName("network-filter-0")
        .setTypedConfig(Any.pack(HttpConnectionManager.newBuilder()
            .setRouteConfig(routeConfig)
            .addHttpFilters(composite)
            .addHttpFilters(router)
            .build()))
        .build();

    Listener serverListener = Listener.newBuilder()
        .setName(SERVER_LISTENER_TEMPLATE_NO_REPLACEMENT)
        .setTrafficDirection(TrafficDirection.INBOUND)
        .addFilterChains(FilterChain.newBuilder()
            .setName("filter-chain-0")
            .setFilterChainMatch(FilterChainMatch.newBuilder()
                .setSourceType(FilterChainMatch.ConnectionSourceType.ANY))
            .addFilters(networkFilter))
        .setAddress(Address.newBuilder()
            .setSocketAddress(SocketAddress.newBuilder().setAddress("0.0.0.0").setPortValue(0)))
        .build();

    controlPlane.setLdsConfig(
        serverListener, ControlPlaneRule.buildClientListener(SERVER_HOST_NAME));
  }

  // ---------------------------------------------------------------------------------------------
  // Proto builders
  // ---------------------------------------------------------------------------------------------

  private static Matcher matcherOnMatchHeader(Matcher.OnMatch onMatch) {
    return Matcher.newBuilder()
        .setMatcherList(Matcher.MatcherList.newBuilder()
            .addMatchers(Matcher.MatcherList.FieldMatcher.newBuilder()
                .setPredicate(Matcher.MatcherList.Predicate.newBuilder()
                    .setSinglePredicate(Matcher.MatcherList.Predicate.SinglePredicate.newBuilder()
                        .setInput(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
                            .setName("request-headers")
                            .setTypedConfig(Any.pack(HttpRequestHeaderMatchInput.newBuilder()
                                .setHeaderName(MATCH_HEADER.name())
                                .build())))
                        .setValueMatch(StringMatcher.newBuilder().setExact("run"))))
                .setOnMatch(onMatch)))
        .build();
  }

  private static Matcher.OnMatch skipAction() {
    return action("skip", Any.pack(SkipFilter.getDefaultInstance()));
  }

  private static Matcher.OnMatch executeRbacAction() {
    return action("execute-rbac", Any.pack(ExecuteFilterAction.newBuilder()
        .setTypedConfig(TypedExtensionConfig.newBuilder()
            .setName("rbac")
            .setTypedConfig(denyAllRbac()))
        .build()));
  }

  private static Matcher.OnMatch executeRbacActionSampled(int percent) {
    return action("execute-rbac", Any.pack(ExecuteFilterAction.newBuilder()
        .setTypedConfig(TypedExtensionConfig.newBuilder()
            .setName("rbac")
            .setTypedConfig(denyAllRbac()))
        .setSamplePercent(io.envoyproxy.envoy.config.core.v3.RuntimeFractionalPercent.newBuilder()
            .setDefaultValue(FractionalPercent.newBuilder()
                .setNumerator(percent)
                .setDenominator(FractionalPercent.DenominatorType.HUNDRED)))
        .build()));
  }

  private static Matcher.OnMatch action(String actionName, Any actionConfig) {
    return Matcher.OnMatch.newBuilder()
        .setAction(com.github.xds.core.v3.TypedExtensionConfig.newBuilder()
            .setName(actionName)
            .setTypedConfig(actionConfig))
        .build();
  }

  /** An ALLOW policy set with no policies in it: nothing can match, so everything is denied. */
  private static Any denyAllRbac() {
    return Any.pack(RBAC.newBuilder()
        .setRules(io.envoyproxy.envoy.config.rbac.v3.RBAC.newBuilder()
            .setAction(io.envoyproxy.envoy.config.rbac.v3.RBAC.Action.ALLOW))
        .build());
  }
}
