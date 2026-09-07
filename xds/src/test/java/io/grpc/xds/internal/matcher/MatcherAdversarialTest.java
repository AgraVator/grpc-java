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

package io.grpc.xds.internal.matcher;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.xds.core.v3.TypedExtensionConfig;
import com.google.protobuf.Any;
import io.grpc.CallOptions;
import io.grpc.Grpc;
import java.net.InetSocketAddress;
import java.util.Collections;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Adversarial and boundary tests for {@link NetworkMatchInputs} including
 * ServerNameInput, SourceIpInput, and DirectSourceIpInput.
 */
@RunWith(JUnit4.class)
public class MatcherAdversarialTest {

  private static final String SERVER_NAME_TYPE_URL =
      "type.googleapis.com/envoy.extensions.matching.common_inputs.network.v3.ServerNameInput";
  private static final String SOURCE_IP_TYPE_URL =
      "type.googleapis.com/envoy.extensions.matching.common_inputs.network.v3.SourceIPInput";
  private static final String DIRECT_SOURCE_IP_TYPE_URL =
      "type.googleapis.com/envoy.extensions.matching.common_inputs.network.v3.DirectSourceIPInput";

  private static MatchInput getMatchInput(String typeUrl) {
    TypedExtensionConfig config = TypedExtensionConfig.newBuilder()
        .setTypedConfig(Any.newBuilder().setTypeUrl(typeUrl).build())
        .build();
    return MatchInputRegistry.getDefaultRegistry().getProvider(typeUrl).getInput(config);
  }

  // =========================================================================
  // SERVER NAME INPUT: Server authority vs SSLSession SNI edge cases
  // =========================================================================

  @Test
  public void serverNameInput_extendedSslSession_nullRequestedServerNames_adversarialCheck() {
    // When no SNI extension was received, ExtendedSSLSession.getRequestedServerNames() returns null
    // per JavaDoc.
    ExtendedSSLSession mockSession = mock(ExtendedSSLSession.class);
    when(mockSession.getRequestedServerNames()).thenReturn(null);

    MatchContext context = MatchContext.newBuilder()
        .setAttributes(io.grpc.Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, mockSession)
            .build())
        .build();

    MatchInput input = getMatchInput(SERVER_NAME_TYPE_URL);
    String result = (String) input.apply(context);
    assertThat(result).isNull();
  }

  @Test
  public void serverNameInput_extendedSslSession_emptyRequestedServerNames_returnsNull() {
    ExtendedSSLSession mockSession = mock(ExtendedSSLSession.class);
    when(mockSession.getRequestedServerNames()).thenReturn(Collections.emptyList());

    MatchContext context = MatchContext.newBuilder()
        .setAttributes(io.grpc.Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, mockSession)
            .build())
        .build();

    MatchInput input = getMatchInput(SERVER_NAME_TYPE_URL);
    assertThat(input.apply(context)).isNull();
  }

  @Test
  public void serverNameInput_extendedSslSession_nonHostNameType_returnsNull() {
    ExtendedSSLSession mockSession = mock(ExtendedSSLSession.class);
    SNIServerName customName = new SNIServerName(99, new byte[]{1, 2, 3}) {};
    when(mockSession.getRequestedServerNames()).thenReturn(Collections.singletonList(customName));

    MatchContext context = MatchContext.newBuilder()
        .setAttributes(io.grpc.Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, mockSession)
            .build())
        .build();

    MatchInput input = getMatchInput(SERVER_NAME_TYPE_URL);
    assertThat(input.apply(context)).isNull();
  }

  @Test
  public void serverNameInput_serverSide_matchesSniNotAuthority() {
    ExtendedSSLSession mockSession = mock(ExtendedSSLSession.class);
    SNIHostName sniHostName = new SNIHostName("tls-sni.example.com");
    when(mockSession.getRequestedServerNames()).thenReturn(Collections.singletonList(sniHostName));

    // Adversarial case: HTTP/2 :authority header differs from TLS SNI
    MatchContext context = MatchContext.newBuilder()
        .setHost("http-authority.example.com")
        .setAttributes(io.grpc.Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, mockSession)
            .build())
        .build();

    MatchInput input = getMatchInput(SERVER_NAME_TYPE_URL);

    // ServerNameInput MUST match the TLS SNI, NOT the HTTP :authority
    assertThat(input.apply(context)).isEqualTo("tls-sni.example.com");
  }

  @Test
  public void serverNameInput_serverSide_plaintextNoTls_returnsNull() {
    // Adversarial case: Plaintext request with HTTP/2 :authority header present
    MatchContext context = MatchContext.newBuilder()
        .setHost("http-authority.example.com")
        .setAttributes(io.grpc.Attributes.EMPTY)
        .build();

    MatchInput input = getMatchInput(SERVER_NAME_TYPE_URL);

    // On server with plaintext (no SSLSession), ServerNameInput MUST return null,
    // never fall back to authority.
    assertThat(input.apply(context)).isNull();
  }

  @Test
  public void serverNameInput_plainSslSession_returnsNull() {
    SSLSession plainSession = mock(SSLSession.class);
    MatchContext context = MatchContext.newBuilder()
        .setAttributes(io.grpc.Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, plainSession)
            .build())
        .build();

    MatchInput input = getMatchInput(SERVER_NAME_TYPE_URL);
    assertThat(input.apply(context)).isNull();
  }

  @Test
  public void serverNameInput_clientSide_callOptionsAuthorityUsed() {
    CallOptions callOptions = CallOptions.DEFAULT.withAuthority("override.client.example.com");
    MatchContext context = MatchContext.newBuilder()
        .setCallOptions(callOptions)
        .build();

    MatchInput input = getMatchInput(SERVER_NAME_TYPE_URL);
    assertThat(input.apply(context)).isEqualTo("override.client.example.com");
  }

  // =========================================================================
  // SOURCE IP INPUTS: Unresolved InetSocketAddress robustness
  // =========================================================================

  @Test
  public void sourceIpInput_unresolvedInetSocketAddress_adversarialCheck() {
    // Adversarial case: Unresolved InetSocketAddress has null getAddress()
    InetSocketAddress unresolved = InetSocketAddress.createUnresolved("example.com", 80);
    MatchContext context = MatchContext.newBuilder()
        .setAttributes(io.grpc.Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, unresolved)
            .build())
        .build();

    MatchInput input = getMatchInput(SOURCE_IP_TYPE_URL);
    String result = (String) input.apply(context);
    assertThat(result).isNull();
  }

  @Test
  public void directSourceIpInput_unresolvedInetSocketAddress_adversarialCheck() {
    InetSocketAddress unresolved = InetSocketAddress.createUnresolved("example.com", 80);
    MatchContext context = MatchContext.newBuilder()
        .setAttributes(io.grpc.Attributes.newBuilder()
            .set(Grpc.TRANSPORT_ATTR_REMOTE_ADDR, unresolved)
            .build())
        .build();

    MatchInput input = getMatchInput(DIRECT_SOURCE_IP_TYPE_URL);
    String result = (String) input.apply(context);
    assertThat(result).isNull();
  }
}
