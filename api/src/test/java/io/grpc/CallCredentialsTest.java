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

package io.grpc;

import static com.google.common.truth.Truth.assertThat;

import io.grpc.CallCredentials.RequestInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link CallCredentials}. */
@RunWith(JUnit4.class)
public class CallCredentialsTest {

  @Test
  public void allowedSecurityLevel_higherOrEqualIsAllowed() {
    assertThat(
            CallCredentials.allowedSecurityLevel(
                requestInfo(SecurityLevel.PRIVACY_AND_INTEGRITY),
                SecurityLevel.PRIVACY_AND_INTEGRITY))
        .isTrue();
    assertThat(
            CallCredentials.allowedSecurityLevel(
                requestInfo(SecurityLevel.PRIVACY_AND_INTEGRITY), SecurityLevel.INTEGRITY))
        .isTrue();
    assertThat(
            CallCredentials.allowedSecurityLevel(
                requestInfo(SecurityLevel.INTEGRITY), SecurityLevel.NONE))
        .isTrue();
  }

  @Test
  public void allowedSecurityLevel_lowerIsNotAllowed() {
    assertThat(
            CallCredentials.allowedSecurityLevel(
                requestInfo(SecurityLevel.INTEGRITY), SecurityLevel.PRIVACY_AND_INTEGRITY))
        .isFalse();
    assertThat(
            CallCredentials.allowedSecurityLevel(
                requestInfo(SecurityLevel.NONE), SecurityLevel.INTEGRITY))
        .isFalse();
  }

  private static RequestInfo requestInfo(final SecurityLevel securityLevel) {
    return new RequestInfo() {
      @Override
      public MethodDescriptor<?, ?> getMethodDescriptor() {
        throw new UnsupportedOperationException();
      }

      @Override
      public SecurityLevel getSecurityLevel() {
        return securityLevel;
      }

      @Override
      public String getAuthority() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Attributes getTransportAttrs() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
