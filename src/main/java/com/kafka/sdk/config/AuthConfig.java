package com.kafka.sdk.config;

import com.kafka.sdk.model.exception.ConfigurationException;
import java.util.Arrays;

/**
 * Immutable authentication configuration.
 * Fields required depend on the chosen {@link AuthMechanism}.
 * Passwords are stored as {@code char[]} and masked in {@link #toString()}.
 */
public final class AuthConfig {

    private final AuthMechanism mechanism;
    private final String tlsKeystorePath;
    private final char[] tlsKeystorePassword;
    private final String tlsTruststorePath;
    private final char[] tlsTruststorePassword;
    private final String scramUsername;
    private final char[] scramPassword;

    private AuthConfig(Builder b) {
        this.mechanism = b.mechanism;
        this.tlsKeystorePath = b.tlsKeystorePath;
        this.tlsKeystorePassword = copy(b.tlsKeystorePassword);
        this.tlsTruststorePath = b.tlsTruststorePath;
        this.tlsTruststorePassword = copy(b.tlsTruststorePassword);
        this.scramUsername = b.scramUsername;
        this.scramPassword = copy(b.scramPassword);
    }

    private static char[] copy(char[] src) {
        return src != null ? Arrays.copyOf(src, src.length) : null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public AuthMechanism getMechanism() { return mechanism; }
    public String getTlsKeystorePath() { return tlsKeystorePath; }
    public char[] getTlsKeystorePassword() { return copy(tlsKeystorePassword); }
    public String getTlsTruststorePath() { return tlsTruststorePath; }
    public char[] getTlsTruststorePassword() { return copy(tlsTruststorePassword); }
    public String getScramUsername() { return scramUsername; }
    public char[] getScramPassword() { return copy(scramPassword); }

    @Override
    public String toString() {
        return "AuthConfig{mechanism=" + mechanism
                + ", tlsKeystorePath=" + tlsKeystorePath
                + ", tlsTruststorePath=" + tlsTruststorePath
                + ", scramUsername=" + scramUsername
                + ", passwords=[MASKED]}";
    }

    public static final class Builder {
        private AuthMechanism mechanism;
        private String tlsKeystorePath;
        private char[] tlsKeystorePassword;
        private String tlsTruststorePath;
        private char[] tlsTruststorePassword;
        private String scramUsername;
        private char[] scramPassword;

        public Builder mechanism(AuthMechanism mechanism) { this.mechanism = mechanism; return this; }
        public Builder tlsKeystorePath(String path) { this.tlsKeystorePath = path; return this; }
        public Builder tlsKeystorePassword(char[] password) { this.tlsKeystorePassword = password; return this; }
        public Builder tlsTruststorePath(String path) { this.tlsTruststorePath = path; return this; }
        public Builder tlsTruststorePassword(char[] password) { this.tlsTruststorePassword = password; return this; }
        public Builder scramUsername(String username) { this.scramUsername = username; return this; }
        public Builder scramPassword(char[] password) { this.scramPassword = password; return this; }

        public AuthConfig build() {
            if (mechanism == null) {
                throw new ConfigurationException("authMechanism is required");
            }
            return new AuthConfig(this);
        }
    }
}
