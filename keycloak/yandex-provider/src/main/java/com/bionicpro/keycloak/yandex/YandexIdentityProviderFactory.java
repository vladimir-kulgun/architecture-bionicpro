package com.bionicpro.keycloak.yandex;

import org.keycloak.broker.oidc.OIDCIdentityProviderConfig;
import org.keycloak.broker.social.SocialIdentityProviderFactory;
import org.keycloak.models.IdentityProviderModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.util.JsonSerialization;

import org.keycloak.Config;
import org.keycloak.models.KeycloakSessionFactory;

import java.io.InputStream;
import java.util.Map;

public class YandexIdentityProviderFactory
        implements SocialIdentityProviderFactory<YandexIdentityProvider> {

    public static final String PROVIDER_ID = "yandex";

    @Override
    public String getName() {
        return "Яндекс ID";
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public YandexIdentityProvider create(KeycloakSession session) {
        throw new UnsupportedOperationException("Use create(session, model)");
    }

    @Override
    public YandexIdentityProvider create(KeycloakSession session, IdentityProviderModel model) {
        return new YandexIdentityProvider(session, new OIDCIdentityProviderConfig(model));
    }

    @Override
    public OIDCIdentityProviderConfig createConfig() {
        return new OIDCIdentityProviderConfig();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> parseConfig(KeycloakSession session, InputStream inputStream) {
        try {
            return JsonSerialization.readValue(inputStream, Map.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse Yandex provider config", e);
        }
    }

    @Override
    public void init(Config.Scope config) {
        // no-op
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
