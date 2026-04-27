package com.bionicpro.keycloak.yandex;

import com.fasterxml.jackson.databind.JsonNode;
import org.keycloak.broker.oidc.AbstractOAuth2IdentityProvider;
import org.keycloak.broker.oidc.OIDCIdentityProviderConfig;
import org.keycloak.broker.oidc.mappers.AbstractJsonUserAttributeMapper;
import org.keycloak.broker.provider.BrokeredIdentityContext;
import org.keycloak.broker.provider.IdentityBrokerException;
import org.keycloak.broker.social.SocialIdentityProvider;
import org.keycloak.connections.httpclient.HttpClientProvider;
import org.keycloak.events.EventBuilder;
import org.keycloak.models.KeycloakSession;
import org.keycloak.util.JsonSerialization;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;

import java.io.InputStream;

public class YandexIdentityProvider
        extends AbstractOAuth2IdentityProvider<OIDCIdentityProviderConfig>
        implements SocialIdentityProvider<OIDCIdentityProviderConfig> {

    public static final String AUTH_URL      = "https://oauth.yandex.ru/authorize";
    public static final String TOKEN_URL     = "https://oauth.yandex.ru/token";
    public static final String PROFILE_URL   = "https://login.yandex.ru/info?format=json";
    public static final String DEFAULT_SCOPE = "login:info login:email";

    public YandexIdentityProvider(KeycloakSession session, OIDCIdentityProviderConfig config) {
        super(session, config);
        config.setAuthorizationUrl(AUTH_URL);
        config.setTokenUrl(TOKEN_URL);
        config.setUserInfoUrl(PROFILE_URL);
    }

    @Override
    protected String getDefaultScopes() {
        return DEFAULT_SCOPE;
    }

    @Override
    protected BrokeredIdentityContext doGetFederatedIdentity(String accessToken) {
        try {
            HttpClient httpClient = session.getProvider(HttpClientProvider.class).getHttpClient();
            HttpGet get = new HttpGet(PROFILE_URL);
            get.setHeader("Authorization", "OAuth " + accessToken);
            get.setHeader("Accept", "application/json");

            HttpResponse response = httpClient.execute(get);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IdentityBrokerException(
                    "Yandex profile request failed: HTTP " + response.getStatusLine().getStatusCode());
            }

            try (InputStream content = response.getEntity().getContent()) {
                JsonNode profile = JsonSerialization.mapper.readTree(content);
                return extractIdentityFromProfile(null, profile);
            }
        } catch (IdentityBrokerException e) {
            throw e;
        } catch (Exception e) {
            throw new IdentityBrokerException("Could not obtain user profile from Yandex", e);
        }
    }

    protected BrokeredIdentityContext extractIdentityFromProfile(EventBuilder event, JsonNode profile) {
        String id        = getJsonProperty(profile, "id");
        String login     = getJsonProperty(profile, "login");
        String email     = getJsonProperty(profile, "default_email");
        String firstName = getJsonProperty(profile, "first_name");
        String lastName  = getJsonProperty(profile, "last_name");
        String realName  = getJsonProperty(profile, "real_name");

        BrokeredIdentityContext identity = new BrokeredIdentityContext(id);
        identity.setIdpConfig(getConfig());
        identity.setIdp(this);
        identity.setUsername(login != null ? login : email);
        identity.setEmail(email);

        if (firstName != null && !firstName.isEmpty()) {
            identity.setFirstName(firstName);
        } else if (realName != null && !realName.isEmpty()) {
            identity.setFirstName(realName);
        }
        if (lastName != null) {
            identity.setLastName(lastName);
        }

        AbstractJsonUserAttributeMapper.storeUserProfileForMapper(identity, profile, getConfig().getAlias());
        return identity;
    }
}
