/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.dashboards.core;

import com.google.gson.JsonElement;
import feign.Response;
import feign.RetryableException;
import feign.gson.GsonDecoder;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.analytics.idp.client.core.api.AnalyticsHttpClientBuilderService;
import org.wso2.carbon.config.ConfigurationException;
import org.wso2.carbon.config.provider.ConfigProvider;
import org.wso2.carbon.dashboards.core.bean.DashboardMetadata;
import org.wso2.carbon.dashboards.core.bean.DashboardMetadataContent;
import org.wso2.carbon.dashboards.core.bean.authorizer.TenantIdInfo;
import org.wso2.carbon.dashboards.core.bean.importer.WidgetType;
import org.wso2.carbon.dashboards.core.bean.widget.WidgetConfigs;
import org.wso2.carbon.dashboards.core.bean.widget.WidgetMetaInfo;
import org.wso2.carbon.dashboards.core.exception.DashboardException;
import org.wso2.carbon.dashboards.core.exception.UnauthorizedException;
import org.wso2.carbon.dashboards.core.internal.authorizer.DashboardAuthorizerServiceFactory;
import org.wso2.carbon.data.provider.DataProviderAuthorizer;
import org.wso2.carbon.data.provider.bean.DataProviderConfigRoot;
import org.wso2.carbon.data.provider.exception.DataProviderException;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.wso2.carbon.dashboards.core.utils.DashboardUtil.findWidgets;

/**
 * Default implementation for Data provider Authorizer.
 */
@Component(
        service = DataProviderAuthorizer.class,
        immediate = true
)
public class DashboardDataProviderAuthorizer implements DataProviderAuthorizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DashboardDataProviderAuthorizer.class);
    private static final String AUTH_CONFIGS_HEADER = "auth.configs";
    private static final String AUTH_CONFIGS_PROPERTIES_HEADER = "properties";
    private static final String ADMIN_SERVICE_BASE_URL_KEY = "adminServiceBaseUrl";
    private static final String ADMIN_USERNAME_KEY = "adminUsername";
    private static final String ADMIN_PASSWORD_KEY = "adminPassword";
    private static final String MAIN_CONFIG = "configs";
    private static final String DATA_PROVIDER_CONFIG = "config";
    private static final String QUERY_DATA = "queryData";
    private static final String QUERY_VALUES = "queryValues";
    private static final String QUERY_NAME = "queryName";
    private static final String QUERY_PROPERTY_NAME = "query";
    private static final String NOT_LIKE_CONTEXT_PATH = "not like \'/t/%\'";
    private static final String LIKE_CONTEXT_PATH = "like \'/t/{{tenantDomain}}/%\'";
    private static final String STRING_NOT_CONTAIN_CONTEXT = "NOT(str:contains(CONTEXT,\'/t/\'))";
    private static final String STRING_CONTAIN_CONTEXT = "(str:contains(CONTEXT,\'/t/{{tenantDomain}}\'))";
    private static final String CONTEXT_CONDITION_KEY = "{{contextCondition}}";
    private static final String CONTEXT_CONTAINS_CONDITION_KEY = "{{contextContainsCondition}}";
    private static final String TENANT_DOMAIN_KEY = "{{tenantDomain}}";
    private static final String TENANT_ID_KEY = "{{tenantId}}";
    private static final String SUPER_TENANT_DOMAIN = "carbon.super";

    private AnalyticsHttpClientBuilderService clientBuilderService;
    private DashboardMetadataProvider dashboardMetadataProvider;
    private ConfigProvider configProvider;

    @Reference(service = DashboardMetadataProvider.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetDashboardMetadataProvider")
    protected void setDashboardMetadataProvider(DashboardMetadataProvider dashboardDataProvider) {
        this.dashboardMetadataProvider = dashboardDataProvider;
        LOGGER.debug("DashboardMetadataProvider '{}' registered.", dashboardDataProvider.getClass().getName());
    }

    protected void unsetDashboardMetadataProvider(DashboardMetadataProvider dashboardDataProvider) {
        this.dashboardMetadataProvider = null;
        LOGGER.debug("DashboardMetadataProvider '{}' unregistered.", dashboardDataProvider.getClass().getName());
    }

    @Reference(
            name = "carbon.anaytics.common.clientservice",
            service = AnalyticsHttpClientBuilderService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unregisterAnalyticsHttpClient"
    )
    protected void registerAnalyticsHttpClient(AnalyticsHttpClientBuilderService service) {
        this.clientBuilderService = service;
        LOGGER.debug("AnalyticsHttpClientBuilderService '{}' registered.", service.getClass().getName());
    }

    protected void unregisterAnalyticsHttpClient(AnalyticsHttpClientBuilderService service) {
        this.clientBuilderService = null;
        LOGGER.debug("AnalyticsHttpClientBuilderService '{}' unregistered.", service.getClass().getName());
    }

    @Reference(service = ConfigProvider.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetConfigProvider")
    protected void setConfigProvider(ConfigProvider configProvider) {
        this.configProvider = configProvider;
        LOGGER.debug("ConfigProvider '{}' registered.", configProvider.getClass().getName());
    }

    protected void unsetConfigProvider(ConfigProvider configProvider) {
        this.configProvider = null;
        LOGGER.debug("ConfigProvider '{}' unregistered.", configProvider.getClass().getName());
    }

    @Override
    public boolean authorize(DataProviderConfigRoot dataProviderConfigRoot) throws DataProviderException {
        // If the action is UNSUBSCRIBE, then allow it. In here, UI won't send username, dashboardId and widgetName.
        if (dataProviderConfigRoot.getAction().equalsIgnoreCase(DataProviderConfigRoot.Types.UNSUBSCRIBE.toString())) {
            return true;
        }
        String dashboardId = dataProviderConfigRoot.getDashboardId();
        String username = dataProviderConfigRoot.getUsername();
        String widgetName = dataProviderConfigRoot.getWidgetName();
        if (dashboardId == null || dashboardId.isEmpty()) {
            String error = "Dashboard Id in the Data Provider Config cannot be empty.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }
        if (username == null || username.isEmpty()) {
            String error = "Username in the Data Provider Config cannot be empty.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }
        if (widgetName == null || widgetName.isEmpty()) {
            String error = "Widget Name in the Data Provider Config cannot be empty.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }

        LOGGER.info("Widget Name: " + widgetName);
        Optional<DashboardMetadata> dashboardMetadata;
        try {
            dashboardMetadata
                    = this.dashboardMetadataProvider.getDashboardByUser(username, dashboardId, null);
        } catch (UnauthorizedException e) {
            return false;
        } catch (DashboardException e) {
            String error = e.getMessage();
            LOGGER.error(error, e);
            throw new DataProviderException(error, e);
        }

        if (!dashboardMetadata.isPresent()) {
            return false;
        }
        DashboardMetadataContent dashboardMetadataContent = dashboardMetadata.get().getContent();
        Map<WidgetType, Set<String>> widgets  = findWidgets(dashboardMetadataContent);

        boolean isWidgetAvailableInDashboard = false;
        // Check whether the requested widget is in the CUSTOM type set
        for (String widget : widgets.get(WidgetType.CUSTOM)) {
            if (widget.equalsIgnoreCase(widgetName)) {
                isWidgetAvailableInDashboard = true;
                break;
            }
        }
        // Check whether the requested widget is in the GENERATED type set
        if (!isWidgetAvailableInDashboard) {
            for (String widget : widgets.get(WidgetType.GENERATED)) {
                if (widget.equalsIgnoreCase(widgetName)) {
                    isWidgetAvailableInDashboard = true;
                    break;
                }
            }
        }

        if (!isWidgetAvailableInDashboard) {
            // Widget is not available in the dashboard. Hence authorization is failed.
            return false;
        }

        WidgetMetadataProvider widgetMetadataProvider = this.dashboardMetadataProvider.getWidgetMetadataProvider();
        Optional<WidgetMetaInfo> widgetMetaInfo;
        try {
            widgetMetaInfo = widgetMetadataProvider.getWidgetConfiguration(widgetName);
        } catch (DashboardException e) {
            LOGGER.error(e.getMessage(), e);
            throw new DataProviderException(e.getMessage(), e);
        }

        if (!widgetMetaInfo.isPresent()) {
            String error = "Widget configuration cannot be found.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }
        WidgetConfigs widgetConfigs = widgetMetaInfo.get().getConfigs();
        JsonElement dataProviderConfig = widgetConfigs.getProviderConfig();
        assembleQuery(username, dataProviderConfigRoot, dataProviderConfig);
        LOGGER.debug("Authorized via the '{}' class.", this.getClass().getName());
        return true;
    }

    /**
     * This method replaces the template values in the query with the values sent from front-end.
     *
     * @param username name of the logged in user
     * @param dataProviderConfigRoot root configuration for the data provider (comes from the front-end)
     * @param dataProviderConfig data provider config obtained through widget conf. (read by back-end)
     **/
    private void assembleQuery(String username, DataProviderConfigRoot dataProviderConfigRoot,
                               JsonElement dataProviderConfig) throws DataProviderException {
        JsonElement queryData; // query data obtained through reading the widget conf
        String queryName; // name of the query need to be run
        JsonElement queryValues = null; // values needed to be replaced in the query as key/value pairs
        String query;
        String contextPath;
        String contextContainsCondition;

        // As an example if the username is "admin@carbon.super", the tenant domain will be extracted as "carbon.super".
        String[] usernameSections = username.split("@");
        String tenantDomain = usernameSections[usernameSections.length - 1];

        if (dataProviderConfig.getAsJsonObject().get(MAIN_CONFIG) != null
                && dataProviderConfig.getAsJsonObject().get(MAIN_CONFIG).getAsJsonObject()
                .get(DATA_PROVIDER_CONFIG) != null
                && dataProviderConfig.getAsJsonObject().get(MAIN_CONFIG).getAsJsonObject()
                .get(DATA_PROVIDER_CONFIG).getAsJsonObject().get(QUERY_DATA) != null) {
            queryData = dataProviderConfig.getAsJsonObject().get(MAIN_CONFIG).getAsJsonObject()
                    .get(DATA_PROVIDER_CONFIG).getAsJsonObject().get(QUERY_DATA);
        } else {
            String error = "Cannot find the query data in the widget configuration.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }

        // capture the query name sent from front-end
        if (dataProviderConfigRoot.getDataProviderConfiguration() != null &&
                dataProviderConfigRoot.getDataProviderConfiguration().getAsJsonObject().get(QUERY_DATA) != null &&
                dataProviderConfigRoot.getDataProviderConfiguration().getAsJsonObject().get(QUERY_DATA)
                        .getAsJsonObject().get(QUERY_NAME) != null) {
            queryName = dataProviderConfigRoot.getDataProviderConfiguration()
                    .getAsJsonObject().get(QUERY_DATA).getAsJsonObject().get(QUERY_NAME).getAsString();
        } else {
            String error = "Query Name cannot be found in the data provider configuration root.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }

        // get the query need to be run, from widget conf read from backend
        if (queryName != null && !queryName.isEmpty() && queryData.getAsJsonObject().get(queryName) != null) {
            query = queryData.getAsJsonObject().get(queryName).getAsString();
        } else {
            String error = "Cannot find the query in the widget configuration.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }

        // capture the query values sent from front-end
        if (dataProviderConfigRoot.getDataProviderConfiguration() != null &&
                dataProviderConfigRoot.getDataProviderConfiguration().getAsJsonObject().get(QUERY_DATA) != null &&
                dataProviderConfigRoot.getDataProviderConfiguration().getAsJsonObject().get(QUERY_DATA)
                        .getAsJsonObject().get(QUERY_VALUES) != null) {
            queryValues = dataProviderConfigRoot.getDataProviderConfiguration()
                    .getAsJsonObject().get(QUERY_DATA).getAsJsonObject().get(QUERY_VALUES);
        }

        if (queryValues != null) {
            // replace the values in the query
            for (String key : queryValues.getAsJsonObject().keySet()) {
                String keyValue = queryValues.getAsJsonObject().get(key).getAsString();
                if (keyValue == null) {
                    String error = "Cannot find the replaceable value for " + key + ".";
                    LOGGER.error(error);
                    throw new DataProviderException(error);
                }
                query = query.replace(key, keyValue);
            }
        }

        if (tenantDomain != null && !tenantDomain.isEmpty()) {
            if (tenantDomain.equalsIgnoreCase(SUPER_TENANT_DOMAIN)) {
                contextPath = NOT_LIKE_CONTEXT_PATH;
                contextContainsCondition = STRING_NOT_CONTAIN_CONTEXT;
            } else {
                contextPath = LIKE_CONTEXT_PATH;
                contextContainsCondition = STRING_CONTAIN_CONTEXT;
            }
            String tenantId = getTenantId(username);
            query = query.replace(CONTEXT_CONDITION_KEY, contextPath)
                    .replace(CONTEXT_CONTAINS_CONDITION_KEY, contextContainsCondition)
                    .replace(TENANT_DOMAIN_KEY, tenantDomain)
                    .replace(TENANT_ID_KEY, tenantId);
        }

        Objects.requireNonNull(dataProviderConfigRoot.getDataProviderConfiguration()).getAsJsonObject()
                .get(QUERY_DATA).getAsJsonObject().addProperty(QUERY_PROPERTY_NAME, query);
    }

    /**
     * This method replaces the template values in the query with the values sent from front-end.
     *
     * @param username name of the logged in user
     * @return id of the tenant
     **/
    private String getTenantId(String username) throws DataProviderException {
        String adminServiceUrl;
        String adminUsername;
        String adminPassword;
        try {
            Map authConfigs = (Map) this.configProvider.getConfigurationObject(AUTH_CONFIGS_HEADER);
            if (authConfigs == null) {
                String error = "Cannot find " + AUTH_CONFIGS_HEADER + " in the deployment.yaml file.";
                LOGGER.error(error);
                throw new DataProviderException(error);
            }
            if (authConfigs.containsKey(AUTH_CONFIGS_PROPERTIES_HEADER)) {
                Map properties = (Map) authConfigs.get(AUTH_CONFIGS_PROPERTIES_HEADER);
                if (properties == null) {
                    String error = AUTH_CONFIGS_PROPERTIES_HEADER + " header under " + AUTH_CONFIGS_HEADER + " in " +
                            "the deployment.yaml file cannot be empty";
                    LOGGER.error(error);
                    throw new DataProviderException(error);
                }
                adminServiceUrl = getPropertyValueFromParentMap(properties, ADMIN_SERVICE_BASE_URL_KEY);
                adminUsername = getPropertyValueFromParentMap(properties, ADMIN_USERNAME_KEY);
                adminPassword = getPropertyValueFromParentMap(properties, ADMIN_PASSWORD_KEY);
            } else {
                String error = "Cannot find " + AUTH_CONFIGS_PROPERTIES_HEADER + " header under the "
                        + AUTH_CONFIGS_HEADER + " in the deployment.yaml file.";
                LOGGER.error(error);
                throw new DataProviderException(error);
            }
        } catch (ConfigurationException e) {
            String error = "Error occurred while getting the " + AUTH_CONFIGS_HEADER + " configuration from " +
                    "deployment.yaml file.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }
        try {
            Response response = DashboardAuthorizerServiceFactory.getAuthorizerHttpsClient(
                    this.clientBuilderService, adminServiceUrl, adminUsername, adminPassword).getTenantId(username);
            if (response == null) {
                String error = "Response returned from the admin rest api is null.";
                LOGGER.error(error);
                throw new DataProviderException(error);
            } else {
                if (response.status() == 200) {
                    TenantIdInfo tenantIdInfo = (TenantIdInfo) new GsonDecoder().decode(response, TenantIdInfo.class);
                    String tenantId = tenantIdInfo.getTenantId().toString();
                    if (tenantId.isEmpty()) {
                        String error = "Tenant Id cannot be found.";
                        LOGGER.error(error);
                        throw new DataProviderException(error);
                    }
                    return tenantId;
                } else if (response.status() == 401) {
                    String error
                            = "Unauthorized to get response from admin rest api. Status Code: " + response.status();
                    LOGGER.error(error);
                    throw new DataProviderException(error);
                } else {
                    String error = "Unknown Error occurred while getting response from admin rest api. Status Code: "
                            + response.status();
                    LOGGER.error(error);
                    throw new DataProviderException(error);
                }
            }
        } catch (RetryableException e) {
            String error = "Unable to reach the admin rest api.";
            LOGGER.error(error, e);
            throw new DataProviderException(error, e);
        } catch (IOException e) {
            String error = "Error occurred while parsing the admin rest api response.";
            LOGGER.error(error, e);
            throw new DataProviderException(error, e);
        }
    }

    /**
     * This method gets property values from the given parent map. This method also validates whether the given key and
     * the corresponding value is not empty.
     *
     * @param parentConfigMap parent configuration map
     * @param keyToBeChecked the key name need to be checked and retrieve the value for
     * @return property value
     **/
    private String getPropertyValueFromParentMap(Map parentConfigMap, String keyToBeChecked)
            throws DataProviderException {
        if (parentConfigMap.containsKey(keyToBeChecked)) {
            String value = (String) parentConfigMap.get(keyToBeChecked);
            if (value == null || value.isEmpty()) {
                String error = "Value of the property '" + keyToBeChecked + "' cannot be empty. Please define the " +
                        "value for the property under " + AUTH_CONFIGS_HEADER + " in the deployment.yaml file.";
                LOGGER.error(error);
                throw new DataProviderException(error);
            }
            return value;
        } else {
            String error = "Cannot find property " + keyToBeChecked + " under " + AUTH_CONFIGS_HEADER + " in the " +
                    "deployment.yaml file.";
            LOGGER.error(error);
            throw new DataProviderException(error);
        }
    }
}
