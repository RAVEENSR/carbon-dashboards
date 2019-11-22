/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.dashboards.core.internal;

import org.wso2.carbon.dashboards.core.WidgetMetadataProvider;
import org.wso2.carbon.dashboards.core.bean.DashboardConfigurations;
import org.wso2.carbon.dashboards.core.bean.importer.WidgetType;
import org.wso2.carbon.dashboards.core.bean.widget.WidgetMetaInfo;
import org.wso2.carbon.dashboards.core.exception.DashboardException;
import org.wso2.carbon.dashboards.core.exception.DashboardRuntimeException;
import org.wso2.carbon.dashboards.core.internal.database.WidgetMetadataDao;
import org.wso2.carbon.dashboards.core.internal.database.WidgetMetadataDaoFactory;
import org.wso2.carbon.dashboards.core.internal.io.WidgetConfigurationReader;
import org.wso2.carbon.datasource.core.api.DataSourceService;
import org.wso2.carbon.uiserver.api.App;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Default widget info provider.
 *
 * @since 4.0.0
 */
public class WidgetMetadataProviderImpl implements WidgetMetadataProvider {

    private static final String EXTENSION_TYPE_WIDGETS = "widgets";

    private final App dashboardApp;
    private final WidgetMetadataDao widgetMetadataDao;

    public WidgetMetadataProviderImpl(App dashboardApp, DataSourceService dataSourceService,
                                      DashboardConfigurations dashboardConfigurations) {
        this.dashboardApp = dashboardApp;
        try {
            this.widgetMetadataDao = WidgetMetadataDaoFactory.createDao(dataSourceService, dashboardConfigurations);
            this.widgetMetadataDao.initWidgetTable();
        } catch (DashboardException e) {
            throw new DashboardRuntimeException("Cannot create widget DAO for DB access.", e);
        }
    }

    WidgetMetadataProviderImpl(App dashboardApp, WidgetMetadataDao dao) {
        this.dashboardApp = dashboardApp;
        this.widgetMetadataDao = dao;
    }

    @Override
    public Optional<WidgetMetaInfo> getWidgetConfiguration(String widgetId) throws DashboardException {
        return dashboardApp.getExtension(EXTENSION_TYPE_WIDGETS, widgetId)
                .map(WidgetConfigurationReader::getConfiguration);
    }

    @Override
    public boolean isWidgetPresent(String widgetName) throws DashboardException {
        return isWidgetPresent(widgetName, WidgetType.CUSTOM);
    }

    @Override
    public boolean isWidgetPresent(String widgetName, WidgetType widgetType) throws DashboardException {
        // TODO: 02/05/19 Custom widget filtering also to be changed with widgetName 
        switch (widgetType) {
            case CUSTOM:
            case ALL:
                return isCustomWidgetPresent(widgetName);
        }
        return false;
    }

    private boolean isCustomWidgetPresent(String widgetName) {
        return dashboardApp.getExtension(EXTENSION_TYPE_WIDGETS, widgetName).isPresent();
    }

    @Override
    public Set<WidgetMetaInfo> getAllWidgetConfigurations() throws DashboardException {
        return dashboardApp.getExtensions(EXTENSION_TYPE_WIDGETS).stream()
                .map(WidgetConfigurationReader::getConfiguration)
                .collect(Collectors.toSet());
    }

    @Override
    public void delete(String widgetId) throws DashboardException {
        widgetMetadataDao.delete(widgetId);
    }
}
