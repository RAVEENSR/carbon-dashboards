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
package org.wso2.carbon.dashboards.core.internal.database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.carbon.dashboards.core.exception.DashboardException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * This is a core class of the WidgetMetadataDao JDBC Based implementation.
 */
public class WidgetMetadataDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(WidgetMetadataDao.class);

    private final DataSource dataSource;
    private final QueryManager queryManager;

    public WidgetMetadataDao(DataSource dataSource, QueryManager queryManager) {
        this.dataSource = dataSource;
        this.queryManager = queryManager;
    }

    public void initWidgetTable() throws DashboardException {
        if (!tableExists(QueryManager.WIDGET_RESOURCE_TABLE)) {
            this.createWidgetResourceTable();
        }
    }

    /**
     * Create widget resource table.
     */
    private void createWidgetResourceTable() throws DashboardException {
        Connection connection = null;
        PreparedStatement ps = null;
        String query = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            query = queryManager.getQuery(connection, QueryManager.CREATE_WIDGET_RESOURCE_TABLE);
            ps = connection.prepareStatement(query);
            ps.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.debug("Failed to execute SQL query {}", query);
            throw new DashboardException("Unable to create the '" + QueryManager.WIDGET_RESOURCE_TABLE +
                    "' table.", e);
        } finally {
            closeQuietly(connection, ps, null);
        }
    }

    /**
     * Method for checking whether or not the given table (which reflects the current event table instance) exists.
     *
     * @return true/false based on the table existence.
     */
    private boolean tableExists(String tableName) {
        Connection connection = null;
        PreparedStatement ps = null;
        String query = null;
        try {
            connection = getConnection();
            query = queryManager.getQuery(connection, QueryManager.TABLE_CHECK);
            ps = connection.prepareStatement(query.replace(QueryManager.TABLE_NAME_PLACEHOLDER, tableName));
            return ps.execute();
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.debug("Table '{}' assumed to not exist since its existence check query {} resulted "
                    + "in exception {}.", tableName, query, e.getMessage());
            return false;
        } finally {
            closeQuietly(connection, ps, null);
        }
    }

    private Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void delete(String widgetId) throws DashboardException {
        Connection connection = null;
        PreparedStatement ps = null;
        String query = null;
        try {
            connection = getConnection();
            query = queryManager.getQuery(connection, QueryManager.DELETE_WIDGET_BY_ID);
            connection.setAutoCommit(false);
            ps = connection.prepareStatement(query);
            ps.setString(1, widgetId);
            ps.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            rollbackQuietly(connection);
            LOGGER.debug("Failed to execute SQL query {}", query);
            throw new DashboardException("Cannot delete widget id: '" + widgetId + "'.", e);
        } finally {
            closeQuietly(connection, ps, null);
        }
    }

    static void closeQuietly(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOGGER.error("An error occurred when closing result set.", e);
            }
        }
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                LOGGER.error("An error occurred when closing prepared statement.", e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.error("An error occurred when closing DB connection.", e);
            }
        }
    }

    static void rollbackQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (SQLException e) {
                LOGGER.error("An error occurred when rollbacking DB connection.", e);
            }
        }
    }
}
