/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.axis2.transport.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * The rabbitmq channel factory implementation for rabbitmq channel pool
 */
public class RabbitMQChannelFactory extends BaseKeyedPooledObjectFactory<String, Channel> {

    private static final Log log = LogFactory.getLog(RabbitMQChannelFactory.class);
    private RabbitMQConnectionPool rabbitMQConnectionPool;

    public RabbitMQChannelFactory(RabbitMQConnectionPool rabbitMQConnectionPool) {
        this.rabbitMQConnectionPool = rabbitMQConnectionPool;
    }

    /**
     * Create an instance that can be served by the pool
     *
     * @param factoryName the factory name used when constructing the channel
     * @return a channel that can be served by the pool
     * @throws Exception
     */
    @Override
    public Channel create(String factoryName) throws Exception {
        Connection connection = rabbitMQConnectionPool.borrowObject(factoryName);
        try {
            Channel channel = connection.createChannel();
            return channel;
        } catch (Exception ex) {
            log.warn("Could not create a channel. Returning the connection to the pool for factory: "
                    + factoryName + " Connection: " + connection);
            throw ex;
        } finally {
            rabbitMQConnectionPool.returnObject(factoryName, connection);
        }
    }

    /**
     * Wrap the provided channel with an implementation of PooledObject
     *
     * @param channel the channel to wrap
     * @return The provided channel, wrapped by a PooledObject
     */
    @Override
    public PooledObject<Channel> wrap(Channel channel) {
        return new DefaultPooledObject<>(channel);
    }

    /**
     * Destroy a channel no longer needed by the pool
     *
     * @param factoryName  the key used when selecting the channel
     * @param pooledObject a PooledObject wrapping the channel to be destroyed
     * @throws Exception
     */
    @Override
    public void destroyObject(String factoryName, PooledObject<Channel> pooledObject) throws Exception {
        pooledObject.getObject().abort();
    }

    /**
     * Ensures that the channel is safe to be returned by the pool
     *
     * @param factoryName  the key used when selecting the channel
     * @param pooledObject a PooledObject wrapping the channel to be validated
     * @return true if the channel is opened
     */
    @Override
    public boolean validateObject(String factoryName, PooledObject<Channel> pooledObject) {
        return pooledObject.getObject().isOpen();
    }
}
