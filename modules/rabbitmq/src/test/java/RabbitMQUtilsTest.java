/**
Copyright (c) 2022, WSO2 LLC. (http://wso2.com) All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.description.ParameterIncludeImpl;
import org.apache.axis2.transport.rabbitmq.AxisRabbitMQException;
import org.apache.axis2.transport.rabbitmq.RabbitMQConnectionFactory;
import org.apache.axis2.transport.rabbitmq.RabbitMQUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.wso2.securevault.XMLSecretResolver;

import java.util.ArrayList;

@RunWith(PowerMockRunner.class)
public class RabbitMQUtilsTest extends TestCase {

    @Test
    public void testResolveTransportDescription() throws AxisFault, AxisRabbitMQException {
        String value = "$secret{password}";
        String name = "secret";
        String password = "plainPassword";
        Parameter param = Mockito.mock(Parameter.class);
        XMLSecretResolver secretResolver = Mockito.mock(XMLSecretResolver.class);
        OMElement paramElement = Mockito.mock(OMElement.class);
        ParameterIncludeImpl pii = Mockito.mock(ParameterIncludeImpl.class);
        ParameterInclude pi = Mockito.mock(ParameterInclude.class);
        ArrayList<Parameter> params = new ArrayList<>();
        params.add(param);
        Mockito.when(pi.getParameters()).thenReturn(params);
        Mockito.doNothing().when(pii).deserializeParameters(Mockito.any());
        Mockito.when(pii.getParameters()).thenReturn(params);
        Mockito.when(param.getValue()).thenReturn(paramElement);
        Mockito.when(param.getParameterElement()).thenReturn(paramElement);
        Mockito.when(paramElement.getAttribute(Mockito.any())).thenReturn(null);
        Mockito.when(secretResolver.isInitialized()).thenReturn(true);
        Mockito.when(paramElement.getText()).thenReturn(value);
        Mockito.when(secretResolver.resolve(Mockito.anyString())).thenReturn(password);
        Mockito.when(secretResolver.getSecureVaultNamespace()).thenReturn("");
        Mockito.when(secretResolver.getSecureVaultAlias()).thenReturn("");
        Mockito.when(secretResolver.isTokenProtected(Mockito.any())).thenReturn(true);
        Mockito.when(param.getName()).thenReturn(name);
        RabbitMQConnectionFactory rabbitMQConnectionFactory = new RabbitMQConnectionFactory();
        RabbitMQUtils.resolveTransportDescription(pi, secretResolver, rabbitMQConnectionFactory, pii);
        String resolvedValue = rabbitMQConnectionFactory.getConnectionFactoryConfigurations().get(name).get(name);
        Assert.assertEquals("Resolved password is not equal to the actual password.", password, resolvedValue);
    }
}
