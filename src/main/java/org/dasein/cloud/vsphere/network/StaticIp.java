/**
 * Copyright (C) 2010-2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.vsphere.network;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.AddressType;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.IPAddressCapabilities;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.IpForwardingRule;
import org.dasein.cloud.network.Protocol;
import org.dasein.cloud.vsphere.PrivateCloud;

/**
 * Does absolutely nothing in the hopes that one day we might be able to add more complicated networking support
 * for VMware environments.
 * @author George Reese (george.reese@imaginary.com)
 * @version 2012.02
 */
public class StaticIp implements IpAddressSupport {
    @SuppressWarnings({"unused", "FieldCanBeLocal"})
    private PrivateCloud provider;
    
    StaticIp(@Nonnull PrivateCloud cloud) { provider = cloud; }
    
    @Override
    public void assign(@Nonnull String addressId, @Nonnull String toServerId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("No assignment of IP addresses");
    }

    @Override
    public void assignToNetworkInterface(@Nonnull String addressId, @Nonnull String nicId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Not yet supported");
    }

    @Override
    public @Nonnull String forward(@Nonnull String addressId, int publicPort, @Nonnull Protocol protocol, @SuppressWarnings("NullableProblems") int privatePort, @Nonnull String onServerId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("IP address forwarding is not supported with vSphere");
    }

    private transient volatile StaticIPCapabilities capabilities;
    @Nonnull
    @Override
    public IPAddressCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new StaticIPCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public @Nullable IpAddress getIpAddress(@Nonnull String addressId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForIpAddress(@Nonnull Locale locale) {
        return "IP address";
    }

    @Override
    public @Nonnull Requirement identifyVlanForVlanIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isAssigned(@Nonnull AddressType type) {
        return false;
    }

    @Override
    public boolean isAssigned(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isAssignablePostLaunch(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isForwarding() {
        return false;
    }

    @Override
    public boolean isForwarding(IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull AddressType type) {
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return false;
    }
    
    @Override
    public @Nonnull Iterable<IpAddress> listPrivateIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        return Collections.emptyList();
    }
    
    @Override
    public @Nonnull Iterable<IpAddress> listPublicIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IpAddress> listIpPool(@Nonnull IPVersion version, boolean unassignedOnly) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listIpPoolStatus(@Nonnull IPVersion version) throws InternalException, CloudException {
        ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

        for( IpAddress addr : listIpPool(version, false) ) {
            status.add(new ResourceStatus(addr.getProviderIpAddressId(), !addr.isAssigned()));
        }
        return status;
    }

    @Override
    public @Nonnull Iterable<IpForwardingRule> listRules(@Nonnull String addressId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public @Nonnull String request(@Nonnull AddressType type) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Unable to allocate new IP addresses.");
    }

    @Override
    public @Nonnull String request(@Nonnull IPVersion version) throws InternalException, CloudException {
        throw new OperationNotSupportedException("No support yet for requesting IP addresses");
    }

    @Override
    public @Nonnull String requestForVLAN(@Nonnull IPVersion version) throws InternalException, CloudException {
        throw new OperationNotSupportedException("No support yet for requesting IP addresses");
    }

    @Override
    public @Nonnull String requestForVLAN(@Nonnull IPVersion version, @Nonnull String vlanId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("No support for VLAN IP addresses");
    }

    @Override
    public void releaseFromPool(@Nonnull String addressId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Unable to delete the specified address.");
    }
    
    @Override
    public void releaseFromServer(@Nonnull String addressId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Unable to release from server");
    }

    @Override
    public void stopForward(@Nonnull String addressId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Unable to stop forwarding");
    }

    @Override
    public boolean supportsVLANAddresses(@Nonnull IPVersion ofVersion) throws InternalException, CloudException {
        return false;
    }
}
