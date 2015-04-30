/**
 * Copyright (C) 2010-2015 Dell, Inc
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

import com.vmware.vim25.*;
import com.vmware.vim25.mo.*;
import org.apache.log4j.Logger;
import org.dasein.cloud.*;
import org.dasein.cloud.Tag;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.vsphere.PrivateCloud;
import org.dasein.cloud.vsphere.compute.Vm;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.rmi.RemoteException;
import java.util.*;

/**
 * User: daniellemayne
 * Date: 11/06/2014
 * Time: 12:33
 */
public class VSphereNetwork extends AbstractVLANSupport{

    private PrivateCloud provider;
    static private final Logger log = PrivateCloud.getLogger(VSphereNetwork.class, "std");

    VSphereNetwork(PrivateCloud provider) {
        super(provider);
        this.provider = provider;
    }

    private @Nonnull ServiceInstance getServiceInstance() throws CloudException, InternalException {
        ServiceInstance instance = provider.getServiceInstance();

        if( instance == null ) {
            throw new CloudException(CloudErrorType.AUTHENTICATION, HttpServletResponse.SC_UNAUTHORIZED, null, "Unauthorized");
        }
        return instance;
    }

    private transient volatile VSphereNetworkCapabilities capabilities;

    @Override
    public VLANCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new VSphereNetworkCapabilities(provider);
        }
        return capabilities;
    }

    @Nonnull @Override
    public String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return "nic";
    }

    @Nonnull @Override
    public String getProviderTermForSubnet(@Nonnull Locale locale) {
        return "network";
    }

    @Nonnull @Override
    public String getProviderTermForVlan(@Nonnull Locale locale) {
        return "network";
    }

    @Nullable @Override
    public String getAttachedInternetGatewayId(@Nonnull String vlanId) throws CloudException, InternalException {
        return null;
    }

    @Nullable @Override
    public InternetGateway getInternetGatewayById(@Nonnull String gatewayId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Nonnull
    @Override
    public Collection<InternetGateway> listInternetGateways(@Nullable String vlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public void removeInternetGatewayById(@Nonnull String id) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Internet gateways not supported in vSphere");
    }

    @Override
    public void removeInternetGatewayTags(@Nonnull String internetGatewayId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //NO-OP
    }

    @Override
    public void removeRoutingTableTags(@Nonnull String routingTableId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //NO-OP
    }

    @Override
    public void updateRoutingTableTags(@Nonnull String routingTableId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //NO-OP
    }

    @Override
    public void updateInternetGatewayTags(@Nonnull String internetGatewayId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //NO-OP
    }

    @Nonnull
    @Override
    public Iterable<VLAN> listVlans() throws CloudException, InternalException {
        APITrace.begin(provider, "Network.listVlans");

        try {
            ServiceInstance instance = getServiceInstance();

            List<VLAN> networkList = new ArrayList<VLAN>();
            Map<String, String> dvsMap = new HashMap<String, String>();
            Datacenter dc;
            Network[] nets;
            String rid = getContext().getRegionId();
            if( rid != null ) {
                dc = provider.getDataCenterServices().getVmwareDatacenterFromVDCId(instance, rid);

                try {
                    nets = dc.getNetworks();
                    if (nets != null) {
                        for( Network network : nets ) {
                            if (network.getMOR().getType().equals("Network")) {
                                networkList.add(toVlan(network));
                            }
                            else if( network.getMOR().getType().equals("DistributedVirtualPortgroup") ) {
                                DistributedVirtualPortgroup dvp = ( DistributedVirtualPortgroup ) network;
                                ManagedObjectReference mor = dvp.getConfig().getDistributedVirtualSwitch();
                                DistributedVirtualSwitch dvs = new DistributedVirtualSwitch(instance.getServerConnection(), mor);
                                if (!dvsMap.containsKey(dvs.getName())) {
                                    dvsMap.put(dvs.getName(),dvs.getName());
                                    networkList.add(toVlan(dvs));
                                }
git                            }
                        }
                    }
                    else {
                        log.debug("dc.getNetworks() returned null");
                    }
                }
                catch( InvalidProperty e ) {
                    throw new CloudException("No network support in cluster: " + e.getMessage());
                }
                catch( RuntimeFault e ) {
                    throw new CloudException("Error in processing request to cluster: " + e.getMessage());
                }
                catch( RemoteException e ) {
                    throw new CloudException("Error in cluster processing request: " + e.getMessage());
                }
            }
            log.debug("listVlans() returning " + networkList.size() + " elements");
            return networkList;
        }
        finally {
            APITrace.end();
        }
    }

    private VLAN toVlan(Network network) throws InternalException, CloudException {
        if (network != null) {
            VLAN vlan = new VLAN();
            vlan.setName(network.getName());
            vlan.setDescription(vlan.getName()+"("+network.getMOR().getVal()+")");
            vlan.setProviderVlanId(vlan.getName());
            vlan.setCidr("");
            vlan.setProviderRegionId(getContext().getRegionId());
            vlan.setProviderOwnerId(getContext().getAccountNumber());
            vlan.setSupportedTraffic(IPVersion.IPV4);
            vlan.setVisibleScope(VisibleScope.ACCOUNT_REGION);
            NetworkSummary s = network.getSummary();
            vlan.setCurrentState(VLANState.PENDING);
            if (s.isAccessible()) {
                vlan.setCurrentState(VLANState.AVAILABLE);
            }
            return vlan;
        }
        return null;
    }

    private VLAN toVlan(DistributedVirtualSwitch dvs) throws InternalException, CloudException {
        if( dvs == null ) {
            return null;
        }
        VLAN vlan = new VLAN();
        vlan.setName(dvs.getName());
        vlan.setDescription(vlan.getName() + "(" + dvs.getMOR().getVal() + ")");
        vlan.setProviderVlanId(vlan.getName());
        vlan.setCidr("");
        vlan.setProviderRegionId(getContext().getRegionId());
        DVSNetworkResourcePool[] pools = dvs.getNetworkResourcePool();
        if( pools != null && pools.length > 0 ) {
            vlan.setProviderDataCenterId(pools[0].getName());
        }
        vlan.setProviderOwnerId(getContext().getAccountNumber());
        vlan.setSupportedTraffic(IPVersion.IPV4);
        vlan.setVisibleScope(VisibleScope.ACCOUNT_REGION);
        vlan.setCurrentState(VLANState.AVAILABLE);
        vlan.setNetworkType("dvs");
        return vlan;
    }
}
