package org.dasein.cloud.vsphere.network;

import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.ManagedObjectReference;
import com.vmware.vim25.NetworkSummary;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.mo.*;
import org.dasein.cloud.*;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.vsphere.PrivateCloud;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Random;

/**
 * User: daniellemayne
 * Date: 11/06/2014
 * Time: 12:33
 */
public class VSphereNetwork extends AbstractVLANSupport{

    private PrivateCloud provider;

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

    @Nonnull
    @Override
    public String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return "nic";
    }

    @Nonnull
    @Override
    public String getProviderTermForSubnet(@Nonnull Locale locale) {
        return "network";
    }

    @Nonnull
    @Override
    public String getProviderTermForVlan(@Nonnull Locale locale) {
        return "network";
    }

    @Nullable
    @Override
    public String getAttachedInternetGatewayId(@Nonnull String vlanId) throws CloudException, InternalException {
        return null;
    }

    @Nullable
    @Override
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

            ArrayList<VLAN> networkList=new ArrayList<VLAN>();
            Datacenter dc;
            Network[] nets;
            String rid = getContext().getRegionId();
            if( rid != null ) {
                dc = provider.getDataCenterServices().getVmwareDatacenterFromVDCId(instance, rid);

                try {
                    nets = dc.getNetworks();
                    for(int d=0; d<nets.length; d++) {
                        if (nets[d].getMOR().getType().equals("Network")) {
                            networkList.add(toVlan(nets[d]));
                        }
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
}
