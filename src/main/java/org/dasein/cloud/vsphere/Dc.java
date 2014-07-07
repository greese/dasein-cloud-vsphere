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

package org.dasein.cloud.vsphere;

import java.net.URI;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

import com.vmware.vim25.ManagedEntityStatus;
import com.vmware.vim25.mo.ClusterComputeResource;
import com.vmware.vim25.mo.ResourcePool;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterCapabilities;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;

import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ServiceInstance;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

public class Dc implements DataCenterServices {

    private PrivateCloud provider;
    
    Dc(@Nonnull PrivateCloud cloud) {
        provider = cloud;
    }
    
    private @Nonnull ProviderContext getContext() throws CloudException {
        ProviderContext ctx = provider.getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        return ctx;
    }
    
    private @Nonnull ServiceInstance getServiceInstance() throws CloudException, InternalException {
        ServiceInstance instance = provider.getServiceInstance();
        
        if( instance == null ) {
            throw new CloudException(CloudErrorType.AUTHENTICATION, HttpServletResponse.SC_UNAUTHORIZED, null, "Unauthorized");            
        }
        return instance;
    }

    private transient volatile DCCapabilities capabilities;
    @Nonnull
    @Override
    public DataCenterCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new DCCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public @Nullable DataCenter getDataCenter(@Nonnull String dcId) throws InternalException, CloudException {
        String regionId = getContext().getRegionId();

        if( regionId == null ) {
            throw new CloudException("No region was specified for this request.");
        }
        for( DataCenter dc : listDataCenters(regionId) ) {
            if( dcId.equals(dc.getProviderDataCenterId()) ) {
                return dc;
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "data center";
    }

    @Override
    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "region";
    }

    @Override
    public @Nullable Region getRegion(@Nonnull String regionId) throws InternalException, CloudException {
        for( Region region : listRegions() ) {
            if( region.getProviderRegionId().equals(regionId) ) {
                return region;
            }
        }
        return null;
    }

    public @Nullable ResourcePool getResourcePoolFromClusterId(@Nonnull ServiceInstance service, @Nonnull String dcId) throws CloudException, InternalException {
        ServiceInstance instance = getServiceInstance();

        DataCenter dsdc = getDataCenter(dcId);

        if( dsdc == null ) {
            return null;
        }
        Datacenter dc = getVmwareDatacenterFromVDCId(instance, dsdc.getRegionId());

        if( dc == null ) {
            return null;
        }

        ManagedEntity[] clusters;

        try {
            clusters = new InventoryNavigator(dc).searchManagedEntities("ClusterComputeResource");
        }
        catch( InvalidProperty e ) {
            throw new CloudException("No virtual machine support in cluster: " + e.getMessage());
        }
        catch( RuntimeFault e ) {
            throw new CloudException("Error in processing request to cluster: " + e.getMessage());
        }
        catch( RemoteException e ) {
            throw new CloudException("Error in cluster processing request: " + e.getMessage());
        }
        for( ManagedEntity entity : clusters ) {
            ClusterComputeResource cluster = (ClusterComputeResource)entity;

            if( cluster.getName().equals(dcId) ) {
                return cluster.getResourcePool();
            }
        }
        return null;
    }

    public @Nullable Datacenter getVmwareDatacenterFromVDCId(@Nonnull ServiceInstance service, @Nonnull String dcId) throws CloudException, InternalException {
        Folder rootFolder = service.getRootFolder();
        
        try {
            return (Datacenter)(new InventoryNavigator(rootFolder).searchManagedEntity("Datacenter", dcId));
        }
        catch( InvalidProperty e ) {
            throw new InternalException("Invalid DC property: " + e.getMessage());
        }
        catch( RuntimeFault e ) {
            throw new InternalException("Error talking to the cluster: " + e.getMessage());
        }
        catch( RemoteException e ) {
            throw new CloudException("Error in processing the request in the cluster: " + e.getMessage());
        }
    }

    @Override
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String regionId) throws InternalException, CloudException {

        if( !provider.isClusterBased() ) {
            return listDataCentersFromVDCs(regionId);
        }
        else {
            return listDataCentersFromClusters(regionId);
        }
    }

    private @Nonnull Collection<DataCenter> listDataCentersFromClusters(@Nonnull String regionId) throws InternalException, CloudException {
        ArrayList<DataCenter> dataCenters = new ArrayList<DataCenter>();
        ServiceInstance instance = getServiceInstance();
        Datacenter dc = getVmwareDatacenterFromVDCId(instance, regionId);

        if( dc == null ) {
            throw new CloudException("No such region: " + regionId);
        }

        ManagedEntity[] clusters;

        try {
            clusters = new InventoryNavigator(dc).searchManagedEntities("ClusterComputeResource");
        }
        catch( InvalidProperty e ) {
            throw new CloudException("No virtual machine support in cluster: " + e.getMessage());
        }
        catch( RuntimeFault e ) {
            throw new CloudException("Error in processing request to cluster: " + e.getMessage());
        }
        catch( RemoteException e ) {
            throw new CloudException("Error in cluster processing request: " + e.getMessage());
        }
        for( ManagedEntity entity : clusters ) {
            ClusterComputeResource cluster = (ClusterComputeResource)entity;
            DataCenter dataCenter = toDataCenter(cluster, regionId);

            if( dataCenter != null ) {
                dataCenters.add(dataCenter);
            }

        }
        return dataCenters;
    }

    private @Nonnull Collection<DataCenter> listDataCentersFromVDCs(@Nonnull String regionId) throws InternalException, CloudException {
        ArrayList<DataCenter> dataCenters = new ArrayList<DataCenter>();
        ServiceInstance instance = getServiceInstance();

        Folder rootFolder = instance.getRootFolder();

        ManagedEntity[] mes;

        try {
            mes = new InventoryNavigator(rootFolder).searchManagedEntities("Datacenter");
        }
        catch( InvalidProperty e ) {
            throw new CloudException("No virtual machine support in cluster: " + e.getMessage());
        }
        catch( RuntimeFault e ) {
            throw new CloudException("Error in processing request to cluster: " + e.getMessage());
        }
        catch( RemoteException e ) {
            throw new CloudException("Error in cluster processing request: " + e.getMessage());
        }

        if( mes == null || mes.length < 1 ) {
            return dataCenters;
        }
        for( ManagedEntity entity : mes ) {
            DataCenter dc = toDataCenter((Datacenter)entity, regionId);

            if( dc != null ) {
                dataCenters.add(dc);
            }
        }
        return dataCenters;
    }

    @Override
    public @Nonnull Collection<Region> listRegions() throws InternalException, CloudException {
        if( !provider.isClusterBased() ) {
            return listRegionsFromEndpoints();
        }
        else {
            return listRegionsFromVDCs();
        }
    }

    @Override
    public Collection<org.dasein.cloud.dc.ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        ArrayList<org.dasein.cloud.dc.ResourcePool> list = new ArrayList<org.dasein.cloud.dc.ResourcePool>();
        Iterable<ResourcePool> rps= null;
        if (provider.isClusterBased()) {
            rps = listResourcePoolsForCluster(providerDataCenterId);
        }
        else {
            rps = listResourcePoolsForDatacenter(providerDataCenterId);
        }

        for (ResourcePool rp : rps) {
            list.add(toResourcePool(rp, providerDataCenterId));
        }
        return list;
    }

    private Collection<ResourcePool> listResourcePoolsForCluster(String providerDataCenterId) throws InternalException, CloudException {
        ServiceInstance instance = getServiceInstance();
        DataCenter dsdc = getDataCenter(providerDataCenterId);

        if( dsdc == null ) {
            return null;
        }
        Datacenter dc = getVmwareDatacenterFromVDCId(instance, dsdc.getRegionId());

        if( dc == null ) {
            return null;
        }

        ManagedEntity[] clusters;

        try {
            clusters = new InventoryNavigator(dc).searchManagedEntities("ClusterComputeResource");
        }
        catch( InvalidProperty e ) {
            throw new CloudException("No virtual machine support in cluster: " + e.getMessage());
        }
        catch( RuntimeFault e ) {
            throw new CloudException("Error in processing request to cluster: " + e.getMessage());
        }
        catch( RemoteException e ) {
            throw new CloudException("Error in cluster processing request: " + e.getMessage());
        }
        ArrayList<ResourcePool> list = new ArrayList<ResourcePool>();
        for( ManagedEntity entity : clusters ) {
            ClusterComputeResource cluster = (ClusterComputeResource)entity;

            ResourcePool rp = cluster.getResourcePool();
            list.add(rp);
        }

        return list;
    }

    private Collection<ResourcePool> listResourcePoolsForDatacenter(String dataCenterId) throws InternalException, CloudException {
        ServiceInstance instance = getServiceInstance();
        DataCenter dsdc = getDataCenter(dataCenterId);

        if( dsdc == null ) {
            return null;
        }
        Datacenter dc = getVmwareDatacenterFromVDCId(instance, dataCenterId);

        if( dc == null ) {
            return null;
        }
        ManagedEntity[] pools = null;
        try {
            pools = new InventoryNavigator(dc).searchManagedEntities("ResourcePool");
        }
        catch( InvalidProperty e ) {
            throw new CloudException(e);
        }
        catch( RuntimeFault e ) {
            throw new InternalException(e);
        }
        catch( RemoteException e ) {
            throw new CloudException(e);
        }
        ArrayList<ResourcePool> list = new ArrayList<ResourcePool>();
        for( ManagedEntity entity : pools ) {
            ResourcePool rp = (ResourcePool)entity;
            list.add(rp);
        }

        return list;
    }

    @Override
    public org.dasein.cloud.dc.ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        Iterable<DataCenter> dcs = listDataCenters(getContext().getRegionId());

        for (DataCenter dc : dcs) {
            Iterable<org.dasein.cloud.dc.ResourcePool> rps = listResourcePools(dc.getProviderDataCenterId());
            for (org.dasein.cloud.dc.ResourcePool rp : rps) {
                if (rp.getProvideResourcePoolId().equals(providerResourcePoolId)) {
                    return rp;
                }
            }
        }
        return null;
    }

    public ResourcePool getVMWareResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        Iterable<DataCenter> dcs = listDataCenters(getContext().getRegionId());
        Iterable<ResourcePool> rps;
        for (DataCenter dc : dcs) {
            if (provider.isClusterBased()) {
                rps = listResourcePoolsForCluster(dc.getProviderDataCenterId());
            }
            else {
                rps = listResourcePoolsForDatacenter(dc.getProviderDataCenterId());
            }
            for (ResourcePool rp : rps) {
                if (rp.getName().equals(providerResourcePoolId)) {
                    return rp;
                }
            }
        }
        return null;
    }

    private @Nonnull Collection<Region> listRegionsFromVDCs() throws InternalException, CloudException {
        ArrayList<Region> regions = new ArrayList<Region>();
        ServiceInstance instance = getServiceInstance();

        Folder rootFolder = instance.getRootFolder();
        ManagedEntity[] mes;

        try {
            mes = new InventoryNavigator(rootFolder).searchManagedEntities("Datacenter");
        }
        catch( InvalidProperty e ) {
            throw new CloudException("No virtual machine support in cluster: " + e.getMessage());
        }
        catch( RuntimeFault e ) {
            throw new CloudException("Error in processing request to cluster: " + e.getMessage());
        }
        catch( RemoteException e ) {
            throw new CloudException("Error in cluster processing request: " + e.getMessage());
        }

        if( mes == null || mes.length < 1 ) {
            return regions;
        }
        for( ManagedEntity entity : mes ) {
            Region r = toRegion((Datacenter) entity);

            if( r != null ) {
                regions.add(r);
            }
        }
        return regions;
    }

    private @Nonnull String getRegionId(@Nonnull String endpoint) {
        try {
            URI uri = new URI(endpoint);

            return uri.getHost();
        }
        catch( Throwable t ) {
            throw new RuntimeException(t);
        }

    }

    private @Nonnull Collection<Region> listRegionsFromEndpoints() throws InternalException, CloudException {
        ArrayList<Region> regions = new ArrayList<Region>();
        String endpoint = getContext().getEndpoint();

        if( endpoint != null ) {
            regions.add(toRegion(getRegionId(endpoint)));
        }
        return regions;
    }

    private @Nullable DataCenter toDataCenter(@Nullable ClusterComputeResource cluster, @Nonnull String regionId) {
        if( cluster == null ) {
            return null;
        }
        ManagedEntityStatus status = cluster.getOverallStatus();
        DataCenter dc = new DataCenter();

        dc.setActive(true);
        dc.setActive(!status.equals(ManagedEntityStatus.red));
        dc.setAvailable(true);
        dc.setName(cluster.getName());
        dc.setProviderDataCenterId(cluster.getName());
        dc.setRegionId(regionId);
        return dc;
    }

    private @Nullable DataCenter toDataCenter(@Nullable Datacenter dc, @Nonnull String regionId) {
        if( dc != null ) {
            DataCenter dsDc = new DataCenter();
            
            dsDc.setActive(true);
            dsDc.setAvailable(true);
            dsDc.setName(dc.getName());
            dsDc.setProviderDataCenterId(dc.getName());
            dsDc.setRegionId(regionId);
            return dsDc;
        }
        return null;
    }

    private @Nullable Region toRegion(@Nullable Datacenter dc) {
        if(dc == null ) {
            return null;
        }
        Region region = new Region();

        region.setActive(true);
        region.setAvailable(true);
        region.setJurisdiction("US");
        region.setName(dc.getName());
        region.setProviderRegionId(dc.getName());
        return region;
    }

    private @Nonnull Region toRegion(@Nonnull String regionId) {
        Region region = new Region();
        
        region.setActive(true);
        region.setAvailable(true);
        region.setJurisdiction("US");
        region.setName(regionId);
        region.setProviderRegionId(regionId);
        return region;        
    }

    private org.dasein.cloud.dc.ResourcePool toResourcePool(@Nonnull ResourcePool resourcePool, @Nonnull String dataCenterId) {
        org.dasein.cloud.dc.ResourcePool rp = new org.dasein.cloud.dc.ResourcePool();
        rp.setName(resourcePool.getName());
        rp.setProvideResourcePoolId(resourcePool.getName());
        rp.setDataCenterId(dataCenterId);

        ManagedEntityStatus status = resourcePool.getRuntime().getOverallStatus();
        if (status.equals(ManagedEntityStatus.red) || status.equals(ManagedEntityStatus.yellow)) {
            rp.setAvailable(false);
        }
        else {
            rp.setAvailable(true);
        }

        return rp;
    }
}
