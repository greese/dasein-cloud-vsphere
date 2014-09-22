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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import com.vmware.vim25.*;
import com.vmware.vim25.mo.ClusterComputeResource;
import com.vmware.vim25.mo.ResourcePool;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterCapabilities;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.FolderType;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.dc.StoragePool;

import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.Datastore;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ServiceInstance;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.vsphere.compute.Host;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Minute;
import org.dasein.util.uom.time.TimePeriod;

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
        APITrace.begin(provider, "DC.getDataCenter");
        try {
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
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "cluster";
    }

    @Override
    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "data center";
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
        APITrace.begin(provider, "DC.getResourcePoolFromClusterId");
        try {
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
                throw new CloudException("No cluster support in datacenter: " + e.getMessage());
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
        finally {
            APITrace.end();
        }
    }

    public @Nullable Datacenter getVmwareDatacenterFromVDCId(@Nonnull ServiceInstance service, @Nonnull String dcId) throws CloudException, InternalException {
        APITrace.begin(provider, "DC.getVmwareDatacenterFromVDCId");
        try {
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
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String regionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listDataCenters");
        try {
            Cache<DataCenter> cache = Cache.getInstance(provider, "dataCenters", DataCenter.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(15, TimePeriod.MINUTE));
            Collection<DataCenter> dcs = (Collection<DataCenter>)cache.get(provider.getContext());

            if( dcs == null ) {
                dcs = listDataCentersFromClusters(regionId);
                if (dcs != null && dcs.size() > 0) {
                    cache.put(provider.getContext(), dcs);
                    return dcs;
                }
                else {
                    // create a dummy dc based on the region (vSphere datacenter)
                    DataCenter dc = new DataCenter();
                    dc.setAvailable(true);
                    dc.setActive(true);
                    dc.setName(regionId);
                    dc.setRegionId(regionId);
                    dc.setProviderDataCenterId(regionId+"-a");
                    dcs.add(dc);
                    cache.put(provider.getContext(), dcs);
                    return Collections.singletonList(dc);
                }
            }
            return dcs;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull Collection<DataCenter> listDataCentersFromClusters(@Nonnull String regionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listDataCentersFromClusters");
        try {
            ArrayList<DataCenter> dataCenters = new ArrayList<DataCenter>();
            ServiceInstance instance = getServiceInstance();
            Datacenter dc = getVmwareDatacenterFromVDCId(instance, regionId);

            if( dc == null ) {
                throw new CloudException("No such dc: " + regionId);
            }

            ManagedEntity[] clusters;

            try {
                clusters = new InventoryNavigator(dc).searchManagedEntities("ClusterComputeResource");
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No cluster support in datacenter: " + e.getMessage());
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
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<Region> listRegions() throws InternalException, CloudException {
        return listRegionsFromVDCs();
    }

    @Override
    public Collection<org.dasein.cloud.dc.ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listResourcePools");
        try {
            ArrayList<org.dasein.cloud.dc.ResourcePool> list = new ArrayList<org.dasein.cloud.dc.ResourcePool>();
            Iterable<ResourcePool> rps;
            DataCenter ourDC = provider.getDataCenterServices().getDataCenter(providerDataCenterId);
            if (ourDC != null) {
                if (ourDC.getProviderDataCenterId().endsWith("-a")) {
                    rps = listResourcePoolsForDatacenter(ourDC.getRegionId());
                }
                else {
                    rps = listResourcePoolsForCluster(providerDataCenterId);
                }

                if (rps != null) {
                    for (ResourcePool rp : rps) {
                        list.add(toResourcePool(rp, providerDataCenterId));
                    }
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    private Collection<ResourcePool> listResourcePoolsForCluster(String providerDataCenterId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listResourcePoolsForCluster");
        try {
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

            ArrayList<ResourcePool> list = new ArrayList<ResourcePool>();
            try {
                clusters = new InventoryNavigator(dc).searchManagedEntities("ClusterComputeResource");

                if (clusters != null) {
                    for( ManagedEntity entity : clusters ) {

                        ClusterComputeResource cluster = (ClusterComputeResource)entity;
                        if (cluster.getName().equals(providerDataCenterId)) {
                            ResourcePool root = cluster.getResourcePool();
                            if (root.getResourcePools() != null && root.getResourcePools().length > 0) {
                                getChildren(root.getResourcePools(), list);
                            }
                        }
                    }
                }
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No cluster support in datacenter: " + e.getMessage());
            }
            catch( RuntimeFault e ) {
                throw new CloudException("Error in processing request to cluster: " + e.getMessage());
            }
            catch( RemoteException e ) {
                throw new CloudException("Error in cluster processing request: " + e.getMessage());
            }

            return list;
        }
        finally {
            APITrace.end();
        }
    }

    private void getChildren(ResourcePool[] pools, ArrayList<ResourcePool> list) throws CloudException, InternalException {
        APITrace.begin(provider, "DC.getChildren(ResourcePool[])");
        try {
            try {
                for (ResourcePool r : pools) {
                    list.add(r);
                    if (r.getResourcePools() != null && r.getResourcePools().length > 0) {
                        getChildren(r.getResourcePools(), list);
                    }
                }
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No resource pool support in cluster: " + e.getMessage());
            }
            catch( RuntimeFault e ) {
                throw new CloudException("Error in processing request to cluster: " + e.getMessage());
            }
            catch( RemoteException e ) {
                throw new CloudException("Error in cluster processing request: " + e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void getFolderChildren(Folder folder, List<Folder> list) throws CloudException, InternalException {
        APITrace.begin(provider, "DC.getFolderChildren(Folder)");
        try {
            try {
                ManagedEntity[] children = folder.getChildEntity();
                for (ManagedEntity child : children) {
                    if (child instanceof Folder) {
                        list.add((Folder)child);
                        getFolderChildren((Folder)child, list);
                    }
                }
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No resource pool support in cluster: " + e.getMessage());
            }
            catch( RuntimeFault e ) {
                throw new CloudException("Error in processing request to cluster: " + e.getMessage());
            }
            catch( RemoteException e ) {
                throw new CloudException("Error in cluster processing request: " + e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    private Collection<ResourcePool> listResourcePoolsForDatacenter(String dataCenterId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listResourcePoolsForDatacenter");
        try {
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
            if (pools != null) {
                for( ManagedEntity entity : pools ) {
                    ResourcePool rp = (ResourcePool)entity;
                    list.add(rp);
                }
            }

            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public org.dasein.cloud.dc.ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getResourcePool");
        try {
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
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<StoragePool> listStoragePools() throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listStoragePools");
        try {
            Cache<StoragePool> cache = Cache.getInstance(provider, "storagePools", StoragePool.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(15, TimePeriod.MINUTE));
            Collection<StoragePool> pools = (Collection<StoragePool>)cache.get(provider.getContext());

            if( pools == null ) {
                pools = new ArrayList<StoragePool>();
                ArrayList<String> datastoreNames = new ArrayList<String>();
                Host hostSupport = provider.getComputeServices().getAffinityGroupSupport();

                for (DataCenter dataCenter : listDataCenters(provider.getContext().getRegionId())) {
                    boolean sameDC = false;
                    for (HostSystem host : hostSupport.listHostSystems(dataCenter.getProviderDataCenterId())) {
                        Iterable<Datastore> datastores = hostSupport.listDatastoresForHost(host);
                        for (Datastore ds: datastores) {
                            if (!datastoreNames.contains(ds.getName())) {
                                datastoreNames.add(ds.getName());
                                StoragePool sp = toStoragePool(ds, host.getName(), dataCenter.getProviderDataCenterId());
                                pools.add(sp);
                            }
                            else {
                                for (StoragePool storagePool: pools) {
                                    if (storagePool.getStoragePoolName().equals(ds.getName())) {
                                        storagePool.setAffinityGroupId(null);
                                        if (!sameDC) {
                                            storagePool.setDataCenterId(null);
                                        }
                                    }
                                }
                            }
                        }
                        sameDC = true;
                    }
                }
                cache.put(provider.getContext(), pools);
            }
            return pools;
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public StoragePool getStoragePool(String providerStoragePoolId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getStoragePool");
        try {
            Collection<StoragePool> pools = listStoragePools();
            for (StoragePool pool : pools) {
                if (pool.getStoragePoolId().equals(providerStoragePoolId)) {
                    return pool;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Collection<org.dasein.cloud.dc.Folder> listVMFolders() throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listVMFolders");
        try {
            ProviderContext ctx = getContext();
            List<Folder> vsphereList = new ArrayList<Folder>();
            List<org.dasein.cloud.dc.Folder> list = new ArrayList<org.dasein.cloud.dc.Folder>();

            ServiceInstance instance = getServiceInstance();
            Datacenter dc = getVmwareDatacenterFromVDCId(instance, ctx.getRegionId());

            if( dc == null ) {
                throw new CloudException("No such dc: " + ctx.getRegionId());
            }
            try {
                Folder vmFolder = dc.getVmFolder();
                getFolderChildren(vmFolder, vsphereList);
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No cluster support in datacenter: " + e.getMessage());
            }
            catch( RuntimeFault e ) {
                throw new CloudException("Error in processing request to cluster: " + e.getMessage());
            }
            catch( RemoteException e ) {
                throw new CloudException("Error in cluster processing request: " + e.getMessage());
            }
            for (Folder folder : vsphereList) {
                org.dasein.cloud.dc.Folder f = toFolder(folder, FolderType.VM, true);
                if (f != null) {
                    list.add(f);
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public org.dasein.cloud.dc.Folder getVMFolder(String providerVMFolderId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getVMFolder");
        try {
            ProviderContext ctx = getContext();
            ServiceInstance instance = getServiceInstance();
            Datacenter dc = getVmwareDatacenterFromVDCId(instance, ctx.getRegionId());

            if( dc == null ) {
                throw new CloudException("No such dc: " + ctx.getRegionId());
            }
            try {
                ManagedEntity tmp = new InventoryNavigator(dc.getVmFolder()).searchManagedEntity("Folder", providerVMFolderId);
                if (tmp != null) {
                    org.dasein.cloud.dc.Folder folder = toFolder((Folder)tmp, FolderType.VM, true);
                    if (folder.getId().equals(providerVMFolderId)) {
                        return folder;
                    }
                }
                return null;
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No cluster support in region: " + e.getMessage());
            }
            catch( RuntimeFault e ) {
                throw new CloudException("Error in processing request to region: " + e.getMessage());
            }
            catch( RemoteException e ) {
                throw new CloudException("Error in region processing request: " + e.getMessage());
            }
        }
        finally {
            APITrace.end();
        }
    }

    public ResourcePool getVMWareResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getVMWareResourcePool");
        try {
            Iterable<DataCenter> dcs = listDataCenters(getContext().getRegionId());
            Iterable<ResourcePool> rps;
            for (DataCenter dc : dcs) {
                if (dc.getProviderDataCenterId().endsWith("-a")) {
                    rps = listResourcePoolsForDatacenter(dc.getRegionId());
                }
                else {
                    rps = listResourcePoolsForCluster(dc.getProviderDataCenterId());
                }
                for (ResourcePool rp : rps) {
                    if (getIdForResourcePool(rp).equals(providerResourcePoolId)) {
                        return rp;
                    }
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull Collection<Region> listRegionsFromVDCs() throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listRegionsFromVDCs");
        try {
            ArrayList<Region> regions = new ArrayList<Region>();
            ServiceInstance instance = getServiceInstance();

            Folder rootFolder = instance.getRootFolder();
            ManagedEntity[] mes;

            try {
                mes = new InventoryNavigator(rootFolder).searchManagedEntities("Datacenter");
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No datacenter support: " + e.getMessage());
            }
            catch( RuntimeFault e ) {
                throw new CloudException("Error in processing request: " + e.getMessage());
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
        finally {
            APITrace.end();
        }
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

    private org.dasein.cloud.dc.ResourcePool toResourcePool(@Nonnull ResourcePool resourcePool, @Nonnull String dataCenterId) {
        org.dasein.cloud.dc.ResourcePool rp = new org.dasein.cloud.dc.ResourcePool();
        rp.setName(resourcePool.getName());
        rp.setDataCenterId(dataCenterId);

        ManagedEntityStatus status = resourcePool.getRuntime().getOverallStatus();
        if (status.equals(ManagedEntityStatus.red) || status.equals(ManagedEntityStatus.yellow)) {
            rp.setAvailable(false);
        }
        else {
            rp.setAvailable(true);
        }

        //get resource pool hierarchy and add to id
        rp.setProvideResourcePoolId(getIdForResourcePool(resourcePool));

        return rp;
    }

    public String getIdForResourcePool(ResourcePool rp) {
        String id = rp.getName();
        ManagedEntity parent = rp.getParent();
        while (parent != null) {
            if (parent instanceof ResourcePool) {
                id = parent.getName()+"."+id;
                parent = parent.getParent();
            }
            else {
                //need to remove the top root resource pool
                int rPIdx = id.indexOf(".")+1;
                if (rPIdx > 0) {
                    id = id.substring(rPIdx);
                }
                else {
                    //we only have the root resource pool so return null
                    return null;
                }
                break;
            }
        }
        return id;
    }

    private StoragePool toStoragePool(Datastore ds, String hostName, String datacenter) {
        StoragePool sp = new StoragePool();
        sp.setAffinityGroupId(hostName);
        sp.setDataCenterId(datacenter);
        sp.setRegionId(provider.getContext().getRegionId());
        sp.setStoragePoolName(ds.getName());
        sp.setStoragePoolId(ds.getName());

        DatastoreSummary info = ds.getSummary();
        long capacityBytes = info.getCapacity();
        long freeBytes = info.getFreeSpace();
        long provisioned = capacityBytes-freeBytes;
        sp.setCapacity((Storage<Megabyte>)new Storage<org.dasein.util.uom.storage.Byte>(capacityBytes, Storage.BYTE).convertTo(Storage.MEGABYTE));
        sp.setFreeSpace((Storage<Megabyte>)new Storage<org.dasein.util.uom.storage.Byte>(freeBytes, Storage.BYTE).convertTo(Storage.MEGABYTE));
        sp.setProvisioned((Storage<Megabyte>)new Storage<org.dasein.util.uom.storage.Byte>(provisioned, Storage.BYTE).convertTo(Storage.MEGABYTE));
        return sp;
    }

    private org.dasein.cloud.dc.Folder toFolder(Folder folder, FolderType type, boolean checkRecursive) throws CloudException, InternalException{
        org.dasein.cloud.dc.Folder f = new org.dasein.cloud.dc.Folder();
        f.setId(folder.getName());
        f.setName(folder.getName());
        f.setType(type);

        if (checkRecursive) {
            ManagedEntity parent = folder.getParent();
            if (parent instanceof Folder) {
                org.dasein.cloud.dc.Folder parentFolder = toFolder((Folder)parent, type, false);
                f.setParent(parentFolder);
            }

            try {
                List<org.dasein.cloud.dc.Folder> childFolders = new ArrayList<org.dasein.cloud.dc.Folder>();
                ManagedEntity[] children = folder.getChildEntity();
                for (ManagedEntity child : children) {
                    if (child instanceof Folder) {
                        org.dasein.cloud.dc.Folder childFolder = toFolder((Folder) child, type, false);
                        childFolders.add(childFolder);
                    }
                }
                f.setChildren(childFolders);
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No folder support: " + e.getMessage());
            }
            catch( RuntimeFault e ) {
                throw new CloudException("Error in processing request: " + e.getMessage());
            }
            catch( RemoteException e ) {
                throw new CloudException("Error in folder processing request: " + e.getMessage());
            }
        }
        return f;
    }
}