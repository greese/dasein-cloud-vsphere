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
import java.util.Locale;

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.DataCenter;
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
    
    @Override
    public @Nullable DataCenter getDataCenter(@Nonnull String dcId) throws InternalException, CloudException {
        String regionId = getContext().getRegionId();
        
        if( regionId == null ) {
            throw new CloudException("No region is established for this request");
        }
        return toDataCenter(getVmwareDatacenter(getServiceInstance(), dcId), regionId);
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

    
    public @Nullable Datacenter getVmwareDatacenter(@Nonnull ServiceInstance service, @Nonnull String dcId) throws CloudException, InternalException {
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
        ArrayList<Region> regions = new ArrayList<Region>();
        
        for( String endpoint : provider.getEndpoints() ) {
            regions.add(toRegion(provider.getRegionId(endpoint)));
        }
        return regions;
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
    
    private @Nonnull Region toRegion(@Nonnull String regionId) {
        Region region = new Region();
        
        region.setActive(true);
        region.setAvailable(true);
        region.setName(regionId);
        region.setProviderRegionId(regionId);
        return region;        
    }
}
