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

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.Calendar;

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.storage.BlobStoreSupport;
import org.dasein.cloud.storage.StorageServices;
import org.dasein.cloud.vsphere.compute.VMwareComputeServices;
import org.dasein.cloud.vsphere.network.VMwareNetworkServices;

import com.vmware.vim25.InvalidLogin;
import com.vmware.vim25.mo.ServiceInstance;
import org.dasein.util.CalendarWrapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class PrivateCloud extends AbstractCloud {
    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');
        
        if( idx < 0 ) {
            return name;
        }
        else if( idx == (name.length()-1) ) {
            return "";
        }
        return name.substring(idx+1);
    }
    
    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls, @Nonnull String type) {
        String pkg = getLastItem(cls.getPackage().getName());
        
        if( pkg.equals("vsphere") ) {
            pkg = "";
        }
        else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.vsphere." + type + "." + pkg + getLastItem(cls.getName()));
    }
    public PrivateCloud() { }

    private int holdCount = 0;

    @Override
    public void hold() {
        super.hold();
        synchronized( this ) {
            holdCount++;
        }
    }

    @Override
    public void release() {
        synchronized ( this ) {
            if( holdCount > 0 ) {
                holdCount--;
            }
        }
        super.release();
    }

    private Thread closingThread = null;

    @Override
    public void close() {
        synchronized( this ) {
            if( closingThread != null ) {
                return;
            }
            if( holdCount < 1 ) {
                cleanUp();
            }
            else {
                closingThread = new Thread() {
                    public void run() {
                        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);

                        synchronized( PrivateCloud.this ) {
                            while( holdCount > 0 && System.currentTimeMillis() < timeout ) {
                                try { PrivateCloud.this.wait(5000L); }
                                catch( InterruptedException ignore ) { }
                            }
                            cleanUp();
                            closingThread = null;
                        }
                    }
                };

                closingThread.setDaemon(true);
                closingThread.start();
            }
        }
    }

    private void cleanUp() {
        super.close();
        try {
            getServiceInstance().getServerConnection().logout();
        }
        catch( CloudException ignore ) {
            // ignore
        }
        catch( InternalException ignore ) {
            // ignore
        }
        catch( NullPointerException ignore ) {
            // ignore
        }
    }

    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
        String name = null;
        
        if( ctx != null ) {
            name = ctx.getCloudName();
        }
        return (name == null ? "vSphere" : name);
    }

    @Override
    public @Nonnull VMwareNetworkServices getNetworkServices() {
        return new VMwareNetworkServices(this);
    }
    
    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
        String name = null;

        if( ctx != null ) {
            name = ctx.getProviderName();
        }
        return (name == null ? "VMware" : name);
    }
    
    @Override
    public @Nonnull VMwareComputeServices getComputeServices() {
        return new VMwareComputeServices(this);
    }
    
    @Override
    public @Nonnull Dc getDataCenterServices() {
        return new Dc(this);
    }
    
    private @Nonnull String getEndpoint(String regionId) throws CloudException {
        for( String endpoint : getEndpoints() ) {
            if( regionId == null ) {
                return endpoint;
            }
            String rid = getRegionId(endpoint);
            
            if( rid.equals(regionId) ) {
                return endpoint;
            }
        }
        throw new CloudException("No endpoint has been configured");
    }
    
    @Nonnull String[] getEndpoints() throws CloudException {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context has been configured");
        }
        String endpoint = ctx.getEndpoint();
        
        if( endpoint == null ) {
            return new String[0];
        }
        String[] parts = endpoint.split(",");
        
        if( parts == null || parts.length < 1 ) {
            return new String[] { endpoint };
        }
        return parts;
    }
    
    @Nonnull String getRegionId(@Nonnull String endpoint) {
        try {
            URI uri = new URI(endpoint);
        
            return uri.getHost();
        }
        catch( Throwable t ) {
            throw new RuntimeException(t);
        }

    }
    
    public @Nullable ServiceInstance getServiceInstance() throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context exists for this request");
        }
        try {
            String endpoint = getEndpoint(ctx.getRegionId());
            
            return new ServiceInstance(new URL(endpoint), new String(ctx.getAccessPublic(), "utf-8"), new String(ctx.getAccessPrivate(), "utf-8"), true);
        }
        catch( InvalidLogin e ) {
            return null;
        }
        catch( RemoteException e ) {
            e.printStackTrace();
            throw new CloudException("Error creating service instance: " + e.getMessage());
        }
        catch( MalformedURLException e ) {
            e.printStackTrace();
            throw new InternalException("Failed to generate endpoint URL for " + ctx.getEndpoint() + ": " + e.getMessage());
        }
        catch( UnsupportedEncodingException e ) {
            e.printStackTrace();
            throw new InternalException("Encoding UTF-8 not supported: " + e.getMessage());
        }
    }
    
    @Override
    public @Nullable String testContext() {
        Logger logger = getLogger(PrivateCloud.class, "std");
        
        if( logger.isTraceEnabled() ) {
            logger.trace("enter - " + PrivateCloud.class.getName() + ".testContext()");
        }
        try {
            try {
                ProviderContext ctx = getContext();
                
                if( ctx == null ) {
                    return null;
                }
                if( !getComputeServices().getVirtualMachineSupport().isSubscribed() ) {
                    return null;
                }
                if( hasStorageServices() ) {
                    // test the storage cloud if connected to one
                    StorageServices services = getStorageServices();
                    
                    if( services != null && services.hasBlobStoreSupport() ) {
                        BlobStoreSupport support = services.getBlobStoreSupport();
                        
                        if( support != null && !support.isSubscribed() ) {
                            return null;
                        }
                    }
                }
                return ctx.getAccountNumber();
            }
            catch( Throwable t ) {
                logger.warn("testContext(): Failed to test vSphere context: " + t.getMessage());
                if( logger.isTraceEnabled() ) {
                    t.printStackTrace();
                }
                return null;
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("exit - " + PrivateCloud.class.getName() + ".testContext()");
            }
        }
    }
}
