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
import java.net.URL;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Properties;

import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.Folder;
import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.ContextRequirements;
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
    public @Nonnull ContextRequirements getContextRequirements() {
        return new ContextRequirements(
                new ContextRequirements.Field("apiKey", "The API Keypair", ContextRequirements.FieldType.KEYPAIR, ContextRequirements.Field.ACCESS_KEYS, true)
        );
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

    public @Nullable ServiceInstance getServiceInstance() throws CloudException, InternalException {
        ProviderContext ctx = getContext();
        
        if( ctx == null ) {
            throw new CloudException("No context exists for this request");
        }
        try {
            String endpoint = ctx.getEndpoint();

            String accessPublic = null;
            String accessPrivate = null;
            try {
                List<ContextRequirements.Field> fields = getContextRequirements().getConfigurableValues();
                for(ContextRequirements.Field f : fields ) {
                    if(f.type.equals(ContextRequirements.FieldType.KEYPAIR)){
                        byte[][] keyPair = (byte[][])getContext().getConfigurationValue(f);
                        accessPublic = new String(keyPair[0], "utf-8");
                        accessPrivate = new String(keyPair[1], "utf-8");
                    }
                }
            }
            catch( UnsupportedEncodingException e ) {
                e.printStackTrace();
                throw new RuntimeException("This cannot happen: " + e.getMessage());
            }
            return new ServiceInstance(new URL(endpoint), accessPublic, accessPrivate, isInsecure());
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
    }

    public @Nullable Folder getVmFolder(ServiceInstance instance) throws InternalException, CloudException {
        if( isClusterBased() ) {
            String regionId = getContext().getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region has been defined for this request");
            }
            Datacenter dc = getDataCenterServices().getVmwareDatacenterFromVDCId(instance, regionId);

            if( dc == null ) {
                return null;
            }
            try {
                return dc.getVmFolder();
            }
            catch( RemoteException e ) {
                throw new CloudException("Error in cluster processing request: " + e.getMessage());
            }
        }
        else {
            return instance.getRootFolder();
        }
    }

    public @Nullable Folder getNetworkFolder(ServiceInstance instance) throws InternalException, CloudException {
        if( isClusterBased() ) {
            String regionId = getContext().getRegionId();

            if( regionId == null ) {
                throw new CloudException("No region has been defined for this request");
            }
            Datacenter dc = getDataCenterServices().getVmwareDatacenterFromVDCId(instance, regionId);

            if( dc == null ) {
                return null;
            }
            return dc.getNetworkFolder();
        }
        else {
            return instance.getRootFolder();
        }
    }

    public boolean isClusterBased() throws CloudException {
        ProviderContext ctx = getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        Properties p = ctx.getCustomProperties();
        boolean cluster = true;

        if( p != null ) {
            String b = p.getProperty("clusterBased", "true");

            cluster = b.equalsIgnoreCase("true");
        }
        return cluster;
    }

    /**
     * Looks up the custom property ({@link ProviderContext#getCustomProperties()}) &quot;insecure&quot; and, if set to
     * &quot;true&quot;, returns true. This indicates that SSL validation should not take place, thus leaving the
     * connection open to man-in-the-middle attacks, even if the connection is encrypted.
     * @return true if SSL certificate validation should be ignored
     */
    public boolean isInsecure() {
        ProviderContext ctx = getContext();
        String value;

        if( ctx == null ) {
            value = null;
        }
        else {
            Properties p = ctx.getCustomProperties();

            if( p == null ) {
                value = null;
            }
            else {
                value = p.getProperty("insecure");
            }
        }
        if( value == null ) {
            value = System.getProperty("insecure");
        }
        return (value != null && value.equalsIgnoreCase("true"));
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
