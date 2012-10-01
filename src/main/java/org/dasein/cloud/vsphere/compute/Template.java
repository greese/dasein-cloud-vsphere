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

package org.dasein.cloud.vsphere.compute;

import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.vsphere.PrivateCloud;

import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.VirtualMachineConfigInfo;
import com.vmware.vim25.VirtualMachineGuestOsIdentifier;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRuntimeInfo;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.VirtualMachine;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

public class Template implements MachineImageSupport {

    private PrivateCloud provider;
    
    Template(@Nonnull PrivateCloud cloud) { provider = cloud; }

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
    public void remove(@Nonnull String templateId) throws InternalException, CloudException {
        ServiceInstance service = getServiceInstance();

        com.vmware.vim25.mo.VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(service, templateId);

        if( vm == null ) {
            throw new CloudException("No such template: " + templateId);
        }
        try {
            vm.destroy_Task();
        }
        catch( RemoteException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public @Nullable MachineImage getMachineImage(@Nonnull String templateId) throws InternalException, CloudException {
        for( MachineImage image : listMachineImages() ) {
            if( image.getProviderMachineImageId().equals(templateId) ) {
                return image;
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return "template";
    }

    @Override
    public @Nonnull Collection<String> listShares(@Nonnull String templateId) throws CloudException, InternalException {
        return Collections.emptyList();
    }


    @Override
    public @Nonnull Collection<MachineImage> listMachineImages() throws InternalException, CloudException {
        ArrayList<MachineImage> machineImages = new ArrayList<MachineImage>();
        ServiceInstance instance = getServiceInstance();
        
        Folder folder = provider.getVmFolder(instance);
        ManagedEntity[] mes;

        try {
            mes = new InventoryNavigator(folder).searchManagedEntities("VirtualMachine");
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

        if( mes != null && mes.length > 0 ) {
            for( ManagedEntity entity : mes ) {
                VirtualMachine template = (VirtualMachine)entity;

                if( template != null ) {
                    VirtualMachineConfigInfo cfg = null;

                    try {
                        cfg = template.getConfig();
                    }
                    catch( RuntimeException e ) {
                        e.printStackTrace();
                    }
                    if( cfg != null && cfg.isTemplate() ) {
                        MachineImage image = toMachineImage(template);

                        if( image != null ) {
                            machineImages.add(image);
                        }
                    }
                }
            }
        }

        return machineImages;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void shareMachineImage(@Nonnull String templateId, @Nullable String withAccountId, boolean grant) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Sharing not supported.");
    }
    
    private @Nullable MachineImage toMachineImage(@Nullable VirtualMachine template) throws InternalException, CloudException {
        if( template != null ) {
            VirtualMachineConfigInfo vminfo;
            MachineImage image = new MachineImage();
            VirtualMachineGuestOsIdentifier os;
            Platform platform;

            try {
                vminfo = template.getConfig();
            }
            catch( RuntimeException e ) {
                return null;
            }
            try {
                os = VirtualMachineGuestOsIdentifier.valueOf(vminfo.getGuestId());
                platform = Platform.guess(vminfo.getGuestFullName());
            }
            catch( IllegalArgumentException e ) {
                System.out.println("DEBUG: No such guest in enum: " + vminfo.getGuestId());
                os = null;
                platform = Platform.guess(vminfo.getGuestId());
            }
            if( os == null ) {
                image.setArchitecture(vminfo.getGuestId().contains("64") ? Architecture.I32 : Architecture.I64);
            }
            else {
                image.setArchitecture(provider.getComputeServices().getVirtualMachineSupport().getArchitecture(os));
            }
            image.setDescription(template.getName());
            image.setName(template.getName());
            image.setProviderOwnerId(getContext().getAccountNumber());
            image.setPlatform(platform);
            image.setProviderMachineImageId(vminfo.getUuid());
            image.setType(MachineImageType.VOLUME);
            image.setProviderRegionId(getContext().getRegionId());
            image.setSoftware("");
            image.setTags(new HashMap<String,String>());
            
            VirtualMachineRuntimeInfo runtime = template.getRuntime();
            VirtualMachinePowerState state = VirtualMachinePowerState.poweredOff;
            
            if( runtime != null ) {
                state = runtime.getPowerState();
            }
            if( state.equals(VirtualMachinePowerState.poweredOff) ) {
                image.setCurrentState(MachineImageState.ACTIVE);
            }
            else {
                image.setCurrentState(MachineImageState.PENDING);
            }
            return image;
        }
        return null;
    }

    @Override
    public void downloadImage(@Nonnull String machineImageId, @Nonnull OutputStream toOutput) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Not supported");
    }

    @Override
    public boolean hasPublicLibrary() {
        return false;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachine(@Nonnull String vmId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        final AsynchronousTask<String> task = new AsynchronousTask<String>();
        final String fvmId = vmId;
        final String fname = name;
        
        Thread t = new Thread() {
            public void run() {
                try {
                    ServiceInstance service = getServiceInstance();
                    
                    com.vmware.vim25.mo.VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(service, fvmId);

                    if( vm == null ) {
                        throw new CloudException("No such virtual machine for imaging: " + fvmId);
                    }
                    MachineImage img = toMachineImage(provider.getComputeServices().getVirtualMachineSupport().clone(service, vm, fname, true));

                    if( img == null ) {
                        throw new CloudException("Failed to identify newly created template");
                    }
                    task.completeWithResult(img.getProviderMachineImageId());
                }
                catch( Throwable t ) {
                    task.complete(t);
                }
            }
        };
        
        t.setName("Image VM " + vmId);
        t.setDaemon(false);
        t.start();
        return task;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachineToStorage(@Nonnull String vmId, @Nonnull String name, @Nonnull String description, @Nonnull String directory) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Not supported");
    }

    @Override
    public @Nonnull String installImageFromUpload(@Nonnull MachineImageFormat format, @Nonnull InputStream imageStream) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Not supported");
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        if( accountId == null ) {
            return Collections.emptyList();
        }
        else if( accountId.equals(getContext().getAccountNumber()) ) {
            return listMachineImages();
        }
        else {
            return Collections.emptyList();
        }
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.VMDK);
    }

    @Override
    public @Nonnull String registerMachineImage(@Nonnull String atStorageLocation) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Not supported");
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        ArrayList<MachineImage> matches = new ArrayList<MachineImage>();
        
        for( MachineImage image : listMachineImages() ) {
            if( architecture != null && !architecture.equals(image.getArchitecture()) ) {
                continue;
            }
            if( platform != null && !platform.equals(Platform.UNKNOWN) ) {
                Platform mine = image.getPlatform();
                
                if( platform.isWindows() && !mine.isWindows() ) {
                    continue;
                }
                if( platform.isUnix() && !mine.isUnix() ) {
                    continue;
                }
                if( platform.isBsd() && !mine.isBsd() ) {
                    continue;
                }
                if( platform.isLinux() && !mine.isLinux() ) {
                    continue;
                }
                if( platform.equals(Platform.UNIX) ) {
                    if( !mine.isUnix() ) {
                        continue;
                    }
                }
                else if( !platform.equals(mine) ) {
                    continue;
                }
            }
            if( keyword != null && !keyword.equals("") ) {
                keyword = keyword.toLowerCase();
                if( !image.getProviderMachineImageId().toLowerCase().contains(keyword) ) {
                    if( !image.getName().toLowerCase().contains(keyword) ) {
                        if( !image.getDescription().toLowerCase().contains(keyword) ) {
                            continue;
                        }
                    }
                }
            }
            matches.add(image);
        }
        return matches;
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsImageSharing() {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return false;
    }

    @Override
    public @Nonnull String transfer(@Nonnull CloudProvider fromCloud, @Nonnull String machineImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Not supported");
    }

}
