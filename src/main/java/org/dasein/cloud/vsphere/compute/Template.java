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

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
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
        remove(templateId, false);
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        ServiceInstance service = getServiceInstance();

        com.vmware.vim25.mo.VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(service, providerImageId);

        if( vm == null ) {
            throw new CloudException("No such template: " + providerImageId);
        }
        try {
            vm.destroy_Task();
        }
        catch( RemoteException e ) {
            throw new CloudException(e);
        }
    }

    @Override
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No image sharing is supported");
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No image sharing is supported");
    }

    @Override
    public void addImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No image sharing is supported");
    }

    @Override
    public void addPublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No image sharing is supported");
    }

    @Override
    public @Nonnull String bundleVirtualMachine(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image bundling is not currently supported");
    }

    @Override
    public void bundleVirtualMachineAsync(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name, @Nonnull AsynchronousTask<String> trackingTask) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image bundling is not currently supported");
    }

    @Override
    public @Nonnull MachineImage captureImage(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        return capture(options, null);
    }

    @Override
    public void captureImageAsync(final @Nonnull ImageCreateOptions options, final @Nonnull AsynchronousTask<MachineImage> taskTracker) throws CloudException, InternalException {
        Thread t = new Thread() {
            public void run() {
                try {
                    capture(options, taskTracker);
                }
                catch( Throwable t ) {
                    taskTracker.complete(t);
                }
            }
        };

        t.setName("vSphere Image Capture of " + options.getVirtualMachineId());
        t.setDaemon(true);
        t.start();
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        for( ImageClass cls : listSupportedImageClasses() ) {
            for( MachineImage image : listImages(cls) ) {
                if( image.getProviderMachineImageId().equals(providerImageId) ) {
                    return image;
                }
            }
        }
        return null;
    }

    @Override
    @Deprecated
    public @Nullable MachineImage getMachineImage(@Nonnull String templateId) throws InternalException, CloudException {
        return getImage(templateId);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return getProviderTermForImage(locale, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return "template";
    }

    @Override
    public @Nonnull String getProviderTermForCustomImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return getProviderTermForImage(locale, cls);
    }

    @Override
    public @Nonnull Collection<String> listShares(@Nonnull String templateId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ImageClass> listSupportedImageClasses() throws CloudException, InternalException {
        return Collections.singletonList(ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImageType> listSupportedImageTypes() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageType.VOLUME);
    }

    @Override
    public @Nonnull MachineImage registerImageBundle(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image registration is ot supported");
    }


    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> listMachineImages() throws InternalException, CloudException {
        return listImages(ImageClass.MACHINE);
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
            image.setImageClass(ImageClass.MACHINE);
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
    public boolean hasPublicLibrary() {
        return false;
    }

    @Override
    public @Nonnull Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    private MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        String vmId = options.getVirtualMachineId();

        if( vmId == null ) {
            throw new CloudException("You must specify a virtual machine to capture");
        }
        ServiceInstance service = getServiceInstance();

        com.vmware.vim25.mo.VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(service, vmId);

        if( vm == null ) {
            throw new CloudException("No such virtual machine for imaging: " + vmId);
        }
        MachineImage img = toMachineImage(provider.getComputeServices().getVirtualMachineSupport().clone(service, vm, options.getName(), true));

        if( img == null ) {
            throw new CloudException("Failed to identify newly created template");
        }
        if( task != null ) {
            task.completeWithResult(img);
        }
        return img;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachine(@Nonnull String vmId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        org.dasein.cloud.compute.VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);

        if( vm == null ) {
            throw new CloudException("No such virtual machine: " + vmId);
        }
        final ImageCreateOptions options = ImageCreateOptions.getInstance(vm,  name, description);
        final AsynchronousTask<String> task = new AsynchronousTask<String>();

        Thread t = new Thread() {
            public void run() {
                try {
                    task.completeWithResult(capture(options, null).getProviderMachineImageId());
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
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

        for( MachineImage img : listImages(cls) ) {
            status.add(new ResourceStatus(img.getProviderMachineImageId(), img.getCurrentState()));
        }
        return status;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls) throws CloudException, InternalException {
        if( !cls.equals(ImageClass.MACHINE) ) {
            return Collections.emptyList();
        }
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
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls, @Nonnull String ownedBy) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        if( accountId == null ) {
            return Collections.emptyList();
        }
        else if( accountId.equals(getContext().getAccountNumber()) ) {
            return listImages(ImageClass.MACHINE);
        }
        else {
            return listImages(ImageClass.MACHINE, accountId);
        }
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.VMDK);
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        return searchImages(null, keyword, platform, architecture, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchImages(@Nullable String accountNumber, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        if( accountNumber != null && !accountNumber.equals(getContext().getAccountNumber()) ) {
            return Collections.emptyList();
        }
        if( imageClasses != null && imageClasses.length > 0 ) {
            boolean ok = false;

            for( ImageClass cls : imageClasses ) {
                if( cls.equals(ImageClass.MACHINE) ) {
                    ok = true;
                    break;
                }
            }
            if( !ok ) {
                return Collections.emptyList();
            }
        }
        ArrayList<MachineImage> matches = new ArrayList<MachineImage>();

        for( MachineImage image : listImages(ImageClass.MACHINE) ) {
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
    public @Nonnull Iterable<MachineImage> searchPublicImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
        return type.equals(MachineImageType.VOLUME);
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
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return false;
    }

    @Override
    public void updateTags(@Nonnull String imageId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

}
