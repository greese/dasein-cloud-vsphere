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

package org.dasein.cloud.vsphere.compute;

import java.rmi.RemoteException;
import java.util.*;

import org.apache.log4j.Logger;
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageCapabilities;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.dc.Region;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.vsphere.PrivateCloud;

import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.VirtualMachineConfigInfo;
import com.vmware.vim25.VirtualMachineGuestOsIdentifier;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRuntimeInfo;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.VirtualMachine;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

public class Template extends AbstractImageSupport<PrivateCloud> {
    static private final Logger log = PrivateCloud.getLogger(Template.class, "std");

    Template(@Nonnull PrivateCloud cloud) {
        super(cloud);
    }

    private @Nonnull ServiceInstance getServiceInstance() throws CloudException, InternalException {
        ServiceInstance instance = getProvider().getServiceInstance();

        if( instance == null ) {
            throw new CloudException(CloudErrorType.AUTHENTICATION, HttpServletResponse.SC_UNAUTHORIZED, null, "Unauthorized");
        }
        return instance;
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.remove");
        try {
            ServiceInstance instance = getServiceInstance();

            Folder folder = getProvider().getVmFolder(instance);
            ManagedEntity[] mes;

            try {
                mes = new InventoryNavigator(folder).searchManagedEntities("VirtualMachine");
            }
            catch( InvalidProperty e ) {
                throw new CloudException("No virtual machine support in cluster: " + e.getMessage(), e);
            }
            catch( RuntimeFault e ) {
                throw new CloudException("Error in processing request to cluster: " + e.getMessage(), e);
            }
            catch( RemoteException e ) {
                throw new CloudException("Error in cluster processing request: " + e.getMessage(), e);
            }
            if( mes == null ) {
                log.warn("No templates found in inventory when removing image: "+providerImageId);
                return;
            }

            for( ManagedEntity entity : mes ) {
                VirtualMachine template = ( VirtualMachine ) entity;
                if( template == null ) {
                    continue;
                }
                VirtualMachineConfigInfo cfg = template.getConfig();
                if( cfg == null || !cfg.isTemplate() ) {
                    continue;
                }
                if( providerImageId.equals(cfg.getUuid()) ) {
                    try {
                        template.destroy_Task();
                    }
                    catch( RuntimeException e ) {
                        throw new InternalException("Error while running a destroy task for image: "+providerImageId, e);
                    }
                    catch( RemoteException ex ) {
                        throw new CloudException("Error while running a destroy task for image: "+providerImageId, ex);
                    }
                    break; // job's done, stop traversing
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile TemplateCapabilities capabilities;
    @Override
    public ImageCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new TemplateCapabilities(getProvider());
        }
        return capabilities;
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.getImage");
        try {
            for( ImageClass cls : getCapabilities().listSupportedImageClasses() ) {
                for( MachineImage image : listImages(ImageFilterOptions.getInstance(cls)) ) {
                    if( image.getProviderMachineImageId().equals(providerImageId) ) {
                        return image;
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
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable MachineImage toMachineImage(@Nullable VirtualMachine template) throws InternalException, CloudException {
        if( template != null ) {
            VirtualMachineConfigInfo vminfo;
            MachineImage image;
            VirtualMachineGuestOsIdentifier os;
            Platform platform;
            Architecture arch;
            MachineImageState imgState;
            String ownerId = "", regionId = "", name = "", description = "", imageId = "", dataCenterId = "";

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
                arch = (vminfo.getGuestId().contains("64") ? Architecture.I32 : Architecture.I64);
            }
            else {
                arch = (getProvider().getComputeServices().getVirtualMachineSupport().getArchitecture(os));
            }
            description = (template.getName());
            name = (template.getName());
            ownerId = (getContext().getAccountNumber());
            imageId = (vminfo.getUuid());
            ManagedEntity parent = template.getParent();
            while (parent != null) {
                if (parent instanceof Datacenter) {
                    Region r = getProvider().getDataCenterServices().getRegion(parent.getName());
                    regionId = r.getProviderRegionId();
                    break;
                }
                parent = parent.getParent();
            }

            VirtualMachineRuntimeInfo runtime = template.getRuntime();
            VirtualMachinePowerState state = VirtualMachinePowerState.poweredOff;

            if( runtime != null ) {
                state = runtime.getPowerState();
            }
            if( state.equals(VirtualMachinePowerState.poweredOff) ) {
                imgState = (MachineImageState.ACTIVE);
            }
            else {
                imgState = (MachineImageState.PENDING);
            }

            image = MachineImage.getMachineImageInstance(ownerId, regionId, imageId, imgState, name, description, arch, platform);
            image.withSoftware("");
            image.setTags(new HashMap<String,String>());
            return image;
        }
        return null;
    }

    @Override
    protected MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.capture");
        try {
            String vmId = options.getVirtualMachineId();

            if( vmId == null ) {
                throw new CloudException("You must specify a virtual machine to capture");
            }
            ServiceInstance service = getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getProvider().getComputeServices().getVirtualMachineSupport().getVirtualMachine(service, vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine for imaging: " + vmId);
            }
            MachineImage img = toMachineImage(getProvider().getComputeServices().getVirtualMachineSupport().clone(service, vm, options.getName(), true));

            if( img == null ) {
                throw new CloudException("Failed to identify newly created template");
            }
            if( task != null ) {
                task.completeWithResult(img);
            }
            return img;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        /*try {
            VirtualMachineGuestOsIdentifier os = VirtualMachineGuestOsIdentifier.valueOf(machineImageId);
            return true;
        }
        catch( IllegalArgumentException ignore ) {}   */
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.listImageStatus");
        try {
            ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

            for( MachineImage img : listImages(cls) ) {
                status.add(new ResourceStatus(img.getProviderMachineImageId(), img.getCurrentState()));
            }
            return status;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Image.listImages");
        try {
            ArrayList<MachineImage> machineImages = new ArrayList<MachineImage>();
            ServiceInstance instance = getServiceInstance();

            Folder folder = getProvider().getVmFolder(instance);
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

                            if( image != null && (options == null || options.matches(image)) ) {
                                if (options!= null) {
                                    if (options.getWithAllRegions()) {
                                        machineImages.add(image);
                                    }
                                    else {
                                        if (image.getProviderRegionId().equals(getContext().getRegionId())) {
                                            machineImages.add(image);
                                        }
                                    }
                                }
                                else {
                                    machineImages.add(image);
                                }
                            }
                        }
                    }
                }
            }

            return machineImages;
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Iterable<MachineImage> searchPublicImages(@Nonnull ImageFilterOptions options) throws CloudException, InternalException {
        // commented out for now as this method does not allow you to launch a functioning vm (no operating system)
        /*List<MachineImage> list = new ArrayList<MachineImage>();
        VirtualMachineGuestOsIdentifier[] osValues = VirtualMachineGuestOsIdentifier.values();
        for (VirtualMachineGuestOsIdentifier os : osValues) {
            if (!os.name().startsWith("other")) {
                MachineImage img = toMachineImage(os);
                if (options!= null) {
                    if (options.matches(img)) {
                        list.add(img);
                    }
                }
                else {
                    list.add(img);
                }
            }
        }
        return list;*/
        return Collections.emptyList();
    }

    private @Nullable MachineImage toMachineImage(@Nonnull VirtualMachineGuestOsIdentifier osIdentifier) throws InternalException, CloudException {
        MachineImage image = null;
        String ownerId = null, regionId = null, imageId = null, name = null, description = null;
        Architecture arch = null;
        MachineImageState state = MachineImageState.ACTIVE;
        Platform platform;

        arch = getProvider().getComputeServices().getVirtualMachineSupport().getArchitecture(osIdentifier);
        description = osIdentifier.name();
        name = getGuestOSNameMap().get(osIdentifier.name());
        if (name == null || name.equals("")) {
            name = osIdentifier.name();
        }
        platform = Platform.guess(name);
        ownerId = "--public--";
        imageId = osIdentifier.name();
        regionId = getContext().getRegionId();

        image = MachineImage.getMachineImageInstance(ownerId, regionId, imageId, state, name, description, arch, platform);
        image.withSoftware("");
        image.setTags(new HashMap<String, String>());
        if (imageId != null && name != null) {
            return image;
        }
        return null;
    }

    private @Nonnull
    Map<String, String> getGuestOSNameMap() {
        Cache<Map> cache = Cache.getInstance(getProvider(), "guestOS", Map.class, CacheLevel.CLOUD, new TimePeriod<Day>(1, TimePeriod.DAY));
        Collection<Map> list = (ArrayList<Map>)cache.get(getProvider().getContext());

        if( list == null ) {
            list = new ArrayList();
            Map<String, String> osMap = new HashMap<String, String>();
            osMap.put("asianux3_64Guest", "Asianux Server 3 (64 bit)");
            osMap.put("asianux3Guest", "Asianux Server 3");
            osMap.put("asianux4_64Guest", "Asianux Server 4 (64 bit)");
            osMap.put("asianux4Guest", "Asianux Server 4");
            osMap.put("centos64Guest", "CentOS 4/5 (64-bit)");
            osMap.put("centosGuest", "CentOS 4/5");
            osMap.put("darwin10_64Guest", "Mac OS 10.6 (64 bit)");
            osMap.put("darwin10Guest", "Mac OS 10.6");
            osMap.put("darwin11_64Guest", "Mac OS 10.7 (64 bit)");
            osMap.put("darwin11Guest", "Mac OS 10.7");
            osMap.put("darwin12_64Guest", "Mac OS 10.8 (64 bit)");
            osMap.put("darwin13_64Guest", "Mac OS 10.9 (64 bit)");
            osMap.put("darwin64Guest", "Mac OS 10.5 (64 bit)");
            osMap.put("darwinGuest", "Mac OS 10.5");
            osMap.put("debian4_64Guest", "Debian GNU/Linux 4 (64 bit)");
            osMap.put("debian4Guest", "Debian GNU/Linux 4");
            osMap.put("debian5_64Guest", "Debian GNU/Linux 5 (64 bit)");
            osMap.put("debian5Guest", "Debian GNU/Linux 5");
            osMap.put("debian6_64Guest", "Debian GNU/Linux 6 (64 bit)");
            osMap.put("debian6Guest", "Debian GNU/Linux 6");
            osMap.put("debian7_64Guest", "Debian GNU/Linux 7 (64 bit)");
            osMap.put("debian7Guest", "Debian GNU/Linux 7");
            osMap.put("dosGuest", "MS-DOS.");
            osMap.put("eComStation2Guest", "eComStation 2.0");
            osMap.put("eComStationGuest", "eComStation 1.x");
            osMap.put("fedora64Guest", "Fedora Linux (64 bit)");
            osMap.put("fedoraGuest", "Fedora Linux");
            osMap.put("freebsd64Guest", "FreeBSD x64");
            osMap.put("freebsdGuest", "FreeBSD");
            osMap.put("genericLinuxGuest", "Other Linux");
            osMap.put("mandrakeGuest", "Mandrake Linux");
            osMap.put("mandriva64Guest", "Mandriva Linux (64 bit)");
            osMap.put("mandrivaGuest", "Mandriva Linux");
            osMap.put("netware4Guest", "Novell NetWare 4 ");
            osMap.put("netware5Guest", "Novell NetWare 5.1");
            osMap.put("netware6Guest", "Novell NetWare 6.x");
            osMap.put("nld9Guest", "Novell Linux Desktop 9");
            osMap.put("oesGuest", "Open Enterprise Server");
            osMap.put("openServer5Guest", "SCO OpenServer 5");
            osMap.put("openServer6Guest", "SCO OpenServer 6");
            osMap.put("opensuse64Guest", "OpenSUSE Linux (64 bit)");
            osMap.put("opensuseGuest", "OpenSUSE Linux");
            osMap.put("oracleLinux64Guest", "Oracle Linux 4/5 (64-bit)");
            osMap.put("oracleLinuxGuest", "Oracle Linux 4/5");
            osMap.put("os2Guest", "OS/2");
            osMap.put("redhatGuest", "Red Hat Linux 2.1");
            osMap.put("rhel2Guest", "Red Hat Enterprise Linux 2");
            osMap.put("rhel3_64Guest", "Red Hat Enterprise Linux 3 (64 bit)");
            osMap.put("rhel3Guest", "Red Hat Enterprise Linux 3");
            osMap.put("rhel4_64Guest", "Red Hat Enterprise Linux 4 (64 bit)");
            osMap.put("rhel4Guest", "Red Hat Enterprise Linux 4");
            osMap.put("rhel5_64Guest", "Red Hat Enterprise Linux 5 (64 bit)");
            osMap.put("rhel5Guest", "Red Hat Enterprise Linux 5");
            osMap.put("rhel6_64Guest", "Red Hat Enterprise Linux 6 (64 bit)");
            osMap.put("rhel6Guest", "Red Hat Enterprise Linux 6");
            osMap.put("rhel7_64Guest", "Red Hat Enterprise Linux 7 (64 bit)");
            osMap.put("rhel7Guest", "Red Hat Enterprise Linux 7");
            osMap.put("sjdsGuest", "Sun Java Desktop System");
            osMap.put("sles10_64Guest", "Suse Linux Enterprise Server 10 (64 bit)");
            osMap.put("sles10Guest", "Suse Linux Enterprise Server 10");
            osMap.put("sles11_64Guest", "Suse Linux Enterprise Server 11 (64 bit)");
            osMap.put("sles11Guest", "Suse Linux Enterprise Server 11");
            osMap.put("sles12_64Guest", "Suse Linux Enterprise Server 12 (64 bit)");
            osMap.put("sles12Guest", "Suse linux Enterprise Server 12");
            osMap.put("sles64Guest", "Suse Linux Enterprise Server 9 (64 bit)");
            osMap.put("slesGuest", "Suse Linux Enterprise Server 9");
            osMap.put("solaris10_64Guest", "Solaris 10 (64 bit) ");
            osMap.put("solaris10Guest", "Solaris 10 (32 bit) ");
            osMap.put("solaris11_64Guest", "Solaris 11 (64 bit)");
            osMap.put("solaris6Guest", "Solaris 6");
            osMap.put("solaris7Guest", "Solaris 7");
            osMap.put("solaris8Guest", "Solaris 8");
            osMap.put("solaris9Guest", "Solaris 9");
            osMap.put("suse64Guest", "Suse Linux (64 bit)");
            osMap.put("suseGuest", "Suse Linux");
            osMap.put("turboLinux64Guest", "Turbolinux (64 bit)");
            osMap.put("turboLinuxGuest", "Turbolinux");
            osMap.put("ubuntu64Guest", "Ubuntu Linux (64 bit)");
            osMap.put("ubuntuGuest", "Ubuntu Linux");
            osMap.put("unixWare7Guest", "SCO UnixWare 7");
            osMap.put("vmkernel5Guest", "VMware ESX 5");
            osMap.put("vmkernelGuest", "VMware ESX 4");
            osMap.put("win2000AdvServGuest", "Windows 2000 Advanced Server");
            osMap.put("win2000ProGuest", "Windows 2000 Professional");
            osMap.put("win2000ServGuest", "Windows 2000 Server");
            osMap.put("win31Guest", "Windows 3.1");
            osMap.put("win95Guest", "Windows 95");
            osMap.put("win98Guest", "Windows 98");
            osMap.put("windows7_64Guest", "Windows 7 (64 bit)");
            osMap.put("windows7Guest", "Windows 7");
            osMap.put("windows7Server64Guest", "Windows Server 2008 R2 (64 bit)");
            osMap.put("windows8_64Guest", "Windows 8 (64 bit)");
            osMap.put("windows8Guest", "Windows 8");
            osMap.put("windows8Server64Guest", "Windows 8 Server (64 bit)");
            osMap.put("windowsHyperVGuest", "Windows Hyper-V");
            osMap.put("winLonghorn64Guest", "Windows Longhorn (64 bit)");
            osMap.put("winLonghornGuest", "Windows Longhorn");
            osMap.put("winMeGuest", "Windows Millenium Edition");
            osMap.put("winNetBusinessGuest", "Windows Small Business Server 2003");
            osMap.put("winNetDatacenter64Guest", "Windows Server 2003, Datacenter Edition (64 bit)");
            osMap.put("winNetDatacenterGuest", "Windows Server 2003, Datacenter Edition");
            osMap.put("winNetEnterprise64Guest", "Windows Server 2003, Enterprise Edition (64 bit)");
            osMap.put("winNetEnterpriseGuest", "Windows Server 2003, Enterprise Edition");
            osMap.put("winNetStandard64Guest", "Windows Server 2003, Standard Edition (64 bit)");
            osMap.put("winNetStandardGuest", "Windows Server 2003, Standard Edition");
            osMap.put("winNetWebGuest", "Windows Server 2003, Web Edition");
            osMap.put("winNTGuest", "Windows NT 4");
            osMap.put("winVista64Guest", "Windows Vista (64 bit)");
            osMap.put("winVistaGuest", "Windows Vista");
            osMap.put("winXPHomeGuest", "Windows XP Home Edition");
            osMap.put("winXPPro64Guest", "Windows XP Professional Edition (64 bit)");
            osMap.put("winXPProGuest", "Windows XP Professional");

            list.add(osMap);
            cache.put(getProvider().getContext(), list);
        }
        return list.iterator().next();
    }
}
