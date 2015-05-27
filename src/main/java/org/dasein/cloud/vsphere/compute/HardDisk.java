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

import com.vmware.vim25.*;
import com.vmware.vim25.mo.*;
import com.vmware.vim25.mo.VirtualMachine;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.StoragePool;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.vsphere.PrivateCloud;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Kilobyte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import java.rmi.RemoteException;
import java.util.*;

/**
 * User: daniellemayne
 * Date: 03/09/2014
 * Time: 12:07
 */
public class HardDisk extends AbstractVolumeSupport<PrivateCloud>{

    private PrivateCloud provider;
    HardDisk(@Nonnull PrivateCloud provider) {
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

    private transient volatile HardDiskCapabilities capabilities;
    @Override
    public VolumeCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new HardDiskCapabilities(provider);
        }
        return capabilities;
    }

    @Nonnull
    @Override
    public Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return getCapabilities().getMinimumVolumeSize();
    }

    @Nonnull
    @Override
    public String getProviderTermForVolume(@Nonnull Locale locale) {
        return "hard disk";
    }

    @Nonnull
    @Override
    public Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        return getCapabilities().listPossibleDeviceIds(platform);
    }

    @Override
    public void attach(@Nonnull String volumeId, @Nonnull String toServer, @Nonnull String deviceId) throws InternalException, CloudException {
        APITrace.begin(provider, "HardDisk.attach");
        try {
            try {
                ServiceInstance instance = getServiceInstance();
                Vm support = provider.getComputeServices().getVirtualMachineSupport();
                com.vmware.vim25.mo.VirtualMachine vm = support.getVirtualMachine(instance, toServer);
                if (vm == null) {
                    throw new CloudException("Unable to find vm with id "+toServer);
                }

                Volume volume = getVolume(volumeId);

                VirtualDeviceConfigSpec[] machineSpecs;

                VirtualDevice[] devices = vm.getConfig().getHardware().getDevice();
                int cKey = 1000;
                boolean scsiExists = false;
                for (VirtualDevice device : devices) {
                    if (device instanceof VirtualSCSIController) {
                        if (!scsiExists) {
                            cKey = device.getKey();
                            scsiExists = true;
                        }
                    }
                }

                if (!scsiExists) {
                    machineSpecs = new VirtualDeviceConfigSpec[2];
                    VirtualDeviceConfigSpec scsiSpec =
                            new VirtualDeviceConfigSpec();
                    scsiSpec.setOperation(VirtualDeviceConfigSpecOperation.add);
                    VirtualLsiLogicSASController scsiCtrl =
                            new VirtualLsiLogicSASController();
                    scsiCtrl.setKey(cKey);
                    scsiCtrl.setBusNumber(0);
                    scsiCtrl.setSharedBus(VirtualSCSISharing.noSharing);
                    scsiSpec.setDevice(scsiCtrl);
                    machineSpecs[0] = scsiSpec;
                } else {
                    machineSpecs = new VirtualDeviceConfigSpec[1];
                }

                VirtualDisk disk = new VirtualDisk();

                disk.controllerKey = cKey;
                disk.unitNumber = Integer.parseInt(deviceId);

                VirtualDeviceConfigSpec diskSpec =
                        new VirtualDeviceConfigSpec();
                diskSpec.operation = VirtualDeviceConfigSpecOperation.add;
                diskSpec.device = disk;

                VirtualDiskFlatVer2BackingInfo diskFileBacking = new VirtualDiskFlatVer2BackingInfo();
                String fileName = volume.getTag("filePath");
                diskFileBacking.fileName = fileName;
                diskFileBacking.diskMode = "persistent";
                diskFileBacking.thinProvisioned = true;
                disk.backing = diskFileBacking;


                if (!scsiExists) {
                    machineSpecs[1] = diskSpec;
                }
                else {
                    machineSpecs[0] = diskSpec;
                }


                VirtualMachineConfigSpec spec = new VirtualMachineConfigSpec();
                spec.setDeviceChange(machineSpecs);

                CloudException lastError = null;
                Task task = vm.reconfigVM_Task(spec);
                String status = task.waitForTask();

                if( !status.equals(Task.SUCCESS) ) {
                    lastError = new CloudException("Failed to attach volume: " + task.getTaskInfo().getError().getLocalizedMessage());
                }
                if( lastError != null ) {
                    throw lastError;
                }
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
            catch (InterruptedException e) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.alterVirtualMachine");
        try {
            if (options.getProviderVirtualMachineId() == null) {
                throw new CloudException("Volumes can only be created in the context of a vm for "+getProvider().getCloudName()+". ProviderVirtualMachineId cannot be null");
            }
            ServiceInstance instance = getServiceInstance();
            Vm vmSupport = getProvider().getComputeServices().getVirtualMachineSupport();
            com.vmware.vim25.mo.VirtualMachine vm = vmSupport.getVirtualMachine(instance, options.getProviderVirtualMachineId());

            if( vm != null ) {
                try {
                    //volumes change
                    VirtualDeviceConfigSpec[] machineSpecs = null;

                    VirtualDevice[] devices = vm.getConfig().getHardware().getDevice();
                    int cKey = 1000;
                    boolean scsiExists = false;
                    int numDisks = 0;
                    List<String> diskNames = new ArrayList<String>();
                    for (VirtualDevice device : devices) {
                        if (device instanceof VirtualSCSIController) {
                            if (!scsiExists) {
                                cKey = device.getKey();
                                scsiExists = true;
                            }
                        }
                        else if (device instanceof VirtualDisk) {
                            numDisks++;
                            VirtualDisk vDisk = (VirtualDisk) device;
                            VirtualDiskFlatVer2BackingInfo bkInfo = (VirtualDiskFlatVer2BackingInfo) vDisk.getBacking();
                            diskNames.add(bkInfo.getFileName());
                        }
                    }

                    if (!scsiExists) {
                        machineSpecs = new VirtualDeviceConfigSpec[2];
                        VirtualDeviceConfigSpec scsiSpec = new VirtualDeviceConfigSpec();
                        scsiSpec.setOperation(VirtualDeviceConfigSpecOperation.add);
                        VirtualLsiLogicSASController scsiCtrl = new VirtualLsiLogicSASController();
                        scsiCtrl.setKey(cKey);
                        scsiCtrl.setBusNumber(0);
                        scsiCtrl.setSharedBus(VirtualSCSISharing.noSharing);
                        scsiSpec.setDevice(scsiCtrl);
                        machineSpecs[0] = scsiSpec;
                    } else {
                        machineSpecs = new VirtualDeviceConfigSpec[1];
                    }
                    // Associate the virtual disk with the scsi controller
                    VirtualDisk disk = new VirtualDisk();

                    disk.controllerKey = cKey;
                    disk.unitNumber = numDisks;
                    //Storage<Gigabyte> diskGB = options.getVolumeSize();
                    //Storage<Kilobyte> diskByte = (Storage<Kilobyte>) (diskGB.convertTo(Storage.KILOBYTE)); //Proper conversion is not desired here
                    //Storage<Kilobyte> diskByte = new Storage<Kilobyte>((diskGB.intValue() * 1000), Storage.KILOBYTE);
                    //disk.capacityInKB = diskByte.longValue();
                    disk.setCapacityInKB(options.getVolumeSize().intValue() * 1000000);

                    VirtualDeviceConfigSpec diskSpec = new VirtualDeviceConfigSpec();
                    diskSpec.operation = VirtualDeviceConfigSpecOperation.add;
                    diskSpec.fileOperation = VirtualDeviceConfigSpecFileOperation.create;
                    diskSpec.device = disk;

                    VirtualDiskFlatVer2BackingInfo diskFileBacking = new VirtualDiskFlatVer2BackingInfo();
                    String fileName2 = "[" + vm.getDatastores()[0].getName() + "]" + vm.getName() + "/" + options.getName();
                    diskFileBacking.setFileName(fileName2);
                    diskFileBacking.setDiskMode("persistent");
                    diskFileBacking.setThinProvisioned(false);
                    diskFileBacking.setWriteThrough(false);
                    disk.backing = diskFileBacking;

                    if (!scsiExists) {
                        machineSpecs[1] = diskSpec;
                    }
                    else {
                        machineSpecs[0] = diskSpec;
                    }

                    VirtualMachineConfigSpec spec = new VirtualMachineConfigSpec();
                    spec.setDeviceChange(machineSpecs);

                    CloudException lastError;
                    Task task = vm.reconfigVM_Task(spec);

                    String status = task.waitForTask();

                    if( status.equals(Task.SUCCESS) ) {
                        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);

                        while( System.currentTimeMillis() < timeout ) {
                            try { Thread.sleep(10000L); }
                            catch( InterruptedException ignore ) { }

                            vm = vmSupport.getVirtualMachine(instance, options.getProviderVirtualMachineId());
                            devices = vm.getConfig().getHardware().getDevice();
                            for (VirtualDevice device : devices) {
                                if (device instanceof VirtualDisk) {
                                    VirtualDisk vDisk = (VirtualDisk) device;
                                    VirtualDiskFlatVer2BackingInfo bkInfo = (VirtualDiskFlatVer2BackingInfo) vDisk.getBacking();
                                    String diskFileName = bkInfo.getFileName();
                                    if (!diskNames.contains(diskFileName)) {
                                        diskFileName = diskFileName.substring(diskFileName.lastIndexOf("/") + 1);
                                        return diskFileName;
                                    }
                                }
                            }
                        }
                        lastError = new CloudException("Unable to identify new volume.");
                    }
                    else {
                        lastError = new CloudException("Failed to create volume: " + task.getTaskInfo().getError().getLocalizedMessage());
                    }
                    if( lastError != null ) {
                        throw lastError;
                    }
                    throw new CloudException("No volume and no error");
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
                catch (InterruptedException e) {
                    throw new CloudException(e);
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void detach(@Nonnull String volumeId, boolean force) throws InternalException, CloudException {
        APITrace.begin(provider, "HardDisk.detach");
        try {
            Volume volume = getVolume(volumeId);
            if (volume.getProviderVirtualMachineId() == null) {
                throw new CloudException("Volume not currently attached");
            }

            Vm support = provider.getComputeServices().getVirtualMachineSupport();
            ServiceInstance instance = getServiceInstance();
            VirtualMachine vm = support.getVirtualMachine(instance, volume.getProviderVirtualMachineId());

            if (vm != null) {
                try {
                    VirtualDeviceConfigSpec[] machineSpecs = new VirtualDeviceConfigSpec[1];

                    VirtualDevice[] devices = vm.getConfig().getHardware().getDevice();
                    String diskId;
                    int diskKey = 0;
                    int controller = 0;
                    boolean found = false;
                    for (VirtualDevice device : devices) {
                        if (device instanceof VirtualDisk) {
                            VirtualDisk disk = (VirtualDisk)device;
                            VirtualDeviceFileBackingInfo info = (VirtualDeviceFileBackingInfo)disk.getBacking();
                            String filePath = info.getFileName();
                            diskId = filePath.substring(info.getFileName().lastIndexOf("/") + 1);
                            if (diskId == null || diskId.equals("")) {
                                //cloud has not returned an id so we need to infer it from vm and volume name
                                diskId = vm.getConfig().getInstanceUuid()+"-"+volume.getName();
                            }
                            if (diskId.equals(volumeId)) {
                                diskKey = disk.getKey();
                                controller = disk.getControllerKey();
                                found = true;
                                break;
                            }
                        }
                    }

                    if (found) {
                        VirtualDeviceConfigSpec diskSpec =
                                new VirtualDeviceConfigSpec();
                        diskSpec.setOperation(VirtualDeviceConfigSpecOperation.remove);

                        VirtualDisk vd = new VirtualDisk();
                        vd.setKey(diskKey);
                        vd.setControllerKey(controller);
                        diskSpec.setDevice(vd);

                        machineSpecs[0] = diskSpec;

                        VirtualMachineConfigSpec spec = new VirtualMachineConfigSpec();
                        spec.setDeviceChange(machineSpecs);

                        CloudException lastError = null;
                        Task task = vm.reconfigVM_Task(spec);

                        String status = task.waitForTask();

                        if( !status.equals(Task.SUCCESS) ) {
                            lastError = new CloudException("Failed to update VM: " + task.getTaskInfo().getError().getLocalizedMessage());
                        }
                        if( lastError != null ) {
                            throw lastError;
                        }
                    }
                    else {
                        throw new CloudException("Couldn't find device "+volumeId+" to detach in vm "+vm.getName());
                    }
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
                catch (InterruptedException e) {
                    throw new CloudException(e);
                }
            }
            else {
                throw new CloudException("Can't find vm with id "+vm.getConfig().getInstanceUuid());
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Iterable<Volume> listVolumes() throws InternalException, CloudException {
        APITrace.begin(provider, "HardDisk.listVolumes");
        try {
            List<Volume> list = new ArrayList<Volume>();
            List<String> fileNames = new ArrayList<String>();
            ProviderContext ctx = provider.getContext();
            if (ctx != null) {
                if (ctx.getRegionId() == null) {
                    throw new CloudException("Region id is not set");
                }
            }

            ServiceInstance instance = getServiceInstance();

            //get attached volumes
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
                    VirtualMachine vm = (VirtualMachine)entity;
                    if(vm != null && vm.getConfig() != null){
                        Platform guestOs = Platform.guess(vm.getConfig().getGuestFullName());
                        if (!vm.getConfig().isTemplate() && (vm.getRuntime().getPowerState().equals(VirtualMachinePowerState.poweredOn) || vm.getRuntime().getPowerState().equals(VirtualMachinePowerState.poweredOff))) {
                            String dc2;
                            try {
                                dc2 = vm.getResourcePool().getOwner().getName();
                            }
                            catch( RemoteException e ) {
                                throw new CloudException(e);
                            }

                            if( dc2 == null ) {
                                return Collections.emptyList();
                            }
                            DataCenter ourDC = provider.getDataCenterServices().getDataCenter(dc2);
                            String regionId = "";
                            if (ourDC == null) {
                                dc2 = dc2+"-a";
                                regionId = dc2;
                            }
                            else {
                                regionId = ourDC.getRegionId();
                            }
                            VirtualDevice[] devices = vm.getConfig().getHardware().getDevice();
                            for (VirtualDevice device : devices) {
                                if (device instanceof VirtualDisk) {
                                    VirtualDisk disk = (VirtualDisk)device;
                                    Volume d = toVolume(disk, vm.getConfig().getInstanceUuid(), dc2, regionId);
                                    if (d != null) {
                                        d.setGuestOperatingSystem(guestOs);
                                        list.add(d);
                                        fileNames.add(d.getProviderVolumeId());
                                    }
                                }
                            }
                        }
                    }
                    else throw new CloudException("An error occurred while listing Volumes: VM could not be properly retrieved from the cloud.");
                }
            }

            //get .vmdk files
            Collection<StoragePool> pools = provider.getDataCenterServices().listStoragePools();
            Datacenter dc = provider.getDataCenterServices().getVmwareDatacenterFromVDCId(instance, ctx.getRegionId());
            String name = dc.getName();
            for (Datastore ds : dc.getDatastores()) {
                String dataCenterId = null;
                for (StoragePool pool : pools) {
                    if (pool.getStoragePoolName().equalsIgnoreCase(ds.getName())) {
                        dataCenterId = pool.getDataCenterId();
                        break;
                    }
                }
                HostDatastoreBrowser browser = ds.getBrowser();
                try {
                    Task task = browser.searchDatastoreSubFolders_Task("[" + ds.getName() + "]", null);
                    String status = task.waitForTask();
                    if( status.equals(Task.SUCCESS) ) {
                        ArrayOfHostDatastoreBrowserSearchResults result = (ArrayOfHostDatastoreBrowserSearchResults)task.getTaskInfo().getResult();
                        HostDatastoreBrowserSearchResults[] res = result.getHostDatastoreBrowserSearchResults();
                        for (HostDatastoreBrowserSearchResults r : res) {
                            FileInfo[] files = r.getFile();
                            if (files != null) {
                                for (FileInfo file : files) {
                                    String filePath = file.getPath();
                                    if (filePath.endsWith(".vmdk") && !filePath.endsWith("-flat.vmdk")) {
                                        if (!fileNames.contains(file.getPath())) {
                                            Volume d = toVolume(file, dataCenterId, ctx.getRegionId());
                                            if (d != null) {
                                                d.setTag("filePath", r.getFolderPath()+d.getProviderVolumeId());
                                                list.add(d);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else {
                        throw new CloudException("Failed listing volumes: " + task.getTaskInfo().getError().getLocalizedMessage());
                    }
                }
                catch (InterruptedException e) {
                    throw new InternalException(e);
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
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (provider.getServiceInstance() != null);
    }

    @Override
    public void remove(@Nonnull String volumeId) throws InternalException, CloudException {
        APITrace.begin(provider, "HardDisk.remove");
        try {
            Volume volume = null;
            Iterable<Volume> attachedVolumes = getAttachedVolumes();
            for (Volume v : attachedVolumes) {
                if (v.getProviderVolumeId().equals(volumeId)) {
                    throw new CloudException("Volume is attached to vm "+v.getProviderVirtualMachineId()+" - removing not allowed");
                }
            }

            volume = getVolume(volumeId);

            if (volume != null) {
                ServiceInstance instance = provider.getServiceInstance();

                Datacenter dc = provider.getDataCenterServices().getVmwareDatacenterFromVDCId(instance, provider.getContext().getRegionId());
                ManagedObjectReference mor = instance.getServiceContent().getFileManager();
                if (mor.getType().equals("FileManager")) {
                    FileManager fileManager = new FileManager(instance.getServerConnection(), mor);
                    String filePath = volume.getTag("filePath");
                    fileManager.deleteDatastoreFile_Task(filePath, dc);
                    //also delete the flat file
                    String flatfile = filePath.substring(0, filePath.indexOf(".vmdk"))+"-flat.vmdk";
                    fileManager.deleteDatastoreFile_Task(flatfile, dc);
                }
            }
            else {
                throw new CloudException("Unable to find volume with id "+volumeId);
            }
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
        finally {
            APITrace.end();
        }
    }

    private Iterable<Volume> getAttachedVolumes() throws InternalException, CloudException {
        APITrace.begin(provider, "HardDisk.getAttachedVolumes");
        try {
            List<Volume> list = new ArrayList<Volume>();
            List<String> fileNames = new ArrayList<String>();
            ProviderContext ctx = provider.getContext();
            if (ctx != null) {
                if (ctx.getRegionId() == null) {
                    throw new CloudException("Region id is not set");
                }
            }

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
                    VirtualMachine vm = (VirtualMachine)entity;
                    Platform guestOs = Platform.guess(vm.getConfig().getGuestFullName());
                    if (vm != null && !vm.getConfig().isTemplate()) {
                        String dc2;
                        try {
                            dc2 = vm.getResourcePool().getOwner().getName();
                        }
                        catch( RemoteException e ) {
                            throw new CloudException(e);
                        }

                        if( dc2 == null ) {
                            return Collections.emptyList();
                        }
                        DataCenter ourDC = provider.getDataCenterServices().getDataCenter(dc2);
                        String regionId = "";
                        if (ourDC == null) {
                            dc2 = dc2+"-a";
                            regionId = dc2;
                        }
                        else {
                            regionId = ourDC.getRegionId();
                        }
                        VirtualDevice[] devices = vm.getConfig().getHardware().getDevice();
                        for (VirtualDevice device : devices) {
                            if (device instanceof VirtualDisk) {
                                VirtualDisk disk = (VirtualDisk)device;
                                Volume d = toVolume(disk, vm.getConfig().getInstanceUuid(), dc2, regionId);
                                if (d != null && !fileNames.contains(d.getTag("filePath"))) {
                                    d.setGuestOperatingSystem(guestOs);
                                    list.add(d);
                                    fileNames.add(d.getTag("filePath"));
                                }
                            }
                        }
                    }
                }
            }
            return list;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable Volume toVolume(@Nonnull VirtualDisk disk, @Nonnull String vmId, @Nonnull String dataCenterId, @Nonnull String regionId) {
        Volume volume = new Volume();

        VirtualDeviceFileBackingInfo info = (VirtualDeviceFileBackingInfo)disk.getBacking();
        String filePath = info.getFileName();
        String fileName = filePath.substring(info.getFileName().lastIndexOf("/") + 1);
        volume.setTag("filePath", filePath);

        volume.setProviderVolumeId(fileName);
        volume.setName(disk.getDeviceInfo().getLabel());
        volume.setProviderDataCenterId(dataCenterId);
        volume.setProviderRegionId(regionId);
        volume.setDescription(disk.getDeviceInfo().getSummary());
        volume.setCurrentState(VolumeState.AVAILABLE);
        volume.setDeleteOnVirtualMachineTermination(true);
        volume.setDeviceId(disk.getUnitNumber().toString());
        volume.setFormat(VolumeFormat.BLOCK);
        volume.setProviderVirtualMachineId(vmId);
        volume.setSize(new Storage<Kilobyte>(disk.getCapacityInKB(), Storage.KILOBYTE));
        volume.setType(VolumeType.SSD);

        if (volume.getProviderVolumeId() == null) {
            volume.setProviderVolumeId(vmId+"-"+volume.getName());
        }
        if (volume.getDeviceId().equals("0")) {
            volume.setRootVolume(true);
        }
        return volume;
    }

    private @Nullable Volume toVolume(@Nonnull FileInfo disk, @Nullable String dataCenterId, @Nonnull String regionId) {
        Volume volume = new Volume();
        volume.setProviderVolumeId(disk.getPath());
        volume.setName(disk.getPath());
        volume.setProviderDataCenterId(dataCenterId);
        volume.setProviderRegionId(regionId);
        volume.setDescription(disk.getPath());
        volume.setCurrentState(VolumeState.AVAILABLE);
        volume.setDeleteOnVirtualMachineTermination(true);
        volume.setFormat(VolumeFormat.BLOCK);
        if (disk.getFileSize() != null) {
            volume.setSize(new Storage<org.dasein.util.uom.storage.Byte>(disk.getFileSize(), Storage.BYTE));
        }
        volume.setType(VolumeType.SSD);
        Calendar cal = disk.getModification();
        if (cal != null) {
            volume.setCreationTimestamp(cal.getTimeInMillis());
        }
        volume.setRootVolume(false);
        return volume;
    }
}