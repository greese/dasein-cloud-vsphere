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
public class HardDisk extends AbstractVolumeSupport{

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
                String fileName2 = "[" + vm.getDatastores()[0].getName() + "]" + vm.getName() + "/" + volumeId;
                diskFileBacking.fileName = fileName2;
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
                                if (d != null) {
                                    list.add(d);
                                    fileNames.add(d.getTag("fileName"));
                                }
                            }
                        }
                    }
                }
            }

            //get .vmdk files
            Collection<StoragePool> pools = provider.getDataCenterServices().listStoragePools();
            Datacenter dc = provider.getDataCenterServices().getVmwareDatacenterFromVDCId(instance, ctx.getRegionId());

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
                    boolean found = false;
                    for (VirtualDevice device : devices) {
                        if (device instanceof VirtualDisk) {
                            VirtualDisk disk = (VirtualDisk)device;
                            diskId = disk.getDiskObjectId();
                            if (diskId == null || diskId.equals("")) {
                                //cloud has not returned an id so we need to infer it from vm and volume name
                                diskId = vm.getConfig().getInstanceUuid()+"-"+volume.getName();
                            }
                            if (diskId.equals(volumeId)) {
                                diskKey = disk.getKey();
                                found = true;
                                break;
                            }
                        }
                    }

                    if (found) {
                        VirtualDeviceConfigSpec diskSpec =
                                new VirtualDeviceConfigSpec();
                        diskSpec.setOperation(VirtualDeviceConfigSpecOperation.remove);
                        diskSpec.setFileOperation(
                                VirtualDeviceConfigSpecFileOperation.destroy);

                        VirtualDisk vd = new VirtualDisk();
                        vd.setKey(diskKey);
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

    private @Nullable Volume toVolume(@Nonnull VirtualDisk disk, @Nonnull String vmId, @Nonnull String dataCenterId, @Nonnull String regionId) {
        Volume volume = new Volume();
        volume.setProviderVolumeId(disk.getDiskObjectId());
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
        VirtualDeviceFileBackingInfo info = (VirtualDeviceFileBackingInfo)disk.getBacking();
        String filePath = info.getFileName();
        String fileName = filePath.substring(info.getFileName().lastIndexOf("/") + 1);
        volume.setTag("fileName", fileName);
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
        //volume.setSize(new Storage<Kilobyte>(disk.getCapacityKb(), Storage.KILOBYTE));
        volume.setType(VolumeType.SSD);
        return volume;
    }
}
