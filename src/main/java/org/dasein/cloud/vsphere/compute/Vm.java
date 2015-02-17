/**
 * Copyright (C) 2010-2014 Dell, Inc
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

import com.vmware.vim25.*;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.apache.log4j.Logger;
import org.dasein.cloud.compute.*;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.vsphere.PrivateCloud;

import com.vmware.vim25.mo.ComputeResource;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.Datastore;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ResourcePool;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;

import org.dasein.util.CalendarWrapper;

import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Kilobyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Minute;
import org.dasein.util.uom.time.TimePeriod;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

public class Vm extends AbstractVMSupport<PrivateCloud> {
    static private final Logger log = PrivateCloud.getLogger(Vm.class, "std");

    Vm(@Nonnull PrivateCloud provider) {
        super(provider);
    }

    private @Nonnull ServiceInstance getServiceInstance() throws CloudException, InternalException {
        ServiceInstance instance = getProvider().getServiceInstance();

        if( instance == null ) {
            throw new CloudException(CloudErrorType.AUTHENTICATION, HttpServletResponse.SC_UNAUTHORIZED, null, "Unauthorized");
        }
        return instance;
    }
    
    @Override
    public void start(@Nonnull String serverId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.start");
        try {
            ServiceInstance instance = getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);

            if( vm != null ) {
                try {
                    String datacenter = vm.getResourcePool().getOwner().getName();
                    Datacenter dc = getVmwareDatacenter(vm);

                    if( dc == null ) {
                        throw new CloudException("Could not identify a deployment data center.");
                    }
                    HostSystem host = getHost(vm);
                    Task task = null;
                    if (host == null)  {
                        task = vm.powerOnVM_Task(getBestHost(dc, datacenter));
                    }
                    else {
                        task = vm.powerOnVM_Task(host);
                    }
                    String status = task.waitForTask();

                    if( !status.equals(Task.SUCCESS) ) {
                        if (task.getTaskInfo().getError().getLocalizedMessage().contains("lock the file")) {
                            throw new CloudException("Failed to start VM: " + task.getTaskInfo().getError().getLocalizedMessage()+". This vm may be using a disk file already in use");
                        }
                        throw new CloudException("Failed to start VM: " + task.getTaskInfo().getError().getLocalizedMessage());
                    }
                }
                catch( TaskInProgress e ) {
                    throw new CloudException(e);
                }
                catch( InvalidState e ) {
                    throw new CloudException(e);
                }
                catch( RuntimeFault e ) {
                    throw new InternalException(e);
                }
                catch( RemoteException e ) {
                    throw new CloudException(e);
                }
                catch ( InterruptedException e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull com.vmware.vim25.mo.VirtualMachine clone(@Nonnull ServiceInstance instance, @Nonnull com.vmware.vim25.mo.VirtualMachine vm, @Nonnull String name,  boolean asTemplate) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.clone(ServiceInstance, VirtualMachine)");
        try {
            try {
                String dcId = getDataCenter(vm);

                if( dcId == null ) {
                    throw new CloudException("Virtual machine " + vm + " has no data center parent");
                }
                name = validateName(name);

                Datacenter dc = null;
                DataCenter ourDC = getProvider().getDataCenterServices().getDataCenter(dcId);
                if (ourDC != null) {
                    dc = getProvider().getDataCenterServices().getVmwareDatacenterFromVDCId(instance, ourDC.getRegionId());
                }
                else {
                    dc = getProvider().getDataCenterServices().getVmwareDatacenterFromVDCId(instance, dcId);
                }
                ResourcePool pool = vm.getResourcePool();

                if( dc == null ) {
                    throw new CloudException("Invalid DC for cloning operation: " + dcId);
                }
                Folder vmFolder = dc.getVmFolder();

                VirtualMachineConfigSpec config = new VirtualMachineConfigSpec();
                VirtualMachineProduct product = getProduct(vm.getConfig().getHardware());
                String[] sizeInfo = product.getProviderProductId().split(":");
                int cpuCount = Integer.parseInt(sizeInfo[0]);
                long memory = Long.parseLong(sizeInfo[1]);

                config.setName(name);
                config.setAnnotation(vm.getConfig().getAnnotation());
                config.setMemoryMB(memory);
                config.setNumCPUs(cpuCount);

                VirtualMachineCloneSpec spec = new VirtualMachineCloneSpec();
                VirtualMachineRelocateSpec location = new VirtualMachineRelocateSpec();

                HostSystem host = getHost(vm);
                if (host == null)  {
                    location.setHost(getBestHost(dc, dcId).getConfig().getHost());
                }
                else {
                    location.setHost(host.getConfig().getHost());
                }
                location.setPool(pool.getConfig().getEntity());
                spec.setLocation(location);
                spec.setPowerOn(false);
                spec.setTemplate(asTemplate);
                spec.setConfig(config);

                Task task = vm.cloneVM_Task(vmFolder, name, spec);
                String status = task.waitForTask();

                if( status.equals(Task.SUCCESS) ) {
                    return (com.vmware.vim25.mo.VirtualMachine)(new InventoryNavigator(vmFolder).searchManagedEntity("VirtualMachine", name));
                }
                else {
                    throw new CloudException("Failed to create VM: " + task.getTaskInfo().getError().getLocalizedMessage());
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
            catch( InterruptedException e ) {
                throw new InternalException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public VirtualMachine alterVirtualMachineSize(@Nonnull String virtualMachineId, @Nullable String cpuCount, @Nullable String ramInMB) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.alterVirtualMachine");
        try {
            ServiceInstance instance = getServiceInstance();
            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, virtualMachineId);

            if( vm != null ) {
                if (cpuCount != null || ramInMB != null) {

                    int cpuCountVal;
                    long memoryVal;

                    try {
                        VirtualMachineConfigSpec spec = new VirtualMachineConfigSpec();
                        if (ramInMB != null) {
                            memoryVal = Long.parseLong(ramInMB);
                            spec.setMemoryMB(memoryVal);
                        }
                        if (cpuCount != null) {
                            cpuCountVal = Integer.parseInt(cpuCount);
                            spec.setNumCPUs(cpuCountVal);
                        }

                        CloudException lastError;
                        Task task = vm.reconfigVM_Task(spec);

                        String status = task.waitForTask();

                        if( status.equals(Task.SUCCESS) ) {
                            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);

                            while( System.currentTimeMillis() < timeout ) {
                                try { Thread.sleep(10000L); }
                                catch( InterruptedException ignore ) { }

                                for( VirtualMachine s : listVirtualMachines() ) {
                                    if( s.getProviderVirtualMachineId().equals(virtualMachineId) ) {
                                        return s;
                                    }
                                }
                            }
                            lastError = new CloudException("Unable to identify updated server.");
                        }
                        else {
                            lastError = new CloudException("Failed to update VM: " + task.getTaskInfo().getError().getLocalizedMessage());
                        }
                        if( lastError != null ) {
                            throw lastError;
                        }
                        throw new CloudException("No server and no error");
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
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull VirtualMachine clone(@Nonnull String serverId, @Nullable String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String ... firewallIds) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.clone");
        try {
            ServiceInstance instance = getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);

            if( vm != null ) {
                VirtualMachine target = toServer(clone(instance, vm, name, false), description);

                if( target == null ) {
                    throw new CloudException("Request appeared to succeed, but no VM was created");
                }
                if( powerOn ) {
                    try { Thread.sleep(5000L); }
                    catch( InterruptedException ignore ) { /* ignore */ }
                    String id = target.getProviderVirtualMachineId();

                    if( id == null ) {
                        throw new CloudException("Got a VM without an ID");
                    }
                    start(id);
                }
                return target;
            }
            throw new CloudException("No virtual machine " + serverId + ".");
        }
        finally {
            APITrace.end();
        }
    }

    private transient volatile VMCapabilities capabilities;
    @Nonnull
    @Override
    public VirtualMachineCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new VMCapabilities(getProvider());
        }
        return capabilities;
    }

    private ManagedEntity[] randomize(ManagedEntity[] source) {
        return source; // TODO: make this random
    }

    private Random random = new Random();
    
    private @Nonnull VirtualMachine defineFromTemplate(@Nonnull VMLaunchOptions options) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.define");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new InternalException("No context was set for this request");
            }
            ServiceInstance instance = getServiceInstance();
            try {
                com.vmware.vim25.mo.VirtualMachine template = getTemplate(instance, options.getMachineImageId());

                if( template == null ) {
                    throw new CloudException("No such template: " + options.getMachineImageId());
                }
                String hostName = validateName(options.getHostName());
                String dataCenterId = options.getDataCenterId();
                String resourceProductStr = options.getStandardProductId();
                String[] items = resourceProductStr.split(":");
                if (items.length==3) {
                    options.withResourcePoolId(items[0]);
                }

                if( dataCenterId == null ) {
                    String rid = ctx.getRegionId();

                    if( rid != null ) {
                        for( DataCenter dsdc : getProvider().getDataCenterServices().listDataCenters(rid) ) {
                            dataCenterId = dsdc.getProviderDataCenterId();
                            if( random.nextInt()%3 == 0 ) {
                                break;
                            }
                        }
                    }
                }
                ManagedEntity[] pools = null;

                Datacenter vdc = null;

                if( dataCenterId != null ) {
                    DataCenter ourDC = getProvider().getDataCenterServices().getDataCenter(dataCenterId);
                    if (ourDC != null) {
                        vdc = getProvider().getDataCenterServices().getVmwareDatacenterFromVDCId(instance, ourDC.getRegionId());

                        if( vdc == null ) {
                            throw new CloudException("Unable to identify VDC " + dataCenterId);
                        }

                        if (options.getResourcePoolId() == null) {
                            ResourcePool pool = getProvider().getDataCenterServices().getResourcePoolFromClusterId(instance, dataCenterId);
                            if( pool != null ) {
                                pools = new ManagedEntity[] { pool };
                            }
                        }
                    }
                    else {
                        vdc = getProvider().getDataCenterServices().getVmwareDatacenterFromVDCId(instance, dataCenterId);
                        if (options.getResourcePoolId() == null) {
                            pools = new InventoryNavigator(vdc).searchManagedEntities("ResourcePool");
                        }
                    }
                }

                CloudException lastError = null;

                if (options.getResourcePoolId() != null) {
                    ResourcePool pool = getProvider().getDataCenterServices().getVMWareResourcePool(options.getResourcePoolId());
                    if (pool != null) {
                        pools = new ManagedEntity[] {pool};
                    }
                    else {
                        throw new CloudException("Unable to find resource pool with id "+options.getResourcePoolId());
                    }
                }

                for( ManagedEntity p : pools ) {
                    ResourcePool pool = (ResourcePool)p;
                    Folder vmFolder = vdc.getVmFolder();
                    if (options.getVmFolderId() != null) {
                        ManagedEntity tmp = new InventoryNavigator(vmFolder).searchManagedEntity("Folder", options.getVmFolderId());
                        if (tmp != null) {
                            vmFolder = (Folder)tmp;
                        }
                    }

                    VirtualMachineConfigSpec config = new VirtualMachineConfigSpec();
                    String[] vmInfo = options.getStandardProductId().split(":");
                    int cpuCount;
                    long memory;
                    if (vmInfo.length == 2) {
                        cpuCount = Integer.parseInt(vmInfo[0]);
                        memory = Long.parseLong(vmInfo[1]);
                    }
                    else {
                        cpuCount = Integer.parseInt(vmInfo[1]);
                        memory = Long.parseLong(vmInfo[2]);
                    }

                    config.setName(hostName);
                    config.setAnnotation(options.getMachineImageId());
                    config.setMemoryMB(memory);
                    config.setNumCPUs(cpuCount);

                    //networking section
                    //borrowed heavily from https://github.com/jedi4ever/jvspherecontrol
                    String vlan = options.getVlanId();
                    int count = 0;
                    int networkIndex = 0;
                    if (vlan != null) {

                        // we don't need to do network config if the selected network
                        // is part of the template config anyway
                        boolean changeRequired = true;
                        Integer[] keys = new Integer[count];
                        GuestNicInfo[] nics = template.getGuest().getNet();
                        if (nics != null) {
                            count = nics.length;
                            keys = new Integer[count];
                            for (int i = 0; i<count; i++) {
                                if (nics[i].getNetwork().equals(vlan)) {
                                    changeRequired = false;
                                    networkIndex = i;
                                    break;
                                }
                                else {
                                    keys[i] = nics[i].getDeviceConfigId();
                                }
                            }
                        }
                        else {
                            log.warn("Unable to find network adapter info for template "+template.getName()+"("+template.getConfig().getInstanceUuid()+")");
                        }

                        if (changeRequired) {
                            if (count > 0) {
                                VirtualDeviceConfigSpec[] machineSpecs = new VirtualDeviceConfigSpec[keys.length+1];
                                for (int j = 0; j<keys.length; j++) {
                                    VirtualDeviceConfigSpec nicSpec = new VirtualDeviceConfigSpec();
                                    nicSpec.setOperation(VirtualDeviceConfigSpecOperation.remove);

                                    VirtualEthernetCard nic = new VirtualE1000();
                                    nic.setKey(keys[j]);

                                    nicSpec.setDevice(nic);
                                    machineSpecs[j]=nicSpec;
                                }
                                VirtualDeviceConfigSpec nicSpec = new VirtualDeviceConfigSpec();
                                nicSpec.setOperation(VirtualDeviceConfigSpecOperation.add);

                                VirtualEthernetCard nic = new VirtualE1000();
                                nic.setConnectable(new VirtualDeviceConnectInfo());
                                nic.connectable.connected=true;
                                nic.connectable.startConnected=true;

                                Description info = new Description();
                                info.setLabel(vlan);
                                info.setSummary("Nic for network "+vlan);

                                VirtualEthernetCardNetworkBackingInfo nicBacking = new VirtualEthernetCardNetworkBackingInfo();
                                nicBacking.setDeviceName(vlan);

                                nic.setAddressType("generated");
                                nic.setBacking(nicBacking);
                                nic.setKey(0);

                                nicSpec.setDevice(nic);

                                machineSpecs[keys.length]=nicSpec;

                                config.setDeviceChange(machineSpecs);
                            }
                            else {
                                VirtualDeviceConfigSpec nicSpec = new VirtualDeviceConfigSpec();
                                nicSpec.setOperation(VirtualDeviceConfigSpecOperation.add);

                                VirtualEthernetCard nic = new VirtualE1000();
                                nic.setConnectable(new VirtualDeviceConnectInfo());
                                nic.connectable.connected=true;
                                nic.connectable.startConnected=true;

                                Description info = new Description();
                                info.setLabel(vlan);
                                info.setSummary("Nic for network "+vlan);

                                VirtualEthernetCardNetworkBackingInfo nicBacking = new VirtualEthernetCardNetworkBackingInfo();
                                nicBacking.setDeviceName(vlan);

                                nic.setAddressType("generated");
                                nic.setBacking(nicBacking);
                                nic.setKey(0);

                                nicSpec.setDevice(nic);

                                VirtualDeviceConfigSpec[] machineSpecs = new VirtualDeviceConfigSpec[1];
                                machineSpecs[0]=nicSpec;

                                config.setDeviceChange(machineSpecs);
                            }
                        }
                        // end networking section
                    }

                    VirtualMachineRelocateSpec location = new VirtualMachineRelocateSpec();
                    if (options.getAffinityGroupId() != null) {
                        Host agSupport= getProvider().getComputeServices().getAffinityGroupSupport();
                        location.setHost(agSupport.getHostSystemForAffinity(options.getAffinityGroupId()).getConfig().getHost());
                    }
                    if (options.getStoragePoolId() != null) {
                        String locationId = options.getStoragePoolId();

                        Datastore[] datastores = vdc.getDatastores();
                        for (Datastore ds : datastores) {
                            if (ds.getName().equals(locationId)) {
                                location.setDatastore(ds.getMOR());
                                break;
                            }
                        }
                    }
                    location.setPool(pool.getConfig().getEntity());

                    boolean isCustomised = false;
                    if (options.getPrivateIp() != null) {
                        isCustomised = true;
                        log.debug("isCustomised");
                    } else {
                        log.debug("notCustomised");
                    }
                    CustomizationSpec customizationSpec = new CustomizationSpec();
                    if (isCustomised) {
                        String templatePlatform = template.getGuest().getGuestFullName();
                        if(templatePlatform == null) templatePlatform = template.getName();
                        Platform platform = Platform.guess(templatePlatform.toLowerCase());
                        if (platform.isLinux()) {
                            CustomizationLinuxPrep lPrep = new CustomizationLinuxPrep();
                            lPrep.setDomain(options.getDnsDomain());
                            lPrep.setHostName(new CustomizationVirtualMachineName());
                            customizationSpec.setIdentity(lPrep);
                        }
                        else if (platform.isWindows()) {
                            CustomizationSysprep sysprep = new CustomizationSysprep();

                            CustomizationGuiUnattended guiCust = new CustomizationGuiUnattended();
                            guiCust.setAutoLogon(false);
                            guiCust.setAutoLogonCount(0);
                            CustomizationPassword password = new CustomizationPassword();
                            password.setPlainText(true);
                            password.setValue(options.getBootstrapPassword());
                            guiCust.setPassword(password);
                            //log.debug("Windows pass for "+hostName+": "+password.getValue());
                            sysprep.setGuiUnattended(guiCust);

                            CustomizationIdentification identification = new CustomizationIdentification();
                            identification.setJoinWorkgroup(options.getWinWorkgroupName());
                            sysprep.setIdentification(identification);

                            CustomizationUserData userData = new CustomizationUserData();
                            userData.setComputerName(new CustomizationVirtualMachineName());
                            userData.setFullName(options.getWinOwnerName());
                            userData.setOrgName(options.getWinOrgName());
                            userData.setProductId(options.getWinProductSerialNum());
                            sysprep.setUserData(userData);

                            customizationSpec.setIdentity(sysprep);
                        }
                        else {
                            log.error("Guest customisation could not take place as platform is not linux or windows: "+platform);
                            isCustomised=false;
                        }

                        if (isCustomised) {
                            CustomizationGlobalIPSettings globalIPSettings = new CustomizationGlobalIPSettings();
                            globalIPSettings.setDnsServerList(options.getDnsServerList());
                            globalIPSettings.setDnsSuffixList(options.getDnsSuffixList());
                            customizationSpec.setGlobalIPSettings(globalIPSettings);

                            List<CustomizationAdapterMapping> nicMappings = new ArrayList<CustomizationAdapterMapping>();
                            for (int i = 0; i < count; i++) {
                                CustomizationAdapterMapping adapterMap = new CustomizationAdapterMapping();
                                if (i != networkIndex) {
                                    CustomizationIPSettings adapter = new CustomizationIPSettings();
                                    adapter.setDnsDomain(options.getDnsDomain());
                                    adapter.setIp(new CustomizationDhcpIpGenerator());
                                    adapterMap.setAdapter(adapter);
                                }
                                else {
                                    CustomizationIPSettings adapter = new CustomizationIPSettings();
                                    adapter.setDnsDomain(options.getDnsDomain());
                                    adapter.setGateway(options.getGatewayList());
                                    CustomizationFixedIp fixedIp = new CustomizationFixedIp();
                                    fixedIp.setIpAddress(options.getPrivateIp());
                                    log.debug("custom IP: " + options.getPrivateIp());
                                    adapter.setIp(fixedIp);
                                    if (options.getMetaData().containsKey("vSphereNetMaskNothingToSeeHere")) {
                                        String netmask = (String)options.getMetaData().get("vSphereNetMaskNothingToSeeHere");
                                        adapter.setSubnetMask(netmask);
                                        log.debug("custom subnet mask: " + netmask);
                                    } else {
                                        adapter.setSubnetMask("255.255.252.0");
                                        log.debug("default subnet mask");
                                    }
                                    adapterMap.setAdapter(adapter);
                                }
                                nicMappings.add(adapterMap);
                            }
                            CustomizationAdapterMapping[] nicSettingMap = nicMappings.toArray(new CustomizationAdapterMapping[nicMappings.size()]);
                            customizationSpec.setNicSettingMap(nicSettingMap);
                        }
                    }

                    VirtualMachineCloneSpec spec = new VirtualMachineCloneSpec();
                    spec.setLocation(location);
                    spec.setPowerOn(true);
                    spec.setTemplate(false);
                    spec.setConfig(config);
                    if (isCustomised) {
                        spec.setCustomization(customizationSpec);
                    }

                    Task task = template.cloneVM_Task(vmFolder, hostName, spec);

                    String status = task.waitForTask();

                    if( status.equals(Task.SUCCESS) ) {
                        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);

                        while( System.currentTimeMillis() < timeout ) {
                            try { Thread.sleep(10000L); }
                            catch( InterruptedException ignore ) { }

                            for( VirtualMachine s : listVirtualMachines() ) {
                                if( s.getName().equals(hostName) ) {
                                    if (isCustomised && s.getPlatform().equals(Platform.WINDOWS)) {
                                        s.setRootPassword(options.getBootstrapPassword());
                                    }
                                    return s;
                                }
                            }
                        }
                        lastError = new CloudException("Unable to identify newly created server.");
                    }
                    else {
                        lastError = new CloudException("Failed to create VM: " + task.getTaskInfo().getError().getLocalizedMessage());
                    }
                }
                if( lastError != null ) {
                    throw lastError;
                }
                throw new CloudException("No server and no error");
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
            catch( InterruptedException e ) {
                throw new InternalException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull VirtualMachine defineFromScratch(@Nonnull VMLaunchOptions options) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.define");
        try {
            ProviderContext ctx = getProvider().getContext();

            if( ctx == null ) {
                throw new InternalException("No context was set for this request");
            }
            ServiceInstance instance = getServiceInstance();
            try {
                String hostName = validateName(options.getHostName());
                String dataCenterId = options.getDataCenterId();
                String resourceProductStr = options.getStandardProductId();
                String imageId = options.getMachineImageId();
                String[] items = resourceProductStr.split(":");
                if (items.length==3) {
                    options.withResourcePoolId(items[0]);
                }

                if( dataCenterId == null ) {
                    String rid = ctx.getRegionId();

                    if( rid != null ) {
                        for( DataCenter dsdc : getProvider().getDataCenterServices().listDataCenters(rid) ) {
                            dataCenterId = dsdc.getProviderDataCenterId();
                            if( random.nextInt()%3 == 0 ) {
                                break;
                            }
                        }
                    }
                }
                ManagedEntity[] pools = null;

                Datacenter vdc = null;

                if( dataCenterId != null ) {
                    DataCenter ourDC = getProvider().getDataCenterServices().getDataCenter(dataCenterId);
                    if (ourDC != null) {
                        vdc = getProvider().getDataCenterServices().getVmwareDatacenterFromVDCId(instance, ourDC.getRegionId());

                        if( vdc == null ) {
                            throw new CloudException("Unable to identify VDC " + dataCenterId);
                        }

                        if (options.getResourcePoolId() == null) {
                            ResourcePool pool = getProvider().getDataCenterServices().getResourcePoolFromClusterId(instance, dataCenterId);
                            if( pool != null ) {
                                pools = new ManagedEntity[] { pool };
                            }
                        }
                    }
                    else {
                        vdc = getProvider().getDataCenterServices().getVmwareDatacenterFromVDCId(instance, dataCenterId);
                        if (options.getResourcePoolId() == null) {
                            pools = new InventoryNavigator(vdc).searchManagedEntities("ResourcePool");
                        }
                    }
                }

                CloudException lastError = null;

                if (options.getResourcePoolId() != null) {
                    ResourcePool pool = getProvider().getDataCenterServices().getVMWareResourcePool(options.getResourcePoolId());
                    if (pool != null) {
                        pools = new ManagedEntity[] {pool};
                    }
                    else {
                        throw new CloudException("Unable to find resource pool with id "+options.getResourcePoolId());
                    }
                }

                for( ManagedEntity p : pools ) {
                    ResourcePool pool = (ResourcePool)p;
                    Folder vmFolder = vdc.getVmFolder();
                    if (options.getVmFolderId() != null) {
                        ManagedEntity tmp = new InventoryNavigator(vmFolder).searchManagedEntity("Folder", options.getVmFolderId());
                        if (tmp != null) {
                            vmFolder = (Folder)tmp;
                        }
                    }

                    VirtualMachineConfigSpec config = new VirtualMachineConfigSpec();
                    String[] vmInfo = options.getStandardProductId().split(":");
                    int cpuCount;
                    long memory;
                    if (vmInfo.length == 2) {
                        cpuCount = Integer.parseInt(vmInfo[0]);
                        memory = Long.parseLong(vmInfo[1]);
                    }
                    else {
                        cpuCount = Integer.parseInt(vmInfo[1]);
                        memory = Long.parseLong(vmInfo[2]);
                    }

                    config.setName(hostName);
                    config.setAnnotation(imageId);
                    config.setMemoryMB(memory);
                    config.setNumCPUs(cpuCount);
                    config.setGuestId(imageId);

                    // create vm file info for the vmx file
                    VirtualMachineFileInfo vmfi = new VirtualMachineFileInfo();
                    String vmDataStoreName = null;
                    Datastore[] datastores = vdc.getDatastores();
                    for (Datastore ds : datastores) {
                        if (options.getStoragePoolId() != null) {
                            String locationId = options.getStoragePoolId();
                            if (ds.getName().equals(locationId)) {
                                vmDataStoreName = ds.getName();
                                break;
                            }
                        }
                        else {
                            //just pick the first datastore as user doesn't care
                            vmDataStoreName = ds.getName();
                            break;
                        }
                    }
                    if (vmDataStoreName == null) {
                        throw new CloudException("Unable to find a datastore for vm "+hostName);
                    }

                    vmfi.setVmPathName("["+ vmDataStoreName +"]");
                    config.setFiles(vmfi);

                    //networking section
                    //borrowed heavily from https://github.com/jedi4ever/jvspherecontrol
                    String vlan = options.getVlanId();
                    if (vlan != null) {
                        VirtualDeviceConfigSpec nicSpec = new VirtualDeviceConfigSpec();
                        nicSpec.setOperation(VirtualDeviceConfigSpecOperation.add);

                        VirtualEthernetCard nic = new VirtualE1000();
                        nic.setConnectable(new VirtualDeviceConnectInfo());
                        nic.connectable.connected=true;
                        nic.connectable.startConnected=true;

                        Description info = new Description();
                        info.setLabel(vlan);
                        info.setSummary("Nic for network "+vlan);

                        VirtualEthernetCardNetworkBackingInfo nicBacking = new VirtualEthernetCardNetworkBackingInfo();
                        nicBacking.setDeviceName(vlan);

                        nic.setAddressType("generated");
                        nic.setBacking(nicBacking);
                        nic.setKey(0);

                        nicSpec.setDevice(nic);

                        VirtualDeviceConfigSpec[] machineSpecs = new VirtualDeviceConfigSpec[1];
                        machineSpecs[0]=nicSpec;

                        config.setDeviceChange(machineSpecs);
                        // end networking section
                    }
                    else {
                        throw new CloudException("You must choose a network when creating a vm from scratch");
                    }

                    // VirtualMachineRelocateSpec location = new VirtualMachineRelocateSpec();
                    HostSystem host = null;
                    if (options.getAffinityGroupId() != null) {

                        Host agSupport= getProvider().getComputeServices().getAffinityGroupSupport();
                        host = agSupport.getHostSystemForAffinity(options.getAffinityGroupId());
                    }

                    Task task = vmFolder.createVM_Task(config, pool, host);

                    String status = task.waitForTask();

                    if( status.equals(Task.SUCCESS) ) {
                        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);

                        while( System.currentTimeMillis() < timeout ) {
                            try { Thread.sleep(10000L); }
                            catch( InterruptedException ignore ) { }

                            for( VirtualMachine s : listVirtualMachines() ) {
                                if( s.getName().equals(hostName) ) {
                                    return s;
                                }
                            }
                        }
                        lastError = new CloudException("Unable to identify newly created server.");
                    }
                    else {
                        lastError = new CloudException("Failed to create VM: " + task.getTaskInfo().getError().getLocalizedMessage());
                    }
                }
                if( lastError != null ) {
                    throw lastError;
                }
                throw new CloudException("No server and no error");
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
            catch( InterruptedException e ) {
                throw new InternalException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull Architecture getArchitecture(@Nonnull VirtualMachineGuestOsIdentifier os) {
        if( os.name().contains("64") ) {
            return Architecture.I64;
        }
        else {
            return Architecture.I32;
        }
    }

    @Nonnull HostSystem getBestHost(@Nonnull Datacenter forDatacenter, @Nonnull String clusterName) throws CloudException, RemoteException {
        APITrace.begin(getProvider(), "Vm.getBestHost");
        try {
            Collection<HostSystem> possibles = getPossibleHosts(forDatacenter, clusterName);

            if( possibles.isEmpty()) {
                HostSystem ohWell = null;

                for( ManagedEntity me : forDatacenter.getHostFolder().getChildEntity() ) {
                    if (me.getName().equals(clusterName)){
                        ComputeResource cluster = (ComputeResource)me;

                        for( HostSystem host : cluster.getHosts() ) {
                            if( host.getConfigStatus().equals(ManagedEntityStatus.green) ) {
                                return host;
                            }
                            if( ohWell == null || host.getConfigStatus().equals(ManagedEntityStatus.yellow) ) {
                                ohWell = host;
                            }
                        }
                    }
                }
                if( ohWell == null ) {
                    throw new CloudException("Insufficient capacity for this operation");
                }
                return ohWell;
            }

            return possibles.iterator().next();
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull Collection<HostSystem> getPossibleHosts(@Nonnull Datacenter dc, @Nonnull String clusterName) throws CloudException, RemoteException {
        APITrace.begin(getProvider(), "Vm.getPossibleHosts");
        try {
            ArrayList<HostSystem> possibles = new ArrayList<HostSystem>();

            for( ManagedEntity me : dc.getHostFolder().getChildEntity() ) {
                if (me.getName().equals(clusterName)){
                    ComputeResource cluster = (ComputeResource)me;

                    for( HostSystem host : cluster.getHosts() ) {
                        if( host.getConfigStatus().equals(ManagedEntityStatus.green) ) {
                            possibles.add(host);
                        }
                    }
                }
            }
            return possibles;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getConsoleOutput(@Nonnull String serverId) throws InternalException, CloudException {
        return "";
    }

    private @Nullable String getDataCenter(@Nonnull com.vmware.vim25.mo.VirtualMachine vm) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.getDataCenter");
        try {
            try {
                return vm.getResourcePool().getOwner().getName();
            }
            catch( RemoteException e ) {
                throw new CloudException(e);
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<String> listFirewalls(@Nonnull String serverId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    private @Nullable HostSystem getHost(@Nonnull com.vmware.vim25.mo.VirtualMachine vm) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "getHostForVM");
        try {
            String dc = getDataCenter(vm);
            ManagedObjectReference vmHost = vm.getRuntime().getHost();

            Host affinityGroupSupport = getProvider().getComputeServices().getAffinityGroupSupport();
            Iterable<HostSystem> hostSystems = affinityGroupSupport.listHostSystems(dc);
            for (HostSystem host : hostSystems) {
                if (vmHost.getVal().equals(host.getMOR().getVal())) {
                    return host;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable com.vmware.vim25.mo.VirtualMachine getTemplate(@Nonnull ServiceInstance instance, @Nonnull String templateId) throws CloudException, RemoteException, InternalException {
        APITrace.begin(getProvider(), "Vm.getTemplate");
        try {
            Folder folder = getProvider().getVmFolder(instance);
            ManagedEntity[] mes;

            mes = new InventoryNavigator(folder).searchManagedEntities("VirtualMachine");
            if( mes != null && mes.length > 0 ) {
                for( ManagedEntity entity : mes ) {
                    com.vmware.vim25.mo.VirtualMachine template = (com.vmware.vim25.mo.VirtualMachine)entity;

                    if( template != null ) {
                        VirtualMachineConfigInfo vminfo = template.getConfig();

                        if( vminfo != null && vminfo.isTemplate() && vminfo.getUuid().equals(templateId) ) {
                            return template;
                        }
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
    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.getProduct(String)");
        try {
            for( VirtualMachineProduct product : listProducts(null, Architecture.I64) ) {
                if( product.getProviderProductId().equals(productId) ) {
                    return product;
                }
            }
            for( VirtualMachineProduct product : listProducts(null, Architecture.I32) ) {
                if( product.getProviderProductId().equals(productId) ) {
                    return product;
                }
            }

            //Product is non-standard so build a new one
            String[] parts = productId.split(":");
            VirtualMachineProduct product = new VirtualMachineProduct();
            product.setCpuCount(Integer.parseInt(parts[0]));
            product.setRamSize(new Storage<Megabyte>(Integer.parseInt(parts[1]), Storage.MEGABYTE));
            product.setDescription("Custom product " + parts[0] + " CPU, " + parts[1] + " RAM");
            product.setName(parts[0] + " CPU/" + parts[1] + " MB RAM");
            product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
            product.setProviderProductId(parts[0] + ":" + parts[1]);
            return product;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable VirtualMachine getVirtualMachine(@Nonnull String serverId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.getVirtualMachine");
        try {
            ServiceInstance instance = getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);

            if( vm == null ) {
                return null;
            }
            return toServer(vm, null);
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull VirtualMachineProduct getProduct(@Nonnull VirtualHardware hardware) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.getProduct(VirtualHardware)");
        VirtualMachineProduct product = getProduct(hardware.getNumCPU() + ":" + hardware.getMemoryMB());

        if( product == null ) {
            int cpu = hardware.getNumCPU();
            int ram = hardware.getMemoryMB();
            int disk = 1;

            product = new VirtualMachineProduct();
            product.setCpuCount(cpu);
            product.setDescription("Custom product " + cpu + " CPU, " + ram + " RAM");
            product.setName(cpu + " CPU/" + ram + " MB RAM");
            product.setRootVolumeSize(new Storage<Gigabyte>(disk, Storage.GIGABYTE));
            product.setProviderProductId(cpu + ":" + ram);
        }
        return product;
    }

    @Override
    public Iterable<VirtualMachineProduct> listProducts(VirtualMachineProductFilterOptions options, Architecture architecture) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.listProducts(VirtualMachineProductFilterOptions, Architecture)");
        try {
            ArrayList<VirtualMachineProduct> allVirtualMachineProducts = new ArrayList<VirtualMachineProduct>();

            Cache<org.dasein.cloud.dc.ResourcePool> cache = Cache.getInstance(getProvider(), "resourcePools", org.dasein.cloud.dc.ResourcePool.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(15, TimePeriod.MINUTE));
            Collection<org.dasein.cloud.dc.ResourcePool> rps = (Collection<org.dasein.cloud.dc.ResourcePool>)cache.get(getContext());

            if( rps == null ) {
                Collection<DataCenter> dcs = getProvider().getDataCenterServices().listDataCenters(getContext().getRegionId());
                rps = new ArrayList<org.dasein.cloud.dc.ResourcePool>();

                for (DataCenter dc : dcs) {
                    Collection<org.dasein.cloud.dc.ResourcePool> pools = getProvider().getDataCenterServices().listResourcePools(dc.getProviderDataCenterId());
                    rps.addAll(pools);
                }
                cache.put(getContext(),rps);
            }

            if (architecture != null) {
                for( Architecture a : getCapabilities().listSupportedArchitectures() ) {
                    if( a.equals(architecture) ) {
                        if( a.equals(Architecture.I32) ) {
                            for( int cpu : new int[] { 1, 2 } ) {
                                for( int ram : new int[] { 512, 1024, 2048 } ) {
                                    // add in product without pool
                                    VirtualMachineProduct product = new VirtualMachineProduct();

                                    product.setCpuCount(cpu);
                                    product.setDescription("Custom product " + architecture + " - " + cpu + " CPU, " + ram + "MB RAM");
                                    product.setName(cpu + " CPU/" + ram + " MB RAM");
                                    product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
                                    product.setProviderProductId(cpu + ":" + ram);
                                    product.setRamSize(new Storage<Megabyte>(ram, Storage.MEGABYTE));
                                    allVirtualMachineProducts.add(product);

                                    //resource pools
                                    for (org.dasein.cloud.dc.ResourcePool pool : rps) {
                                        product = new VirtualMachineProduct();
                                        product.setCpuCount(cpu);
                                        product.setDescription("Custom product " + architecture + " - " + cpu + " CPU, " + ram + "MB RAM");
                                        product.setName("Pool "+pool.getName()+"/"+cpu + " CPU/" + ram + " MB RAM");
                                        product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
                                        product.setProviderProductId(pool.getProvideResourcePoolId()+":"+cpu + ":" + ram);
                                        product.setRamSize(new Storage<Megabyte>(ram, Storage.MEGABYTE));
                                        allVirtualMachineProducts.add(product);
                                    }
                                }
                            }
                        }
                        else {
                            for( int cpu : new int[] { 1, 2, 4, 8 } ) {
                                for( int ram : new int[] { 1024, 2048, 4096, 10240, 20480 } ) {
                                    // add in product without pool
                                    VirtualMachineProduct product = new VirtualMachineProduct();

                                    product.setCpuCount(cpu);
                                    product.setDescription("Custom product " + architecture + " - " + cpu + " CPU, " + ram + "MB RAM");
                                    product.setName(cpu + " CPU/" + ram + " MB RAM");
                                    product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
                                    product.setProviderProductId(cpu + ":" + ram);
                                    product.setRamSize(new Storage<Megabyte>(ram, Storage.MEGABYTE));
                                    allVirtualMachineProducts.add(product);

                                    //resource pools
                                    for (org.dasein.cloud.dc.ResourcePool pool : rps) {
                                        product = new VirtualMachineProduct();
                                        product.setCpuCount(cpu);
                                        product.setDescription("Custom product " + architecture + " - " + cpu + " CPU, " + ram + "MB RAM");
                                        product.setName("Pool "+pool.getName()+"/"+cpu + " CPU/" + ram + " MB RAM");
                                        product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
                                        product.setProviderProductId(pool.getProvideResourcePoolId()+":"+cpu + ":" + ram);
                                        product.setRamSize(new Storage<Megabyte>(ram, Storage.MEGABYTE));
                                        allVirtualMachineProducts.add(product);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else {
                for( int cpu : new int[] { 1, 2, 4, 8 } ) {
                    for( int ram : new int[] { 512, 1024, 2048, 4096, 10240, 20480 } ) {
                        // add in product without pool
                        VirtualMachineProduct product = new VirtualMachineProduct();

                        product.setCpuCount(cpu);
                        product.setDescription("Custom product " + architecture + " - " + cpu + " CPU, " + ram + "MB RAM");
                        product.setName(cpu + " CPU/" + ram + " MB RAM");
                        product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
                        product.setProviderProductId(cpu + ":" + ram);
                        product.setRamSize(new Storage<Megabyte>(ram, Storage.MEGABYTE));
                        allVirtualMachineProducts.add(product);

                        //resource pools
                        for (org.dasein.cloud.dc.ResourcePool pool : rps) {
                            product = new VirtualMachineProduct();
                            product.setCpuCount(cpu);
                            product.setDescription("Custom product " + architecture + " - " + cpu + " CPU, " + ram + "MB RAM");
                            product.setName("Pool "+pool.getName()+"/"+cpu + " CPU/" + ram + " MB RAM");
                            product.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
                            product.setProviderProductId(pool.getProvideResourcePoolId()+":"+cpu + ":" + ram);
                            product.setRamSize(new Storage<Megabyte>(ram, Storage.MEGABYTE));
                            allVirtualMachineProducts.add(product);
                        }
                    }
                }
            }
            if (options != null) {
                ArrayList<VirtualMachineProduct> filteredProducts = new ArrayList<VirtualMachineProduct>();
                for (VirtualMachineProduct product : allVirtualMachineProducts) {
                    if (options.matches(product)) {
                        filteredProducts.add(product);
                    }
                }
                return filteredProducts;
            }
            else {
                return allVirtualMachineProducts;
            }
        }
        finally {
            APITrace.end();
        }
    }

    static private Collection<Architecture> architectures;

    @Override
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        return getCapabilities().listSupportedArchitectures();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.listVirtualMachineStatus");
        try {
            ServiceInstance instance = getServiceInstance();
            Folder folder = getProvider().getVmFolder(instance);

            ArrayList<ResourceStatus> servers = new ArrayList<ResourceStatus>();
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
                    ResourceStatus server = toStatus((com.vmware.vim25.mo.VirtualMachine)entity);

                    if( server != null ) {
                        servers.add(server);
                    }
                }
            }
            return servers;
        }
        finally {
            APITrace.end();
        }
    }

    @Nullable com.vmware.vim25.mo.VirtualMachine getVirtualMachine(@Nonnull ServiceInstance instance, @Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Vm.getVirtualMachine(ServiceInstance, String)");
        try {
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
                    com.vmware.vim25.mo.VirtualMachine vm = (com.vmware.vim25.mo.VirtualMachine)entity;
                    VirtualMachineConfigInfo cfg = (vm == null ? null : vm.getConfig());
                    if( cfg != null && cfg.getInstanceUuid().equals(vmId) ) {
                        return vm;
                    }
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable Datacenter getVmwareDatacenter(@Nonnull com.vmware.vim25.mo.VirtualMachine vm) throws CloudException {
        ManagedEntity parent = vm.getParent();

        if( parent == null ) {
            parent = vm.getParentVApp();
        }
        while( parent != null ) {
            if( parent instanceof Datacenter ) {
                return ((Datacenter)parent);
            }
            parent = parent.getParent();
        }
        return null;
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Vm.launch");
        try {
            ServiceInstance instance = getServiceInstance();
            VirtualMachine server;
            boolean isOSId = false;
            String imageId = withLaunchOptions.getMachineImageId();
            try {
                VirtualMachineGuestOsIdentifier os = VirtualMachineGuestOsIdentifier.valueOf(imageId);
                isOSId = true;
            }
            catch( IllegalArgumentException e ) {
                 log.debug("Couldn't find a match to os identifier so trying existing templates instead: "+imageId);
            }
            if (!isOSId) {
                try {
                    com.vmware.vim25.mo.VirtualMachine template = getTemplate(instance, imageId);
                    if( template == null ) {
                        throw new CloudException("No such template or guest os identifier: " + imageId);
                    }
                    server = defineFromTemplate(withLaunchOptions);
                }
                catch (RemoteException e) {
                    throw new CloudException(e);
                }
            }
            else {
                server = defineFromScratch(withLaunchOptions);
            }
            return server;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.listVirtualMachines");
        try {
            ServiceInstance instance = getServiceInstance();
            Folder folder = getProvider().getVmFolder(instance);

            ArrayList<VirtualMachine> servers = new ArrayList<VirtualMachine>();
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
                    VirtualMachine server = toServer((com.vmware.vim25.mo.VirtualMachine)entity, null);

                    if( server != null ) {
                        servers.add(server);
                    }
                }
            }
            return servers;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void resume(@Nonnull String serverId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.resume");
        try {
            ServiceInstance instance = getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);

            if( vm != null ) {
                try {
                    vm.powerOnVM_Task(null);
                }
                catch( TaskInProgress e ) {
                    throw new CloudException(e);
                }
                catch( InvalidState e ) {
                    throw new CloudException(e);
                }
                catch( RuntimeFault e ) {
                    throw new InternalException(e);
                }
                catch( RemoteException e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.stop");
        try {
            ServiceInstance instance = getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, vmId);

            if( vm != null ) {
                try {
                    vm.powerOffVM_Task();
                }
                catch( TaskInProgress e ) {
                    throw new CloudException(e);
                }
                catch( InvalidState e ) {
                    throw new CloudException(e);
                }
                catch( RuntimeFault e ) {
                    throw new InternalException(e);
                }
                catch( RemoteException e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void suspend(@Nonnull String serverId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "Vm.suspend");
        try {
            ServiceInstance instance = getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);

            if( vm != null ) {
                try {
                    vm.suspendVM_Task();
                }
                catch( TaskInProgress e ) {
                    throw new CloudException(e);
                }
                catch( InvalidState e ) {
                    throw new CloudException(e);
                }
                catch( RuntimeFault e ) {
                    throw new InternalException(e);
                }
                catch( RemoteException e ) {
                    throw new CloudException(e);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void reboot(@Nonnull String serverId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "Vm.reboot");
        try {
            String id = serverId;
            ServiceInstance instance = getProvider().getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, id);
            if (vm.getRuntime().getPowerState().equals(VirtualMachinePowerState.poweredOn)) {
                try {
                    vm.rebootGuest();
                }

                catch( TaskInProgress e ) {
                    throw new CloudException(e);
                }
                catch( InvalidState e ) {
                    throw new CloudException(e);
                }
                catch( RuntimeFault e ) {
                    throw new InternalException(e);
                }
                catch( RemoteException e ) {
                    throw new CloudException(e);
                }
            }
            else {
                throw new CloudException("Vm must be powered on before rebooting os");
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void powerOnAndOff(@Nonnull String serverId) {
        APITrace.begin(getProvider(), "Vm.powerOnAndOff");
        try {
            try {
                ServiceInstance instance = getProvider().getServiceInstance();

                com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);
                HostSystem host = getHost(vm);

                if( vm != null ) {
                    Task task = vm.powerOffVM_Task();
                    String status = task.waitForTask();

                    if( !status.equals(Task.SUCCESS) ) {
                        System.err.println("Reboot failed: " + status);
                    }
                    else {
                        try { Thread.sleep(15000L); }
                        catch( InterruptedException ignore ) { /* ignore */ }
                        vm = getVirtualMachine(instance, serverId);
                        vm.powerOnVM_Task(host);
                    }
                }

            }
            catch( Throwable t ) {
                t.printStackTrace();
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void terminate(@Nonnull String serverId) throws InternalException, CloudException {
        terminate(serverId, "");
    }

    @Override
    public void terminate(@Nonnull String vmId, String explanation)throws InternalException, CloudException{
        final String id = vmId;

        getProvider().hold();
        Thread t = new Thread() {
            public void run() {
                try { terminateVm(id); }
                finally { getProvider().release(); }
            }
        };

        t.setName("Terminate " + vmId);
        t.setDaemon(true);
        t.start();
    }

    private void terminateVm(@Nonnull String serverId) {
        APITrace.begin(getProvider(), "Vm.terminateVm");
        try {
            try {
                ServiceInstance instance = getServiceInstance();

                com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);

                if( vm != null ) {
                    VirtualMachineRuntimeInfo runtime = vm.getRuntime();

                    if( runtime != null ) {
                        String status = "";

                        VirtualMachinePowerState state = runtime.getPowerState();
                        if(state != VirtualMachinePowerState.poweredOff){
                            Task task = vm.powerOffVM_Task();
                            status = task.waitForTask();
                        }

                        if(!status.equals("") && !status.equals(Task.SUCCESS)) {
                            System.err.println("Termination failed: " + status);
                        }
                        else {
                            try { Thread.sleep(15000L); }
                            catch( InterruptedException ignore ) { /* ignore */ }
                            vm = getVirtualMachine(instance, serverId);
                            if( vm != null ) {
                                vm.destroy_Task();
                            }
                        }
                    }
                }
            }
            catch( Throwable t ) {
                t.printStackTrace();
            }
        }
        finally {
            APITrace.end();
        }
    }

    private boolean isPublicIpAddress(@Nonnull String ipv4Address) {
        if( ipv4Address.startsWith("10.") || ipv4Address.startsWith("192.168") || ipv4Address.startsWith("169.254") ) {
            return false;
        }
        else if( ipv4Address.startsWith("172.") ) {
            String[] parts = ipv4Address.split("\\.");

            if( parts.length != 4 ) {
                return true;
            }
            int x = Integer.parseInt(parts[1]);

            if( x >= 16 && x <= 31 ) {
                return false;
            }
        }
        return true;
    }

    private @Nullable ResourceStatus toStatus(@Nullable com.vmware.vim25.mo.VirtualMachine vm) {
        if( vm == null ) {
            return null;
        }
        VirtualMachineConfigInfo vminfo;

        try {
            vminfo = vm.getConfig();
        }
        catch( RuntimeException e ) {
            return null;
        }
        if( vminfo == null || vminfo.isTemplate() ) {
            return null;
        }
        String id = vminfo.getInstanceUuid();

        if( id == null ) {
            return null;
        }

        VirtualMachineRuntimeInfo runtime = vm.getRuntime();
        VmState vmState = VmState.PENDING;

        if( runtime != null ) {
            VirtualMachinePowerState state = runtime.getPowerState();

            switch( state ) {
                case suspended:
                    vmState = VmState.SUSPENDED;
                    break;
                case poweredOff:
                    vmState = VmState.STOPPED;
                    break;
                case poweredOn:
                    vmState = VmState.RUNNING;
                    break;
                default:
                    System.out.println("DEBUG: Unknown vSphere server state: " + state);
            }
        }
        return new ResourceStatus(id, vmState);
    }

    private @Nullable VirtualMachine toServer(@Nullable com.vmware.vim25.mo.VirtualMachine vm, @Nullable String description) throws InternalException, CloudException {
        if( vm != null ) {
            VirtualMachineConfigInfo vminfo;

            try {
                vminfo = vm.getConfig();
            }
            catch( RuntimeException e ) {
                return null;
            }
            if( vminfo == null || vminfo.isTemplate() ) {
                return null;
            }
            Map<String, String> properties = new HashMap<String, String>();
            VirtualMachineConfigInfoDatastoreUrlPair[] datastoreUrl = vminfo.getDatastoreUrl();
            for (int i=0;i<datastoreUrl.length; i++) {
                properties.put("datastore"+i,datastoreUrl[i].getName());
            }

            ManagedEntity parent = vm.getParent();
            while (parent != null) {
                if (parent instanceof Folder) {
                    properties.put("vmFolder", parent.getName());
                    break;
                }
                parent = parent.getParent();
            }

            VirtualMachineGuestOsIdentifier os = VirtualMachineGuestOsIdentifier.valueOf(vminfo.getGuestId());
            VirtualMachine server = new VirtualMachine();

            HostSystem host = getHost(vm);
            if (host != null) {
                server.setAffinityGroupId(host.getName());
            }

            server.setName(vm.getName());
            server.setPlatform(Platform.guess(vminfo.getGuestFullName()));
            server.setProviderVirtualMachineId(vm.getConfig().getInstanceUuid());
            server.setPersistent(true);
            server.setImagable(true);
            server.setClonable(true);
            server.setArchitecture(getArchitecture(os));
            if( description == null ) {
                description = vm.getName();
            }
            server.setDescription(description);
            server.setProductId(getProduct(vminfo.getHardware()).getProviderProductId());
            String imageId = vminfo.getAnnotation();

            if (imageId != null && imageId.length()>0 && !imageId.contains(" ")) {
                server.setProviderMachineImageId(imageId);
            }
            else {
                server.setProviderMachineImageId(getContext().getAccountNumber() + "-unknown");
            }
            String dc = getDataCenter(vm);

            if( dc == null ) {
                return null;
            }
            DataCenter ourDC = getProvider().getDataCenterServices().getDataCenter(dc);
            if (ourDC != null) {
                server.setProviderDataCenterId(dc);
                server.setProviderRegionId(ourDC.getRegionId());
            }
            else if ( dc.equals(getContext().getRegionId()) ) {
                // env doesn't have clusters?
                server.setProviderDataCenterId(dc+"-a");
                server.setProviderRegionId(dc);
            }
            else {
                return null;
            }

            try {
                ResourcePool rp = vm.getResourcePool();
                if (rp != null) {
                    String id = getProvider().getDataCenterServices().getIdForResourcePool(rp);
                    server.setResourcePoolId(id);
                }
            }
            catch (InvalidProperty ex) {
                throw new CloudException(ex);
            }
            catch (RuntimeFault ex) {
                throw new CloudException(ex);
            }
            catch (RemoteException ex) {
                throw new CloudException(ex);
            }

            GuestInfo guest = vm.getGuest();
            if( guest != null ) {
                if( guest.getHostName() != null ) {
                    server.setPrivateDnsAddress(guest.getHostName());
                }
                if (guest.getIpAddress() != null) {
                    server.setProviderAssignedIpAddressId(guest.getIpAddress());
                }
                GuestNicInfo[] nicInfoArray = guest.getNet();
                if (nicInfoArray != null && nicInfoArray.length>0) {
                    List<RawAddress> pubIps = new ArrayList<RawAddress>();
                    List<RawAddress> privIps = new ArrayList<RawAddress>();
                    for (GuestNicInfo nicInfo : nicInfoArray) {
                        String net = nicInfo.getNetwork();
                        if (net != null) {
                            if (server.getProviderVlanId() == null) {
                                server.setProviderVlanId(net);
                            }
                        }
                        String[] ipAddresses = nicInfo.getIpAddress();
                        if (ipAddresses != null) {
                            for (String ip : ipAddresses) {
                                if( ip != null ) {
                                    if( isPublicIpAddress(ip) ) {
                                        pubIps.add(new RawAddress(ip));
                                    }
                                    else {
                                        privIps.add(new RawAddress(ip));
                                    }
                                }
                            }

                        }
                    }
                    if (privIps != null && privIps.size() > 0) {
                        RawAddress[] rawPriv = privIps.toArray(new RawAddress[privIps.size()]);
                        server.setPrivateAddresses(rawPriv);
                    }
                    if (pubIps != null && pubIps.size() > 0) {
                        RawAddress[] rawPub = pubIps.toArray(new RawAddress[pubIps.size()]);
                        server.setPublicAddresses(rawPub);
                    }
                }
            }



            VirtualMachineRuntimeInfo runtime = vm.getRuntime();

            if( runtime != null ) {
                VirtualMachinePowerState state = runtime.getPowerState();

                if( server.getCurrentState() == null ) {
                    switch( state ) {
                        case suspended:
                            server.setCurrentState(VmState.SUSPENDED);
                            break;
                        case poweredOff:
                            server.setCurrentState(VmState.STOPPED);
                            break;
                        case poweredOn:
                            server.setCurrentState(VmState.RUNNING);
                            server.setRebootable(true);
                            break;
                    }
                }
                Calendar suspend = runtime.getSuspendTime();
                Calendar time = runtime.getBootTime();

                if( suspend == null || suspend.getTimeInMillis() < 1L ) {
                    server.setLastPauseTimestamp(-1L);
                }
                else {
                    server.setLastPauseTimestamp(suspend.getTimeInMillis());
                    server.setCreationTimestamp(server.getLastPauseTimestamp());
                }
                if( time == null || time.getTimeInMillis() < 1L ) {
                    server.setLastBootTimestamp(0L);
                }
                else {
                    server.setLastBootTimestamp(time.getTimeInMillis());
                    server.setCreationTimestamp(server.getLastBootTimestamp());
                }
            }
            server.setProviderOwnerId(getContext().getAccountNumber());
            server.setTags(properties);
            return server;
        }
        return null;
    }

    private String validateName(String name) {
        name = name.toLowerCase().replaceAll("_", "-").replaceAll(" ", "");
        if( name.length() <= 30 ) {
            return name;
        }
        return name.substring(0, 30);
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (getProvider().getServiceInstance() != null);
    }
}
