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
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Random;

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.vsphere.PrivateCloud;

import com.vmware.vim25.GuestInfo;
import com.vmware.vim25.InvalidProperty;
import com.vmware.vim25.InvalidState;
import com.vmware.vim25.ManagedEntityStatus;
import com.vmware.vim25.RuntimeFault;
import com.vmware.vim25.TaskInProgress;
import com.vmware.vim25.VirtualHardware;
import com.vmware.vim25.VirtualMachineCloneSpec;
import com.vmware.vim25.VirtualMachineConfigInfo;
import com.vmware.vim25.VirtualMachineConfigSpec;
import com.vmware.vim25.VirtualMachineGuestOsIdentifier;
import com.vmware.vim25.VirtualMachinePowerState;
import com.vmware.vim25.VirtualMachineRelocateSpec;
import com.vmware.vim25.VirtualMachineRuntimeInfo;
import com.vmware.vim25.mo.ComputeResource;
import com.vmware.vim25.mo.Datacenter;
import com.vmware.vim25.mo.Folder;
import com.vmware.vim25.mo.HostSystem;
import com.vmware.vim25.mo.InventoryNavigator;
import com.vmware.vim25.mo.ManagedEntity;
import com.vmware.vim25.mo.ResourcePool;
import com.vmware.vim25.mo.ServiceInstance;
import com.vmware.vim25.mo.Task;
import org.dasein.util.CalendarWrapper;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;

public class Vm implements VirtualMachineSupport {

    private PrivateCloud provider;
    
    Vm(@Nonnull PrivateCloud provider) { this.provider = provider;  }

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
    public void boot(@Nonnull String serverId) throws InternalException, CloudException {
        ServiceInstance instance = getServiceInstance();
        
        com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);

        if( vm != null ) {
            try {
                Datacenter dc = getVmwareDatacenter(vm);

                if( dc == null ) {
                    throw new CloudException("Could not identify a deployment data center.");
                }
                vm.powerOnVM_Task(getBestHost(dc));
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

    @Nonnull com.vmware.vim25.mo.VirtualMachine clone(@Nonnull ServiceInstance service, @Nonnull com.vmware.vim25.mo.VirtualMachine vm, @Nonnull String name,  boolean asTemplate) throws InternalException, CloudException {
        try {
            String dcId = getDataCenter(vm);

            if( dcId == null ) {
                throw new CloudException("Virtual machine " + vm + " has no data center parent");
            }
            name = validateName(name);
            
            Datacenter dc = provider.getDataCenterServices().getVmwareDatacenter(service, dcId);

            if( dc == null ) {
                throw new CloudException("Invalid DC for cloning operation: " + dcId);
            }
            Folder vmFolder = dc.getVmFolder();
            
            VirtualMachineConfigSpec config = new VirtualMachineConfigSpec();
            VirtualMachineProduct product = getProduct(vm.getConfig().getHardware());
            String[] sizeInfo = product.getProductId().split(":");
            int cpuCount = Integer.parseInt(sizeInfo[0]);
            long memory = Long.parseLong(sizeInfo[1]);
            
            config.setName(name);
            config.setAnnotation(vm.getConfig().getAnnotation());
            config.setMemoryMB(memory);
            config.setNumCPUs(cpuCount);

            VirtualMachineCloneSpec spec = new VirtualMachineCloneSpec();
            VirtualMachineRelocateSpec location = new VirtualMachineRelocateSpec();
            ResourcePool pool = (ResourcePool) new InventoryNavigator(dc).searchManagedEntities("ResourcePool")[0];
            
            location.setHost(getBestHost(dc).getConfig().getHost());
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
    
    @Override
    public @Nonnull VirtualMachine clone(@Nonnull String serverId, @Nullable String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String ... firewallIds) throws InternalException, CloudException {
        ServiceInstance service = getServiceInstance();
        
        com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(service, serverId);

        if( vm != null ) {
            VirtualMachine target = toServer(clone(service, vm, name, false), description);

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
                boot(id);
            }
            return target;
        }
        throw new CloudException("No virtual machine " + serverId + ".");
    }

    private Random random = new Random();
    
    private @Nonnull VirtualMachine define(@Nonnull String templateId, @Nonnull VirtualMachineProduct size, @Nullable String dataCenterId, @Nonnull String name) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new InternalException("No context was set for this request");
        }
        ServiceInstance service = getServiceInstance();
        try {
            com.vmware.vim25.mo.VirtualMachine template = getTemplate(service, templateId);

            if( template == null ) {
                throw new CloudException("No such template: " + templateId);
            }
            name = validateName(name);

            Datacenter dc = null;

            if( dataCenterId == null ) {
                String rid = ctx.getRegionId();

                if( rid != null ) {
                    for( DataCenter dsdc : provider.getDataCenterServices().listDataCenters(rid) ) {
                        dataCenterId = dsdc.getProviderDataCenterId();
                        if( random.nextInt()%3 == 0 ) {
                            break;
                        }
                    }
                }
            }
            if( dataCenterId != null ) {
                dc = provider.getDataCenterServices().getVmwareDatacenter(service, dataCenterId);
            }
            if( dc == null ) {
                throw new CloudException("Could not identify a valid data center (" + dataCenterId + " attempted)");
            }
            ManagedEntity[] pools = new InventoryNavigator(dc).searchManagedEntities("ResourcePool");


            //Collection<HostSystem> possibleHosts = getPossibleHosts(dc);
            CloudException lastError = null;

            for( ManagedEntity p : pools ) {
                ResourcePool pool = (ResourcePool)p;
            //for( HostSystem sys : possibleHosts ) {
                Folder vmFolder = dc.getVmFolder();

                VirtualMachineConfigSpec config = new VirtualMachineConfigSpec();
                String[] vmInfo = size.getProductId().split(":");
                int cpuCount = Integer.parseInt(vmInfo[0]);
                long memory = Long.parseLong(vmInfo[1]);

                config.setName(name);
                config.setAnnotation(templateId);
                config.setMemoryMB(memory);
                config.setNumCPUs(cpuCount);

                VirtualMachineCloneSpec spec = new VirtualMachineCloneSpec();
                VirtualMachineRelocateSpec location = new VirtualMachineRelocateSpec();


                //location.setHost(sys.getConfig().getHost());
                location.setPool(pool.getConfig().getEntity());

                spec.setLocation(location);
                spec.setPowerOn(false);
                spec.setTemplate(false);
                spec.setConfig(config);

                Task task = template.cloneVM_Task(vmFolder, name, spec);

                String status = task.waitForTask();

                if( status.equals(Task.SUCCESS) ) {
                    long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);

                    while( System.currentTimeMillis() < timeout ) {
                        try { Thread.sleep(10000L); }
                        catch( InterruptedException ignore ) { }

                        for( VirtualMachine s : listVirtualMachines() ) {
                            if( s.getName().equals(name) ) {
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

    @Nonnull Architecture getArchitecture(@Nonnull VirtualMachineGuestOsIdentifier os) {
        if( os.name().contains("64") ) {
            return Architecture.I64;
        }
        else {
            return Architecture.I32;
        }
    }
    
    @Nonnull HostSystem getBestHost(@Nonnull Datacenter forDatacenter) throws CloudException, RemoteException {
        Collection<HostSystem> possibles = getPossibleHosts(forDatacenter);

        if( possibles.isEmpty()) {
            HostSystem ohWell = null;

            for( ManagedEntity me : forDatacenter.getHostFolder().getChildEntity() ) {
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
            if( ohWell == null ) {
                throw new CloudException("Insufficient capacity for this operation");
            }
            return ohWell;
        }
        return possibles.iterator().next();
    }

    /*
    private Collection<ClusterComputeResource> getClusters(@Nonnull Datacenter dc) throws CloudException, InternalException {
        ArrayList<ClusterComputeResource> list = new ArrayList<ClusterComputeResource>();

        try {
            for( ManagedEntity me : dc.getHostFolder().getChildEntity() ) {
                ClusterComputeResource cluster = (ClusterComputeResource)me;

                if( !cluster.getConfigStatus().equals(ManagedEntityStatus.red) ) {
                    list.add(cluster);
                }
            }
        }
        catch( RemoteException e ) {
            throw new CloudException(e);
        }
        return list;
    }
    */

    @Nonnull Collection<HostSystem> getPossibleHosts(@Nonnull Datacenter dc) throws CloudException, RemoteException {
        ArrayList<HostSystem> possibles = new ArrayList<HostSystem>();

        for( ManagedEntity me : dc.getHostFolder().getChildEntity() ) {
            ComputeResource cluster = (ComputeResource)me;

            for( HostSystem host : cluster.getHosts() ) {
                if( host.getConfigStatus().equals(ManagedEntityStatus.green) ) {
                    possibles.add(host);
                }
            }
        }
        return possibles;
    }

    @Override
    public @Nonnull String getConsoleOutput(@Nonnull String serverId) throws InternalException, CloudException {
        return "";
    }

    private @Nullable String getDataCenter(@Nonnull com.vmware.vim25.mo.VirtualMachine vm) throws InternalException, CloudException {
        ManagedEntity parent = vm.getParent();

        if( parent == null ) {
            parent = vm.getParentVApp();
        }
        while( parent != null ) {
            if( parent instanceof Datacenter ) {
                return parent.getName();
            }
            parent = parent.getParent();
        }
        return null;
    }
    
    @Override
    public @Nonnull Collection<String> listFirewalls(@Nonnull String serverId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    private @Nullable HostSystem getHost(@Nonnull com.vmware.vim25.mo.VirtualMachine vm) throws InternalException, CloudException {
        ManagedEntity parent = vm.getParent();

        while( parent != null ) {
            if( parent instanceof HostSystem ) {
                return ((HostSystem)parent);
            }
            parent = parent.getParent();
        }
        return null;
    }
    
    private @Nullable com.vmware.vim25.mo.VirtualMachine getTemplate(@Nonnull ServiceInstance service, @Nonnull String templateId) throws RemoteException {
        Folder rootFolder = service.getRootFolder();
        ManagedEntity[] mes;
        
        mes = new InventoryNavigator(rootFolder).searchManagedEntities("VirtualMachine");
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
    
    @Override 
    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        for( VirtualMachineProduct product : listProducts(Architecture.I64) ) {
            if( product.getProductId().equals(productId) ) {
                return product;
            }
        }
        for( VirtualMachineProduct product : listProducts(Architecture.I32) ) {
            if( product.getProductId().equals(productId) ) {
                return product;
            }
        }
        return null;
    }
    
    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "server";
    }

    @Override
    public @Nullable VirtualMachine getVirtualMachine(@Nonnull String serverId) throws InternalException, CloudException {
        ServiceInstance instance = getServiceInstance();

        com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);
            
        if( vm == null ) {
            return null;
        }
        return toServer(vm, null);
    }

    @Override
    public @Nullable VmStatistics getVMStatistics(@Nonnull String serverId, long start, long end) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Collection<VmStatistics> getVMStatisticsForPeriod(@Nonnull String serverId, long start, long end) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    private @Nonnull VirtualMachineProduct getProduct(@Nonnull VirtualHardware hardware) throws InternalException, CloudException {
        VirtualMachineProduct product = getProduct(hardware.getNumCPU() + ":" + hardware.getMemoryMB());
        
        if( product == null ) {
            int cpu = hardware.getNumCPU();
            int ram = hardware.getMemoryMB();
            int disk = 1;

            product = new VirtualMachineProduct();
            product.setCpuCount(cpu);
            product.setDescription("Custom product " + cpu + " CPU, " + ram + " RAM");
            product.setName(cpu + " CPU/" + ram + " MB RAM");
            product.setDiskSizeInGb(disk);
            product.setProductId(cpu + ":" + ram);
        }
        return product;
    }
    
    @Override
    public @Nonnull Collection<VirtualMachineProduct> listProducts(@Nonnull Architecture architecture) throws InternalException, CloudException {
        ArrayList<VirtualMachineProduct> sizes = new ArrayList<VirtualMachineProduct>();
        
        switch( architecture ) {
            case I32:
                for( int cpu : new int[] { 1, 2 } ) {
                    for( int ram : new int[] { 512, 1024, 2048 } ) {
                        VirtualMachineProduct product = new VirtualMachineProduct();
                        
                        product.setCpuCount(cpu);
                        product.setDescription("Custom product " + cpu + " CPU, " + ram + "GB RAM");
                        product.setName(cpu + " CPU/" + ram + " GB RAM");
                        product.setDiskSizeInGb(1);
                        product.setProductId(cpu + ":" + ram);
                        product.setRamInMb(ram);
                        sizes.add(product);
                    }
                }
                break;
            case I64:
                for( int cpu : new int[] { 1, 2, 4, 8 } ) {
                    for( int ram : new int[] { 1024, 2048, 4096, 10240, 20480 } ) {
                        VirtualMachineProduct product = new VirtualMachineProduct();
                        
                        product.setCpuCount(cpu);
                        product.setDescription("Custom product " + cpu + " CPU, " + ram + "GB RAM");
                        product.setName(cpu + " CPU/" + ram + " GB RAM");
                        product.setDiskSizeInGb(1);
                        product.setProductId(cpu + ":" + ram);
                        product.setRamInMb(ram * 1024);
                        sizes.add(product);
                    }
                }                
                break;
        }
        return sizes;
    }

    @Nullable com.vmware.vim25.mo.VirtualMachine getVirtualMachine(@Nonnull ServiceInstance instance, @Nonnull String vmId) throws CloudException, InternalException {
        Folder rootFolder = instance.getRootFolder();
        ManagedEntity[] mes;
        
        try {
            mes = new InventoryNavigator(rootFolder).searchManagedEntities("VirtualMachine");
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

                if( vm != null && vm.getConfig().getInstanceUuid().equals(vmId) ) {
                    return vm;
                }
            }
        }
        return null;
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
        System.out.println("Failed to find VMware DC for " + vm + " in " + vm.getParent() + " / " + vm.getParentVApp());
        return null;
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String templateId, @Nonnull VirtualMachineProduct product, @Nullable String dataCenterId, @Nonnull String serverName, @Nonnull String description, @Nullable String withKey, @Nullable String inVlanId, boolean withMonitoring, boolean forImaging, @Nullable String... firewalls) throws InternalException, CloudException {
        return launch(templateId, product, dataCenterId, serverName, description, withKey, inVlanId, withMonitoring, forImaging, firewalls, new Tag[0]);
    }
    
    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String templateId, @Nonnull VirtualMachineProduct product, @Nullable String dataCenterId, @Nonnull String serverName, @Nonnull String description, String withKeypairId, String inVlanId, boolean withMonitoring, boolean asSandbox, @Nullable String[] firewalls, @Nullable Tag ... tags) throws InternalException, CloudException {        
        VirtualMachine server = define(templateId, product, dataCenterId, serverName);
        
        boot(server.getProviderVirtualMachineId());
        return server;
    }

    @Override
    public @Nonnull Collection<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        ServiceInstance instance = getServiceInstance();
        
        ArrayList<VirtualMachine> servers = new ArrayList<VirtualMachine>();
        Folder rootFolder = instance.getRootFolder();
        ManagedEntity[] mes;

        try {
            mes = new InventoryNavigator(rootFolder).searchManagedEntities("VirtualMachine");
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

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public void enableAnalytics(@Nonnull String serverId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public void pause(@Nonnull String serverId) throws InternalException, CloudException {
        ServiceInstance instance = getServiceInstance();

        com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(instance, serverId);

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

    @Override
    public void reboot(@Nonnull String serverId) throws CloudException, InternalException {
        final String id = serverId;
        
        provider.hold();
        Thread t = new Thread() {
            public void run() {
                try {
                    powerOnAndOff(id);
                }
                finally {
                    provider.release();
                }
            }
        };
        
        t.setName("Reboot " + serverId);
        t.setDaemon(true);
        t.start();
    }
    
    private void powerOnAndOff(@Nonnull String serverId) {
        try {
            ServiceInstance service = provider.getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(service, serverId);
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
                    vm = getVirtualMachine(service, serverId);
                    vm.powerOnVM_Task(host);
                }
            }

        }
        catch( Throwable t ) {
            t.printStackTrace();
        }
    }

    @Override
    public void terminate(@Nonnull String serverId) throws InternalException, CloudException {
        final String id = serverId;

        provider.hold();
        Thread t = new Thread() {
            public void run() {
                try { terminateVm(id); }
                finally { provider.release(); }
            }
        };
        
        t.setName("Terminate " + serverId);
        t.setDaemon(true);
        t.start();
    }

    private void terminateVm(@Nonnull String serverId) {
        try {
            ServiceInstance service = getServiceInstance();

            com.vmware.vim25.mo.VirtualMachine vm = getVirtualMachine(service, serverId);

            if( vm != null ) {
                Task task = vm.powerOffVM_Task();
                String status = task.waitForTask();

                if( !status.equals(Task.SUCCESS) ) {
                    System.err.println("Termination failed: " + status);
                }
                else {
                    try { Thread.sleep(15000L); }
                    catch( InterruptedException ignore ) { /* ignore */ }
                    vm = getVirtualMachine(service, serverId);
                    if( vm != null ) {
                        vm.destroy_Task();
                    }
                }
            }
        }
        catch( Throwable t ) {
            t.printStackTrace();
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
    
    private @Nullable VirtualMachine toServer(@Nullable com.vmware.vim25.mo.VirtualMachine vm, @Nullable String description) throws InternalException, CloudException {
        if( vm != null ) {
            VirtualMachineConfigInfo vminfo = null;

            try {
                vminfo = vm.getConfig();
            }
            catch( RuntimeException e ) {
                return null;
            }
            if( vminfo == null || vminfo.isTemplate() ) {
                return null;
            }
            VirtualMachineGuestOsIdentifier os = VirtualMachineGuestOsIdentifier.valueOf(vminfo.getGuestId());
            VirtualMachine server = new VirtualMachine();
            GuestInfo guest = vm.getGuest();
            String addr = guest.getIpAddress();

            server.setPrivateIpAddresses(new String[0]);
            server.setPublicIpAddresses(new String[0]);
            if( addr != null ) {
                if( isPublicIpAddress(addr) ) {
                    server.setPublicIpAddresses(new String[] { addr });
                }
                else {
                    server.setPrivateIpAddresses(new String[] { addr });
                }
            }
            if( guest.getHostName() != null ) {
                server.setPrivateDnsAddress(guest.getHostName());
            }
            server.setName(vm.getName());
            server.setPlatform(Platform.guess(vminfo.getGuestFullName()));
            server.setProviderVirtualMachineId(vm.getConfig().getInstanceUuid());
            server.setPersistent(true);
            server.setArchitecture(getArchitecture(os));
            if( description == null ) {
                description = vm.getName();
            }
            server.setDescription(description);
            server.setProduct(getProduct(vminfo.getHardware()));
            String imageId = vminfo.getAnnotation();
            MachineImage img = null;
            

            if( imageId != null ) {
                img = provider.getComputeServices().getImageSupport().getMachineImage(imageId);
            }
            if( img != null ) {
                server.setProviderMachineImageId(vminfo.getAnnotation());
            }
            else {  
                server.setProviderMachineImageId(getContext().getAccountNumber() + "-unknown");
            }
            server.setProviderRegionId(getContext().getRegionId());
            String dc = getDataCenter(vm);

            if( dc == null ) {
                return null;
            }
            server.setProviderDataCenterId(dc);

            GuestInfo guestInfo = vm.getGuest();
            
            if( guestInfo != null ) {
                String ipAddress = guestInfo.getIpAddress();
                
                if( ipAddress != null ) {
                    server.setPrivateIpAddresses(new String[] { guestInfo.getIpAddress() });
                    server.setPrivateDnsAddress(guestInfo.getIpAddress());
                }
            }

            VirtualMachineRuntimeInfo runtime = vm.getRuntime();
            
            if( runtime != null ) {
                VirtualMachinePowerState state = runtime.getPowerState();
                
                if( server.getCurrentState() == null ) {
                    switch( state ) {
                        case poweredOff: case suspended:
                            server.setCurrentState(VmState.PAUSED);
                            break;
                        case poweredOn:
                            server.setCurrentState(VmState.RUNNING);
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
            return server;
        }
        return null;
    }
    
    @Override
    public void disableAnalytics(String serverId) throws InternalException, CloudException {
        // NO-OP
    }

    private String validateName(String name) {
        name = name.toLowerCase().replaceAll("_", "-");
        if( name.length() <= 30 ) {
            return name;
        }
        return name.substring(0, 30);
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (provider.getServiceInstance() != null);
    }

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return false;
    }
}
