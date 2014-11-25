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

import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.vsphere.PrivateCloud;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VMwareComputeServices extends AbstractComputeServices {
    private PrivateCloud cloud;
    
    public VMwareComputeServices(@Nonnull PrivateCloud cloud) { this.cloud = cloud; }

    @Override
    public @Nonnull Template getImageSupport() {
        return new Template(cloud);
    }
    
    @Override
    public @Nonnull Vm getVirtualMachineSupport() {
        return new Vm(cloud);
    }

    @Nullable
    @Override
    public HardDisk getVolumeSupport() {
        return new HardDisk(cloud);
    }

    @Override
    public @Nonnull Host getAffinityGroupSupport() {
        return new Host(cloud);
    }
}
