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

package org.dasein.cloud.vsphere.network;

import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.network.VLANSupport;
import org.dasein.cloud.vsphere.PrivateCloud;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VMwareNetworkServices extends AbstractNetworkServices<PrivateCloud> {
    public VMwareNetworkServices(@Nonnull PrivateCloud cloud) { super(cloud); }
    
    public @Nullable StaticIp getIpAddressSupport() {
        return null;
        //return new StaticIp(getProvider());
    }

    @Nullable
    @Override
    public VLANSupport getVlanSupport() {
        return new VSphereNetwork(getProvider());
    }
}
