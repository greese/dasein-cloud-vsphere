package org.dasein.cloud.vsphere;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.dc.DataCenterCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 04/07/2014
 * Time: 16:30
 */
public class DCCapabilities extends AbstractCapabilities<PrivateCloud> implements DataCenterCapabilities {
    public DCCapabilities(@Nonnull PrivateCloud provider) {
        super(provider);
    }
    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "cluster";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "datacenter";
    }

    @Override
    public boolean supportsAffinityGroups() {
        return true;
    }

    @Override
    public boolean supportsResourcePools() {
        return true;
    }
}
