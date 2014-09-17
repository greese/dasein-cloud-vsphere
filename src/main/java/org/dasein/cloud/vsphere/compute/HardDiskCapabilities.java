package org.dasein.cloud.vsphere.compute;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VolumeCapabilities;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.util.NamingConstraints;
import org.dasein.cloud.vsphere.PrivateCloud;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 03/09/2014
 * Time: 12:32
 */
public class HardDiskCapabilities extends AbstractCapabilities<PrivateCloud> implements VolumeCapabilities {
    public HardDiskCapabilities(@Nonnull PrivateCloud provider) {
        super(provider);
    }

    @Override
    public boolean canAttach(VmState vmState) throws InternalException, CloudException {
        return (vmState.equals(VmState.STOPPED));
    }

    @Override
    public boolean canDetach(VmState vmState) throws InternalException, CloudException {
        return false;
    }

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return 15;
    }

    @Override
    public int getMaximumVolumeProductIOPS() throws InternalException, CloudException {
        return AbstractCapabilities.LIMIT_UNKNOWN;
    }

    @Override
    public int getMinimumVolumeProductIOPS() throws InternalException, CloudException {
        return AbstractCapabilities.LIMIT_UNKNOWN;
    }

    @Override
    public int getMaximumVolumeSizeIOPS() throws InternalException, CloudException {
        return AbstractCapabilities.LIMIT_UNKNOWN;
    }

    @Override
    public int getMinimumVolumeSizeIOPS() throws InternalException, CloudException {
        return AbstractCapabilities.LIMIT_UNKNOWN;
    }

    @Nullable
    @Override
    public Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(2000, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public NamingConstraints getVolumeNamingConstraints() throws CloudException, InternalException {
        return NamingConstraints.getAlphaNumeric(5, 50);
    }

    @Nonnull
    @Override
    public String getProviderTermForVolume(@Nonnull Locale locale) {
        return "hard disk";
    }

    @Nonnull
    @Override
    public Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return Requirement.NONE;
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    @Nonnull
    @Override
    public Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        //device ids are numeric 0-15 excluding 7 in vsphere.
        //They are not defined when you add a disk to a vm, vsphere handles this for you
        List<String> list = new ArrayList<String>();
        for (Integer i=0; i<=15; i++) {
            if (i != 7) {
                //0-6, 8-15
                list.add(i.toString());
            }
        }
        return list;
    }

    @Nonnull
    @Override
    public Iterable<VolumeFormat> listSupportedFormats() throws InternalException, CloudException {
        return Collections.singletonList(VolumeFormat.BLOCK);
    }

    @Nonnull
    @Override
    public Requirement requiresVMOnCreate() throws InternalException, CloudException {
        return Requirement.REQUIRED;
    }
}
