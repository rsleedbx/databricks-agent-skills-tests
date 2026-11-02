# Disk Setup for Podman and Native Databases

## For Podman: XFS data disk at `/var/lib/containers`

**Every Podman VM puts its data on an XFS data disk**, mounted at `/var/lib/containers`, before Podman is installed. That's where rootful Podman keeps images, containers, and named volumes (the database files), so all of it lands on XFS. Format with `ftype=1` (required by the overlay driver) and `reflink=1` (cheap copies).

Find the disk by its **stable path**, never by `/dev/sdb`. On Azure, `/dev/sdb` is usually the VM's temporary resource disk (on sizes like `Standard_B4ms`), which is wiped when the VM is deallocated; device letters can also change between boots.

| Cloud | Data disk path |
|---|---|
| Azure, disk at LUN 0 (`az vm disk attach --lun 0` or `--data-disk-sizes-gb`) | `/dev/disk/azure/scsi1/lun0` |
| Azure, NVMe sizes (v6 and later) | `/dev/disk/azure/data/by-lun/0` |
| GCP, created with `device-name=data` | `/dev/disk/by-id/google-data` |

Run [`scripts/disk-setup.sh`](../scripts/disk-setup.sh) from the laptop right after the VM is created. It's safe to rerun: it formats only a blank disk, keeps a disk that's already XFS, and stops on any other filesystem rather than wiping it.

```bash
# Azure: USER_HOST=azureuser@$AZURE_PUBLIC_IP DISK=/dev/disk/azure/scsi1/lun0
# GCP:   USER_HOST=ubuntu@$GCP_PUBLIC_IP     DISK=/dev/disk/by-id/google-data
ssh -i ~/.ssh/lfc_test_rsa "$USER_HOST" DISK="$DISK" MNT=/var/lib/containers 'bash -s' < skills/lfc-source-cloud-vm/scripts/disk-setup.sh
```

If `/var/lib/containers` already has content on the OS disk (Podman was installed first), stop Podman, move it aside (`sudo mv /var/lib/containers /var/lib/containers.old`), then run the script, which mounts the XFS disk in its place.

After installing Podman, confirm it's using that disk:

```bash
ssh -i ~/.ssh/lfc_test_rsa "$USER_HOST" \
  'sudo podman info --format "{{.Store.GraphRoot}} {{.Store.GraphDriverName}}"; findmnt -no FSTYPE --target /var/lib/containers/storage'
# /var/lib/containers/storage overlay
# xfs
```

Always run Podman with `sudo`. Rootless Podman stores everything under `~/.local/share/containers`, on the OS disk, not on XFS.

### Logical sector size: 4096 or 512

A data disk's logical sector size decides the smallest direct-I/O write it accepts. [Azure Premium SSD v2](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-deploy-premium-v2) (and Ultra Disk) is 4096 by default: "Premium SSD v2 disks support a 4k physical sector size by default, but you can configure them to use a 512E sector size." The OS disk and Premium SSD (v1) are 512. Check: `sudo blockdev --getss /dev/disk/azure/scsi1/lun0` (or `cat /sys/block/<dev>/queue/logical_block_size`).

| Engine | On a 4096-byte-sector disk |
|---|---|
| Db2 11.5 | `CREATE DATABASE` fails: `SQL0293N Error accessing a table space container.` `db2diag.log` shows `pwrite` of 512 bytes, `OSERR: EINVAL (22)`, `DIO/CIO Mode = Yes`. Set `db2set DB2_4K_DEVICE_SUPPORT=ON`, restart the instance (`db2stop force; db2start`), then create the database. `slot.sh` and `tpc-master.sh` do this (Db2 starts without `DBNAME`, the setting stays in the instance home, so clones keep it) |
| Postgres, MySQL, MariaDB, SQL Server 2022, Oracle 23ai Free, Informix 14.10, CockroachDB, YugabyteDB | no change: each built an SF 1 (Oracle, Informix: SF 0.1) TPC master on a 4096-byte-sector disk |

To get 512 instead (only a new disk; an existing disk's sector size can't change): `az disk create ... --sku PremiumV2_LRS --logical-sector-size 512`. An earlier test VM's Premium SSD v2 data disk was created with 512, and Db2 made its database there without the setting.

## For Native Databases: Use OS Disk

Native MySQL, PostgreSQL, and SQL Server packages use the OS disk for database files. Ensure `/` has at least 50 GB free:

```bash
df -h /
```

The 64 GB OS disk allocated during VM creation should be sufficient for test databases.

## Advanced: Separate data volume for native databases

To keep a native database's files off the OS disk, use the same block with `MNT` set to the database's data directory (for example `MNT=/var/lib/mysql`), **before** installing the database package. Then set ownership after install (`sudo chown mysql:mysql /var/lib/mysql && sudo chmod 750 /var/lib/mysql`).

## Replace the data disk

Tested: the old disk detached and kept, a blank one attached at the same LUN, and the masters brought back from object storage (see [juicefs-branches.md](juicefs-branches.md) and [master-snapshots.md](master-snapshots.md)).

1. On the VM: stop every container, `systemctl stop podman.socket podman.service`, save what's only on the disk (JuiceFS metadata, see juicefs-branches.md), then `umount /var/lib/containers`.
2. Create the new disk: `az disk create -g <rg> -n <name> -l <region> --zone <the VM's zone> --sku PremiumV2_LRS --size-gb <gb> --disk-iops-read-write 3000 --disk-mbps-read-write 125 --tags purpose=lfc-test` (on a VM without a zone: `--sku Premium_LRS`, and mind its size's IOPS, [disk-io-limits.md](disk-io-limits.md)). Attach a v2 disk with caching `None`.
3. Detach the old disk, then attach the new one at the same LUN, so `/dev/disk/azure/scsi1/lun0` still points at it. If `az vm disk detach` or `attach` fails with `InvalidApiVersionParameter` (an az CLI bug seen in 2026-10), patch the VM with `az rest --method patch --url "https://management.azure.com<vm-id>?api-version=2025-04-01"`: first a body with `"dataDisks": []`, then one with `{"lun": 0, "createOption": "Attach", "managedDisk": {"id": "<disk-id>"}}`. Wait until `az disk show ... --query diskState` says `Unattached` before attaching, or the attach fails with `AttachDiskWhileBeingDetached`.
4. Run `disk-setup.sh` again. It comments out the old disk's `/etc/fstab` line, which `mount` would otherwise pick first and fail on, adds the new one, and mounts it.

Delete the old disk only once everything is back.
