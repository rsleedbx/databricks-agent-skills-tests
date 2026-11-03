---
name: lfc-source-eventhubs
description: "Provision an Azure Event Hubs namespace as a Kafka source for a test: its Kafka endpoint, one event hub as the topic, a Listen+Send rule whose connection string goes into a 0600 file, and the source contract (engine kafka) for the tests to read as SOURCE_JSON; then delete only the namespace it made. Use when a Kafka test needs a broker on Azure."
---

# Azure Event Hubs as a Kafka source

[`scripts/eventhubs.sh`](scripts/eventhubs.sh) makes the broker and hands it to a test through the source contract
(**lfc-source-contract**, engine `kafka`). Run it with no arguments for its usage.

| Command | Does | Changes anything |
|---|---|---|
| `eventhubs.sh up <resource group> <namespace> <region> --password-file F [--topic T] [--partitions 32] [--tu 2] [--max-tu 20] [--tags "..."] [--source-json F] [--apply]` | A Standard namespace with the Kafka endpoint on and auto-inflate from `--tu` to `--max-tu`, tagged `CreatedBy=lfc-source-eventhubs` (plus `--tags`); one event hub as the topic (24 h retention); a Listen+Send rule `kafka-test`. Its connection string goes into the 0600 password file; the contract (bootstrap `<namespace>.servicebus.windows.net:9093`, user `$ConnectionString`, the topic, the password file's path, no secret) into `--source-json`, default `<namespace>.source.json` beside the password file | Only with `--apply` |
| `eventhubs.sh down <resource group> <namespace> [--apply]` | Deletes the namespace, its hubs and rules, only when its `CreatedBy` tag says this script made it; never the group | Only with `--apply` |
| [`scripts/test-eventhubs.sh`](scripts/test-eventhubs.sh) | Both against a fake `az`: the group is never created, re-tagged or deleted; only its own namespace is; the contract is valid and holds no secret | No |

## Rules it keeps

- **The resource group is yours and exists.** A subscription allows only so many groups, so `up` never creates one:
  it stops when the group is missing. `az group create` on an existing group replaces its tags, so it's never called.
- **It deletes only what it made.** A namespace of that name without its `CreatedBy` tag stops `up` and is left alone
  by `down`. `down` also accepts the tag `lfc-kafka-freshness-test`, which this script wrote before it moved here.
- **A failed lookup isn't "nothing there".** `down` exits 0 only on `ResourceNotFound`; an expired login, or a group
  not in that subscription (`ResourceGroupNotFound`), exits 1 with the error, nothing deleted.
- **The cloud identity is explicit.** `AZ_SUBSCRIPTION`, when set, goes on every `az` call.
- **No secret is printed or in the contract.** The connection string stays in the 0600 file the contract points to.

## Sizing

About 1 throughput unit per 1,000 events/s or 1 MB/s of ingress: `--tu 4 --max-tu 40` carried a 30,000 events/s burst;
the default 2-20 is short of it. Partitions spread the load across consumers (`--partitions`, 32 by default).

## Use

The subscription, region, resource group and owner come from the saved settings (**lfc-test-settings**), asked once:

```bash
eval "$(bash skills/lfc-test-settings/scripts/settings.sh env)"   # AZ_SUBSCRIPTION (on every az call), AZ_REGION, AZ_RESOURCE_GROUP, OWNER
bash skills/lfc-test-settings/scripts/settings.sh require AZ_SUBSCRIPTION AZ_REGION AZ_RESOURCE_GROUP OWNER   # what to ask, if anything
S=skills/lfc-source-eventhubs/scripts
bash $S/eventhubs.sh up "$AZ_RESOURCE_GROUP" <namespace> "$AZ_REGION" --password-file ~/.lfc/kafka/<namespace>.pw \
  --tu 4 --max-tu 40 --tags "Owner=$OWNER RemoveAfter=<date>" --apply
export SOURCE_JSON=~/.lfc/kafka/<namespace>.source.json        # what a Kafka test reads
bash $S/eventhubs.sh down "$AZ_RESOURCE_GROUP" <namespace> --apply  # when done
```

From the laptop, check the endpoint first: `nc -z -w 5 <namespace>.servicebus.windows.net 9093`.

## Tested

2026-11-03 on Azure (centralus), with **lfc-kafka-freshness-test**: `up` into an existing group (its tags unchanged),
a 15-minute run at 3,000 events/s with a 30,000/s burst, then `down`. The contract file was added after that run and is
checked by `test-eventhubs.sh`.
