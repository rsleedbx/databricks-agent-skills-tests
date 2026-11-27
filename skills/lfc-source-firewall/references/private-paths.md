# Private paths (alternatives to allowlisting)

## Private paths instead of allowlisting (not tested yet)

Two Databricks features let serverless reach a source without the published ranges in its firewall. Both are set up by an **account admin** on an NCC (network connectivity configuration) attached to the workspace, so `ready.py` can't see them as a workspace admin. Ask the user whether the workspace has either.

| | Private network gateway | PrivateLink to a load balancer |
|---|---|---|
| Clouds | Azure: Public Preview (25 regions, centralus included). AWS: Private Preview, through the account team. GCP: in development (2026-10) | AWS (Enterprise plan) |
| What you set up | One subnet (`/28` or larger) delegated to `Microsoft.Databricks/workspaces`; Databricks runs the gateway in it. Serverless then reaches whatever that subnet can, including peered VNets and on-premises over ExpressRoute or VPN | An internal NLB in front of the database, a VPC endpoint service that allows the Databricks role, and a private endpoint rule on the NCC for the database's FQDN |
| What the source's firewall allows | The gateway subnet's private range: one stable range you own, nothing to refresh | The NLB's traffic, from inside the VPC |
| Routing | `SPECIFIC_DESTINATIONS` routes DNS names only (`destination_type: DNS_NAME`), so a connection on an IP address needs a DNS name (an Azure private DNS zone linked to the VNet), or `ALL_TRAFFIC`, which sends all serverless egress through it | the FQDNs in the rule |
| Lakeflow Connect | Database ingestion is named in the announcement; serverless only, so not for a classic gateway | not stated in the docs |
| Other requirements | Premium plan; the VNet's subscription in the same Entra ID tenant as the workspace's; billed from 2026-12-01 | one NLB and endpoint service per target |

Docs: [private network gateway](https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/private-network-gateway), [configure it](https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/private-network-gateway/configure-private-network-gateway), [announcement](https://www.databricks.com/blog/introducing-private-network-gateway), [AWS PrivateLink to your VPC](https://docs.databricks.com/aws/en/security/network/serverless-network-security/pl-to-internal-network).

The FEVM test workspaces can't use either today: the account APIs refused a non-admin (`This API is disabled for users without account admin status`). Testing needs a Databricks account where the tester is account admin, or an admin to set up the NCC. The plan is in lfc-test-results [test-matrix.md](../../lfc-test-results/references/test-matrix.md#planned).
