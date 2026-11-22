"""lfc_medallion_pack: model packs and their runner. A pack holds everything that knows one model (its tiers' signal
numbers, its silver and gold SQL, how gold reads the signal back, its contract); the runner knows no model: lobs draws
the LOBs and their 20-70-10 tiers, plan turns a project's modules into bundle resources within Lakeflow Connect's
limits, source builds the LOBs in the source database, check tests a pack's marts offline, verify checks a deployment.
"""
