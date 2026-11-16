-- tpch-labels.sql (Postgres): TPC-H's own words in the text columns a reader or a model uses, in place of the numbers
-- the generic fill writes there: region and nation names (each nation in its spec region, clause 4.2.3), market
-- segments, order priorities, ship modes and instructions, and Supplier#/Customer# names. Run in the workload's schema
-- after the fill (tpch-sources.sh does). Each value comes from the row's key, so a rerun gives the same values.
--   psql -v names=spec       the spec's names (ALGERIA .. UNITED STATES, AFRICA .. MIDDLE EAST)
--   psql -v names=anonymous  Nation 01 .. 25 and Region 1 .. 5, same map: for data where some nations are made slow
--                            (tpch-lead-times.sql), which with real country names reads as a claim about real countries
UPDATE region SET r_name = CASE WHEN :'names' = 'spec'
  THEN (ARRAY['AFRICA','AMERICA','ASIA','EUROPE','MIDDLE EAST'])[r_regionkey + 1] ELSE 'Region ' || (r_regionkey + 1) END;
UPDATE nation SET
  n_name = CASE WHEN :'names' = 'spec'
    THEN (ARRAY['ALGERIA','ARGENTINA','BRAZIL','CANADA','EGYPT','ETHIOPIA','FRANCE','GERMANY','INDIA','INDONESIA','IRAN',
                'IRAQ','JAPAN','JORDAN','KENYA','MOROCCO','MOZAMBIQUE','PERU','CHINA','ROMANIA','SAUDI ARABIA','VIETNAM',
                'RUSSIA','UNITED KINGDOM','UNITED STATES'])[n_nationkey + 1]
    ELSE 'Nation ' || lpad((n_nationkey + 1)::text, 2, '0') END,
  n_regionkey = (ARRAY[0,1,1,1,4,0,3,3,2,2,4,4,2,4,0,0,0,1,2,3,4,2,3,3,1])[n_nationkey + 1];
UPDATE supplier SET s_name = 'Supplier#' || lpad(s_suppkey::text, 9, '0');
UPDATE customer SET c_name = 'Customer#' || lpad(c_custkey::text, 9, '0'),
  c_mktsegment = (ARRAY['AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY'])[1 + (c_custkey * 7) % 5];
UPDATE orders SET o_orderpriority = (ARRAY['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'])[1 + (o_orderkey * 3) % 5];
UPDATE lineitem SET
  l_shipmode = (ARRAY['AIR','REG AIR','RAIL','SHIP','TRUCK','MAIL','FOB'])[1 + (l_orderkey * 5 + l_linenumber) % 7],
  l_shipinstruct = (ARRAY['DELIVER IN PERSON','COLLECT COD','NONE','TAKE BACK RETURN'])[1 + (l_orderkey + l_linenumber * 3) % 4];
