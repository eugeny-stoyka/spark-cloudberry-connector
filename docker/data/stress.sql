-- The table of the stress test (README.md, "Performance"): test.its_stress — :rows rows of ~90 bytes
-- of text each (id bigint, d date, amount numeric(12,2), note text), 12 monthly leaf partitions,
-- distributed by id. 560 million rows (default) — ~50 GB of text, ~60 GB in Cloudberry.
--   docker/run.sh stress generate [rows]
--   psql -d cloudberry -v rows=560000000 -f /testdata/stress.sql     (in the cloudberry container, gpadmin)
-- The rows are generated on the segments at once (gp_dist_random('gp_id'): a row per primary segment), in 10
-- batches — transactions of their own; the text size (what the connector transfers) goes into the comment of
-- the table for the measurements (stress.scala).
\set ON_ERROR_STOP on
\if :{?rows}
\else
  \set rows 560000000
\endif
\timing on
set client_min_messages = warning;
drop table if exists test.its_stress;
drop table if exists test.its_stress_copy;
create table test.its_stress (id bigint, d date, amount numeric(12,2), note text)
distributed by (id)
partition by range (d) (start (date '2026-01-01') inclusive end (date '2027-01-01') exclusive every (interval '1 month'));

-- a batch b: the ids lo..hi, segment s of n takes lo + s, lo + s + n, ...
select format($q$insert into test.its_stress
select g, date '2026-01-01' + (g %% 365)::int, (g %% 100000) / 100.0,
       'payment ' || g || ' of the client ' || (g %% 9973) || ' at the shop ' || (g %% 101) || ' paid by card'
from (select gp_segment_id as s from gp_dist_random('gp_id')) seg, generate_series(%s + seg.s, %s, %s) g$q$,
       b * chunk + 1, least((b + 1) * chunk, :rows::bigint), n)
from (select ceil(:rows::numeric / 10)::bigint as chunk,
             (select count(*) from gp_segment_configuration where role = 'p' and content >= 0) as n) p,
     generate_series(0, 9) b
where b * chunk < :rows::bigint
\gexec

analyze test.its_stress;
-- the text of the rows as the connector transfers them: the fields, the tabs, the line ends
select format('comment on table test.its_stress is %L',
              sum(length(id::text) + 1 + 10 + 1 + length(amount::text) + 1 + length(note) + 1))
from test.its_stress
\gexec
select count(*) as rows,
       pg_size_pretty(obj_description('test.its_stress'::regclass)::bigint) as text,
       (select pg_size_pretty(sum(pg_total_relation_size(inhrelid))) from pg_inherits
        where inhparent = 'test.its_stress'::regclass) as in_cloudberry
from test.its_stress;
