{{ config(materialized='table') }}

-- Dim Vendor : référentiel statique des fournisseurs de taxi.

with vendors as (
    select 1 as vendor_id, 'Creative Mobile Technologies' as vendor_name
    union all
    select 2, 'VeriFone Inc.'
    union all
    select 6, 'Myle Technologies Inc'
    union all
    select 7, 'Helix'
)

select * from vendors
