-- Technical compatibility fixture only. It has no NBP business meaning.
select cast('fixture_a_1' as varchar) as fixture_id,
       cast('2024-01-01' as date) as metric_date,
       cast('A' as varchar) as category,
       cast(2.00 as decimal(18, 2)) as value
union all
select cast('fixture_b_1' as varchar), cast('2024-01-01' as date), cast('B' as varchar), cast(3.00 as decimal(18, 2))
union all
select cast('fixture_a_2' as varchar), cast('2024-01-02' as date), cast('A' as varchar), cast(5.00 as decimal(18, 2))
union all
select cast('fixture_a_3' as varchar), cast('2024-01-03' as date), cast('A' as varchar), cast(7.00 as decimal(18, 2))
union all
select cast('fixture_a_4' as varchar), cast('2024-01-04' as date), cast('A' as varchar), cast(11.00 as decimal(18, 2))
