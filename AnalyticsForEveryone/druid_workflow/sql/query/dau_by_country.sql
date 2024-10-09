select __time, country, APPROX_COUNT_DISTINCT_DS_THETA(users_theta)
from players_flat_rollup
where universe_id = '1000' and __time = '2024-01-01'
group by __time, country