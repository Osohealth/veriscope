import { db } from "../db";
import { sql } from "drizzle-orm";

const DEFAULT_DAYS_BACK = 60;

export async function backfillPortDailyBaselines(
  daysBack: number = DEFAULT_DAYS_BACK,
  endDate: Date = new Date(),
) {
  const normalizedDays = Math.max(daysBack, 1);
  const end = new Date(Date.UTC(
    endDate.getUTCFullYear(),
    endDate.getUTCMonth(),
    endDate.getUTCDate(),
  ));
  const start = new Date(end);
  start.setUTCDate(end.getUTCDate() - (normalizedDays - 1));

  await db.execute(sql`
    with days as (
      select generate_series(${start}::date, ${end}::date, interval '1 day')::date as day
    ),
    port_list as (
      select id from ports
    ),
    daily_stats as (
      select
        p.id as port_id,
        d.day as date,
        count(*) filter (where pc.arrival_time_utc::date = d.day) as arrivals,
        count(*) filter (where pc.departure_time_utc::date = d.day) as departures,
        count(distinct pc.vessel_id) filter (where pc.arrival_time_utc::date = d.day) as unique_vessels,
        avg(extract(epoch from (pc.departure_time_utc - pc.arrival_time_utc)) / 3600.0)
          filter (where pc.departure_time_utc::date = d.day) as avg_dwell_hours,
        count(*) filter (
          where pc.arrival_time_utc <= d.day + interval '1 day'
            and (pc.departure_time_utc is null or pc.departure_time_utc > d.day + interval '1 day')
        ) as open_calls
      from port_list p
      cross join days d
      left join port_calls pc on pc.port_id = p.id
      group by p.id, d.day
    ),
    with_rollups as (
      select
        daily_stats.*,
        avg(arrivals) over (
          partition by port_id
          order by date
          rows between 29 preceding and current row
        ) as arrivals_30d_avg,
        stddev_samp(arrivals) over (
          partition by port_id
          order by date
          rows between 29 preceding and current row
        ) as arrivals_30d_std,
        avg(avg_dwell_hours) over (
          partition by port_id
          order by date
          rows between 29 preceding and current row
        ) as dwell_30d_avg,
        stddev_samp(avg_dwell_hours) over (
          partition by port_id
          order by date
          rows between 29 preceding and current row
        ) as dwell_30d_std,
        avg(open_calls) over (
          partition by port_id
          order by date
          rows between 29 preceding and current row
        ) as open_calls_30d_avg
      from daily_stats
    )
    insert into port_daily_baselines (
      port_id,
      date,
      arrivals,
      departures,
      unique_vessels,
      avg_dwell_hours,
      open_calls,
      arrivals_30d_avg,
      arrivals_30d_std,
      dwell_30d_avg,
      dwell_30d_std,
      open_calls_30d_avg,
      updated_at
    )
    select
      port_id,
      date,
      coalesce(arrivals, 0),
      coalesce(departures, 0),
      coalesce(unique_vessels, 0),
      avg_dwell_hours,
      coalesce(open_calls, 0),
      arrivals_30d_avg,
      arrivals_30d_std,
      dwell_30d_avg,
      dwell_30d_std,
      open_calls_30d_avg,
      now()
    from with_rollups
    on conflict (port_id, date) do update set
      arrivals = excluded.arrivals,
      departures = excluded.departures,
      unique_vessels = excluded.unique_vessels,
      avg_dwell_hours = excluded.avg_dwell_hours,
      open_calls = excluded.open_calls,
      arrivals_30d_avg = excluded.arrivals_30d_avg,
      arrivals_30d_std = excluded.arrivals_30d_std,
      dwell_30d_avg = excluded.dwell_30d_avg,
      dwell_30d_std = excluded.dwell_30d_std,
      open_calls_30d_avg = excluded.open_calls_30d_avg,
      updated_at = excluded.updated_at
  `);
}

export function schedulePortBaselineJob(options?: {
  daysBack?: number;
  intervalHours?: number;
}) {
  const daysBack = options?.daysBack ?? DEFAULT_DAYS_BACK;
  const intervalHours = Math.max(options?.intervalHours ?? 24, 1);
  const intervalMs = intervalHours * 60 * 60 * 1000;

  const run = async () => {
    try {
      await backfillPortDailyBaselines(daysBack);
    } catch (error) {
      console.error("port_daily_baselines job failed", error);
    }
  };

  void run();
  return setInterval(run, intervalMs);
}
