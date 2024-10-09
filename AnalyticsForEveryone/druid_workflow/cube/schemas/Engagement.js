cube(`Engagement`, {
    sql: `
    SELECT * FROM druid.dev_engagement_low_cardinality_dims WHERE
    ${FILTER_PARAMS.Engagement.os.filter('os')}
    AND ${FILTER_PARAMS.Engagement.age_group.filter('age_group')}
    AND ${FILTER_PARAMS.Engagement.universe_id.filter('universe_id')}
    AND ${FILTER_PARAMS.Engagement.platform.filter('platform')}
    `,
  
    title: `Engagement Data`,
    description: `Engagement data`,
    
    measures: {
      time_spent_secs: {
        sql: `time_spent_secs`,
        type: `sum`,
        title: `Time Spent (Sec)`
      },
      time_spent_hours: {
        sql: `${time_spent_secs} / 3600.0`,
        type: `number`,
        title: `Time Spent (Hours)`
      },
      visits_count: {
        sql: `visits_cnt`,
        type: `sum`,
        title: `Visit Count`
      },
      new_users: {
        sql: `active_users_theta`,
        type: `countDistinctApprox`,
        title: `New Users`,
        filters: [{sql:  `${CUBE}.is_new_user = '1'`}]
      },
      dau: {
        sql: `active_users_theta`,
        type: `countDistinctApprox`,
        title: `Daily Active Users`,
      },
      mau_theta: {
        sql: `THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta))`,
        type: `number`,
        title: `MAUTheta`,
        rollingWindow: {
            trailing: `30 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      mau: {
        sql: `APPROX_COUNT_DISTINCT_DS_HLL(active_users_hll, 14)`,
        type: `number`,
        title: `MAU`,
        rollingWindow: {
            trailing: `30 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      wau: {
        sql: `THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta))`,
        type: `number`,
        title: `WAU`,
        rollingWindow: {
            trailing: `7 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      average_session_length_minutes: {
        sql: `${time_spent_secs} / 60.0 / ${visits_count}`,
        type: `number`,
        title: `Average Session Length Minutes`
      },
      d1_retention_ratio: {
        sql: `100 * LEAST(GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_user_1d_new_theta) FILTER(WHERE(__time = date_from))) - 1, 0) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from AND is_new_user = 1))), 1), 1)`,
        type: `number`,
        title: `Day 1 Retention x100 [DEPRECATED]`,
        rollingWindow: {
          trailing: `1 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      d7_retention_ratio: {
        sql: `100 * LEAST(GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_user_7d_new_theta) FILTER(WHERE(__time = date_from))) - 1, 0) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from AND is_new_user = 1))), 1), 1)`,
        type: `number`,
        title: `Day 7 Retention x100 [DEPRECATED]`,
        rollingWindow: {
          trailing: `7 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      d30_retention_ratio: {
        sql: `100 * LEAST(GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_user_30d_new_theta) FILTER(WHERE(__time = date_from))) - 1, 0) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from AND is_new_user = 1))), 1), 1)`,
        type: `number`,
        title: `Day 30 Retention x100 [DEPRECATED]`,
        rollingWindow: {
          trailing: `30 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      d1_stickiness_ratio: {
        sql: `100 * THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT(DS_THETA(active_users_theta) FILTER(WHERE(__time = date_from)), DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from)))) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from))), 1) - case when(max(date_from) = max(__time)) then 0 else null end`,
        type: `number`,
        title: `Day 1 Stickiness x100 [DEPRECATED]`,
        rollingWindow: {
          trailing: `1 day`,
        }
      },
      d7_stickiness_ratio: {
        sql: `100 * THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT(DS_THETA(active_users_theta) FILTER(WHERE(__time = date_from)), DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from)))) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from))), 1) - case when(max(date_from) = max(__time)) then 0 else null end`,
        type: `number`,
        title: `Day 7 Stickiness x100 [DEPRECATED]`,
        rollingWindow: {
          trailing: `7 day`,
        }
      },
      d30_stickiness_ratio: {
        sql: `100 * THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT(DS_THETA(active_users_theta) FILTER(WHERE(__time = date_from)), DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from)))) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from))), 1) - case when(max(date_from) = max(__time)) then 0 else null end`,
        type: `number`,
        title: `Day 30 Stickiness x100 [DEPRECATED]`,
        rollingWindow: {
          trailing: `30 day`,
        }
      },
      d1_retention: {
        sql: `LEAST(GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_user_1d_new_theta) FILTER(WHERE(__time = date_from))) - 1, 0) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from AND is_new_user = 1))), 1), 1)`,
        type: `number`,
        title: `Day 1 Retention Ratio`,
        rollingWindow: {
          trailing: `1 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      d7_retention: {
        sql: `LEAST(GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_user_7d_new_theta) FILTER(WHERE(__time = date_from))) - 1, 0) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from AND is_new_user = 1))), 1), 1)`,
        type: `number`,
        title: `Day 7 Retention Ratio`,
        rollingWindow: {
          trailing: `7 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      d30_retention: {
        sql: `LEAST(GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_user_30d_new_theta) FILTER(WHERE(__time = date_from))) - 1, 0) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from AND is_new_user = 1))), 1), 1)`,
        type: `number`,
        title: `Day 30 Retention Ratio`,
        rollingWindow: {
          trailing: `30 day`,
        },
        filters: [{sql:  `MAX(${CUBE.time}) = MAX(date_from)`}]
      },
      d1_stickiness: {
        sql: `THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT(DS_THETA(active_users_theta) FILTER(WHERE(__time = date_from)), DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from)))) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from))), 1) - case when(max(date_from) = max(__time)) then 0 else null end`,
        type: `number`,
        title: `Day 1 Stickiness Ratio`,
        rollingWindow: {
          trailing: `1 day`,
        }
      },
      d7_stickiness: {
        sql: `THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT(DS_THETA(active_users_theta) FILTER(WHERE(__time = date_from)), DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from)))) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from))), 1) - case when(max(date_from) = max(__time)) then 0 else null end`,
        type: `number`,
        title: `Day 7 Stickiness Ratio`,
        rollingWindow: {
          trailing: `7 day`,
        }
      },
      d30_stickiness: {
        sql: `THETA_SKETCH_ESTIMATE(THETA_SKETCH_INTERSECT(DS_THETA(active_users_theta) FILTER(WHERE(__time = date_from)), DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from)))) / GREATEST(THETA_SKETCH_ESTIMATE(DS_THETA(active_users_theta) FILTER(WHERE(__time != date_from))), 1) - case when(max(date_from) = max(__time)) then 0 else null end`,
        type: `number`,
        title: `Day 30 Stickiness Ratio`,
        rollingWindow: {
          trailing: `30 day`,
        }
      }

    },
    
    dimensions: {
      time: {
        sql: `__time`,
        type: `time`,
        title: `Time`
      },
      os: {
        sql: `os`,
        type: `string`,
        title: `OS`,
      },
      age_group: {
        sql: `age_group`,
        type: `string`,
        title: `Age Group`,
      },
      universe_id: {
        sql: `universe_id`,
        type: `string`,
        title: `Universe ID`
      },
      platform: {
        sql: `platform`,
        type: `string`,
        title: `Platform`
      },
      gender: {
        sql: `gender`,
        type: `string`,
        title: `Gender`
      },
      is_new_user: {
        sql: `is_new_user`,
        type: `string`,
        title: `Is New User`
      }
    },
  
    // refreshKey: {
    //   every: `12 hour`,
    // },
  
    dataSource: `default`
  });

cube(`EngagementAllDim`, {
  extends: Engagement,
  sql: `
    SELECT * FROM druid.dev_engagement_all_dims WHERE
    ${FILTER_PARAMS.EngagementAllDim.os.filter('os')}
    AND ${FILTER_PARAMS.EngagementAllDim.age_group.filter('age_group')}
    AND ${FILTER_PARAMS.EngagementAllDim.universe_id.filter('universe_id')}
    AND ${FILTER_PARAMS.EngagementAllDim.platform.filter('platform')}
    AND ${FILTER_PARAMS.EngagementAllDim.country.filter('country')}
    AND ${FILTER_PARAMS.EngagementAllDim.locale.filter('locale')}
    `,

  title: `Engagement High Cardinality Data`,
  description: `Engagement high cardinality data including locale and country`,

  dimensions: {
    country: {
      sql: `country`,
      type: `string`,
      title: `Country`
    },
    locale: {
      sql: `locale`,
      type: `string`,
      title: `Locale`
    }
  },

  dataSource: `default`
})
