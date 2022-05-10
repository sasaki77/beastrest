SQL_CURRENT_ALARM_BASE = """
                         SELECT
                           pv.alarm_time,
                           CASE
                             WHEN at4.name = '{0}'
                               THEN at3.name
                             WHEN at3.name = '{0}'
                               THEN at2.name
                             ELSE at4.name
                           END AS group,
                           CASE
                             WHEN at4.name = '{0}'
                               THEN at2.name
                             WHEN at3.name = '{0}'
                               THEN NULL
                             ELSE at3.name
                           END AS sub_group,
                           CASE
                             WHEN at4.name != '{0}'
                                  AND at3.name != '{0}'
                               THEN at2.name
                             ELSE NULL
                           END AS sub_sub_group,
                           severity.name AS severity,
                           status.name AS status,
                           pv.descr,
                           at1.name AS pv_name,
                           pv.severity_id
                         FROM
                           pv
                             JOIN status
                               ON status.status_id = pv.status_id
                             JOIN severity
                               ON severity.severity_id = pv.severity_id
                             JOIN alarm_tree AS at1
                               ON at1.component_id = pv.component_id
                             LEFT JOIN alarm_tree AS at2
                               ON at1.parent_cmpnt_id = at2.component_id
                             LEFT JOIN alarm_tree AS at3
                               ON at2.parent_cmpnt_id = at3.component_id
                             LEFT JOIN alarm_tree AS at4
                               ON at3.parent_cmpnt_id = at4.component_id
                             LEFT JOIN alarm_tree AS at5
                               ON at4.parent_cmpnt_id = at5.component_id
                         WHERE
                           pv.severity_id > 1
                           AND (at3.name = '{0}' OR at4.name = '{0}'
                                OR at5.name = '{0}')
                           {1}
                         """

SQL_CURRENT_ALARM_ALL = SQL_CURRENT_ALARM_BASE.format(
                        "{0}",
                        ""
                        )

SQL_CURRENT_ALARM_MSG = SQL_CURRENT_ALARM_BASE.format(
                        "{0}",
                        "AND pv.descr ~ '{1}'"
                        )

SQL_PV_LIST = """
              SELECT
                at1.name AS record_name,
                pv.descr AS message,
                CASE
                  WHEN at4.name = '{0}'
                    THEN at3.name
                  WHEN at3.name = '{0}'
                    THEN at2.name
                  ELSE at4.name
                END AS group,
                CASE
                  WHEN at4.name = '{0}'
                    THEN at2.name
                  WHEN at3.name = '{0}'
                    THEN NULL
                  ELSE at3.name
                END AS sub_group,
                CASE
                  WHEN at4.name != '{0}'
                       AND at3.name != '{0}'
                    THEN at2.name
                  ELSE NULL
                END as sub_sub_group
              FROM
                pv
                JOIN alarm_tree AS at1
                  ON at1.component_id = pv.component_id
                LEFT JOIN alarm_tree AS at2
                  ON at1.parent_cmpnt_id = at2.component_id
                LEFT JOIN alarm_tree AS at3
                  ON at2.parent_cmpnt_id = at3.component_id
                LEFT JOIN alarm_tree AS at4
                  ON at3.parent_cmpnt_id = at4.component_id
              """

SQL_HISTORY_BASE = """
                   SELECT
                     *
                   FROM
                   (
                     SELECT
                       message.id AS id
                     , message.datum AS datum
                     , message.name AS record_name
                     , message.severity AS severity
                     , MAX(
                         CASE
                           WHEN msg_property_type.name = 'EVENTTIME'
                             THEN message_content.value
                           ELSE '-'
                         END
                       ) AS eventtime
                     , MAX(
                         CASE
                           WHEN msg_property_type.name = 'STATUS'
                             THEN message_content.value
                           ELSE '-'
                         END
                       ) AS status
                     FROM
                       message
                       JOIN message_content
                         ON message.id=message_content.message_id
                       JOIN msg_property_type
                         ON message_content.msg_property_type_id = msg_property_type.id
                            AND (msg_property_type.name = 'EVENTTIME'
                                 OR msg_property_type.name = 'STATUS'
                                 )
                     WHERE
                       message.datum BETWEEN %s AND %s
                       AND message.type='alarm'
                       {0}
                     GROUP BY
                       message.id
                     ORDER BY
                       message.datum DESC
                     LIMIT
                       100000
                   ) t
                   WHERE
                     status != 'OK'
                     AND status != 'Disabled'
                     AND status != 'Starting'
                   """

SQL_HISTORY_ALL = SQL_HISTORY_BASE.format("")

SQL_HISTORY_GROUP = SQL_HISTORY_BASE.format("AND message.name=ANY(%s)")
