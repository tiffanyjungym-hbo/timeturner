from metaflow import FlowSpec, step, IncludeFile

import argparse
import datetime
import logging
import pathlib
import pandas as pd
import time

from common import snowflake_utils
from typing import Dict, List

import numpy as np
import math
import re
from datetime import date, datetime, timedelta
import json
from abc import ABCMeta, abstractmethod
import boto3
import snowflake.connector

class Credentials(metaclass=ABCMeta):
    pass
    
    
class SSMPSCredentials(Credentials):
    def __init__(self, secretid: str):
        self._secretid = secretid
        self._secrets = {}
        
    def get_keys(self):
        """
        credential fetching 
        """
        _aws_sm_args = {'service_name': 'secretsmanager', 'region_name': 'us-east-1'}
        secrets_client = boto3.client(**_aws_sm_args)
        get_secret_value_response = secrets_client.get_secret_value(SecretId=self._secretid)
        return get_secret_value_response
    
    
class BaseConnector(metaclass=ABCMeta):
    @abstractmethod
    def connect(self):
        raise NotImplementedError

        
class SnowflakeConnector(BaseConnector):
    def __init__(self, credentials: Credentials):
        keys = credentials.get_keys()
        self._secrets = json.loads(keys.get('SecretString', "{}"))

    def connect(self, dbname: str, schema: str = 'DEFAULT'):
        ctx = snowflake.connector.connect(
            user=self._secrets['login_name'],
            password=self._secrets['login_password'],
            account=self._secrets['account'],
            warehouse=self._secrets['warehouse'],
            database=dbname,
            schema=schema
        )
        return ctx

    
def run_query(querystr, ctx):
    cursor_list = ctx.execute_string(
        querystr
        )
    return 


## Credentials
SF_CREDS = 'datascience-max-dev-sagemaker-notebooks'

## Snowflake connection 
conn=SnowflakeConnector(SSMPSCredentials(SF_CREDS))
ctx=conn.connect("MAX_PROD","DATASCIENCE_STAGE")
cur = ctx.cursor()

class TimeTurnerFlow(FlowSpec):
    """
    A flow to generate some statistics about the movie genres.

    The flow performs the following steps:
    1) Ingests a CSV into a Pandas Dataframe.
    2) Fan-out over genre using Metaflow foreach.
    3) Compute quartiles for each genre.
    4) Save a dictionary of genre specific statistics.

    """
    
    @step
    def start(self):
        """
        The start step:
        1) Loads the movie metadata into pandas dataframe.
        2) Finds all the unique genres.
        3) Launches parallel statistics computation for each genre.

        """

        query = '''
        create or replace table max_dev.workspace.timeturner_endpoint as (
        with base_meta_data as (
            select
            distinct title_id
            , title_name
            , tier
            , season_number
            , content_category
            , category
            , effective_start_date
            , case when content_category = 'series' then concat(title_id, '-', season_number) else title_id end as match_id
              from max_prod.content_analytics.psi_past_base
        )
        , activepct_actual as (
            select
            match_id
            , pct_actives as activepct_28d
            ,'actual' as phase
            from max_prod.content_datascience.activepct_us_snapshot_v2
            where days_on_hbo_max=28
        )
        , viewpct_retail_actual as (
            select
            match_id
            , retail_viewed_count_percent * 100 as viewpct_retail_28d
            ,'actual' as phase
            from max_prod.workspace.title_retail_funnel_metrics
            where days_since_first_offered=28
        )
        , activepct_pred_launched as (
            select
              match_id
             , prediction as activepct_28d
             , 'postlaunch' as phase
            from max_prod.content_datascience.activepct_postlaunch_us_pred_v2
            where phase='Post-Launch'
            and target_day = 28
            and pred_day<=28
            qualify row_number() over(partition by match_id order by pred_day desc) = 1
        )
        , viewpct_retail_pred_launched as (
          select
              match_id
            , prediction as viewpct_retail_28d
            , 'postlaunch' as phase
          from max_prod.content_datascience.viewpct_retail_postlaunch_us_pred
          where phase='Post-Launch'
          and target_day = 28
          and pred_day<=28
          qualify row_number() over(partition by match_id order by pred_day desc) = 1
        )
        , firstview_pred_launched as (
          select
            title_id
            , title_name
            , season_number
            , finished_window_flag
            , sum(predicted_first_views) as firstview_90d
            , case when finished_window_flag=1 then 'actual' else 'postlaunch' end as phase
          from max_prod.content_analytics.psi_daily_rw_mean_forecast
          where schedule_label='past'
          group by 1,2,3,4
        )
        --pre_launch, postgl prediction for future titles
        , firstview_pred_future as (
          select
            distinct
             title_name
            , season_number
            , case when season_number > 0 then concat(title_name, ' S', season_number) else title_name end as title_name_season
            , premiere_date as effective_start_date
            , tier
            , content_category
            , category
            , sum(first_views_pred) as firstview_90d
          from max_prod.content_datascience.firstview_postgl
          where schedule_label='alpha'
          group by 1,2,3,4,5,6,7
        )
        , activepct_pred_future as (
            select
            distinct
            title_name
            , prediction as activepct_28d
            , case when phase='Pre-Launch' then 'prelaunch' else 'postgreenlight' end as phase
            from max_prod.content_datascience.activepct_postlaunch_us_pred_v2
            where phase!='Post-Launch'
            and target_day = 28
            and pred_day<=0
            qualify row_number() over(partition by title_name order by pred_day desc) = 1
        )
        , viewpct_retail_pred_future as (
          select
            title_name
            , effective_start_date
            , prediction as viewpct_retail_28d
            , 'prelaunch' as phase
          from max_prod.content_datascience.viewpct_retail_postlaunch_us_pred
          where phase!='Post-Launch'
          and target_day = 28
          qualify row_number() over(partition by match_id order by pred_day desc) = 1
        )
        , timeturner_launched as(
        select
          b.title_id
          , b.match_id
          , initcap(b.title_name) as title_name
          , case when b.season_number > 0 then concat(b.title_name, ' S', b.season_number) else b.title_name end as title_name_season
          , b.effective_start_date
          , b.tier
          , b.season_number
          , b.category
          , b.content_category
          , coalesce(apa.activepct_28d, apl.activepct_28d) as activepct_28d
          , coalesce(vra.viewpct_retail_28d, vrl.viewpct_retail_28d) as viewpct_retail_28d
          , fvl.firstview_90d
          , coalesce(vra.phase, vrl.phase) as phase_28d
          , fvl.phase as phase_90d
        from base_meta_data b
        left join activepct_actual apa
        on b.match_id= apa.match_id
        left join viewpct_retail_actual vra
        on b.match_id= vra.match_id
        left join activepct_pred_launched apl
        on b.match_id = apl.match_id
        left join viewpct_retail_pred_launched vrl
        on b.match_id=vrl.match_id
        left join firstview_pred_launched fvl
        on b.title_id = fvl.title_id and b.season_number = fvl.season_number
        order by effective_start_date desc
        )
        , timeturner_future as (
        select
            NULL as title_id
          , NULL as match_id
          , initcap(fpf.title_name) as title_name
          , fpf.title_name_season
          , fpf.effective_start_date
          , fpf.tier
          , fpf.season_number
          , fpf.category
          , fpf.content_category
          , activepct_28d
          , viewpct_retail_28d
          , firstview_90d
          , apf.phase as phase_28d
          , 'postgreenlight' as phase_90d
        from firstview_pred_future fpf
        left join activepct_pred_future apf
            on fpf.title_name_season=apf.title_name
        left join viewpct_retail_pred_future vpf
            on fpf.title_name_season=vpf.title_name
        order by effective_start_date desc
        )
        select
        *
        from timeturner_launched
        union
        select
        *
        from timeturner_future
        order by effective_start_date asc
    )'''
        
        run_query(querystr_base_assets, ctx)
        self.next(self.end)

    @step
    def end(self):
        """
        End the flow.

        """
        pass


if __name__ == "__main__":
    TimeTurnerFlow()
