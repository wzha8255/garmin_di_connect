




# dbt Project Analysis: Calculation Logics and Transformations
Project Overview

Project Name: [Project Name]

Project Description: [Brief description of the project]
 


## Model Analysis


## Table of Contens
- [Model 1: staging_au_catsales_basket_sales] 

---

## Model 1: staging_au_catsales_basket_sales
<a name="staging_au_catsales_basket_sales"></a>

Description: 
 Get last 13 weeks (a week is calculated as Wednesday to Tuesday) all basket sales (not aggregated), enrich with site, customer lifestage segment, price segment/ affluence, calendar week (startdate, enddate, weekno). All basket sales between ty_startdate and ty_enddate.

SQL Code:



```sql
# collapse-code [model_1]

{{ config(
  tags= ["au_catsales"]
)
}}

# calendar: generate last 13 weeks with weekno,startdate(Wednesday),enddate(Tuesday)

WITH calendar AS (
        SELECT 
                enddate.calendarday AS ty_enddate, 
                startdate.calendarday AS ty_startdate,
                DENSE_RANK() OVER (ORDER BY startdate.calendarday DESC) weekno
        FROM {{ ref('dim_date_current') }} startdate
              JOIN {{ ref('dim_date_current') }}  enddate
                  on enddate.calendarday = DATE_ADD(startdate.calendarday,INTERVAL 6 DAY)
        WHERE enddate.weekdayname = 'Tuesday'
        
        AND DATE_DIFF(CURRENT_DATE(), enddate.calendarday, DAY) > 0
        GROUP BY    
                enddate.calendarday , 
                startdate.calendarday
        ORDER BY startdate.calendarday desc
        LIMIT 13
) ,


ty AS (
    SELECT 
      ty_startdate,
      ty_enddate,
      weekno, 
      art.sub_brand AS brand,
      art.categorydescription AS category,
      art.sub_categorydescription AS subcategory,
      IFNULL(site.state,'UNKNOW') AS state,

      (CASE WHEN sales.postxntype = 'S101' THEN 'In-store' ELSE 'Online' END ) AS channel_view,
      (CASE
          WHEN lseg.lifestagesegmentdescription IS NULL THEN 'NO SEGMENT'
        ELSE
        lseg.lifestagesegmentdescription
      END
      )  AS lifestage_segment,

      (CASE
          WHEN cseg.pricesegmentdescription IS NULL THEN 'UNKNOWN'
        ELSE
        cseg.pricesegmentdescription
      END
      )  AS affluence,

      basketkey AS basket,
      sales.retailquantity AS units,
      sales.RetailAmount AS amt_inc_gst,

      CASE
            WHEN POSTXNType = 'S101' THEN Sales.TaxableAmount
            ELSE
            IFNULL(Sales.RetailAmount,0) - IFNULL(Sales.taxamount,0)
        END AS amt_exc_gst

      FROM 
            {{ ref('smkt_pos_item_line_detail_crn_current') }} sales
      INNER JOIN 
      -- enrich article_hierarchy
            {{ ref('dim_article_hierarchy_all') }} art
        ON sales.article = art.article
        AND CAST(art.SalesOrg AS STRING) IN ('1005')
        -- what is postxntype? channel view: s101 in-store, s111 online.
        AND sales.postxntype IN ('S101','S111')
        AND  IFNULL(art.sub_brand,'') <> ''
        AND  IFNULL(art.categorydescription,'') <> ''
        AND  IFNULL(art.sub_categorydescription,'') <> ''
      LEFT OUTER JOIN 
      -- join site
            {{ ref('dim_site_current') }}  site
        ON site.site = sales.sitenumber
      LEFT OUTER JOIN
      -- join lifestage_segment: young couple, retiree ...
            {{ ref('qtm_cust_lifestage_segment_curr') }} lseg
        ON
          sales.customerregistrationnumber = lseg.crn
      LEFT OUTER JOIN
      -- join price_segment: budget, ...
            {{ ref('qtm_cust_price_segment_curr') }} cseg
      ON
        sales.customerregistrationnumber = cseg.crn
      JOIN calendar
      -- join calendar get weekno, startdate and enddate
                on sales.businessdate BETWEEN calendar.ty_startdate AND calendar.ty_enddate

)
SELECT
        ty.ty_startdate,
        ty.ty_enddate,
        ty.weekno,
        ty.brand,
        ty.category,
        ty.subcategory,
        ty.state,
        ty.channel_view,
        ty.lifestage_segment,
        ty.affluence,
        ty.basket,
        ty.units,
        ty.amt_inc_gst,
        ty.amt_exc_gst
FROM ty

```


---
## Model 2: staging_au_catsales_ly

Description: Get the corresponding 13 weeks which are 52 weeks prior to the current last 13 weeks (a week is calculated as Wednesday to Tuesday),  aggregated sales (# of baskets, $sales inc gst, $sales exc gst, # of units) by dimensions (weekno, brand, category, subcategory, state, channel_view, lifestagesegment, pricestagesegment)

SQL Code:

```sql
{{ config(
  tags= ["au_catsales"]
)
}}

-- generate calendar as the last 13 weeks (Wednesday to Tuesday) with startdate and enddate [ty_startdate,ty_enddate], for each week get the week (Wednesday to Tuesday) 52 weeks ago [ly_startdate, ly_enddate].
WITH calendar AS (
        SELECT 
                DENSE_RANK() OVER (ORDER BY startdate.calendarday DESC) weekno, 
                
                enddate.calendarday AS ty_enddate, 
                startdate.calendarday AS ty_startdate,

                min(startdately.calendarday) AS ly_startdate,
                min(enddately.calendarday) AS ly_enddate

        FROM {{ ref('dim_date_current') }} startdate
              JOIN {{ ref('dim_date_current') }} enddate
                  ON enddate.calendarday = DATE_ADD(startdate.calendarday,INTERVAL 6 DAY)
                  AND enddate.weekdayname = 'Tuesday'
                  -- enddate.calendarday < current_date()
                  AND DATE_DIFF(CURRENT_DATE(), enddate.calendarday, DAY) > 0
              JOIN {{ ref('dim_date_current') }} enddately
                  ON (enddately.calendarday BETWEEN DATE_SUB(enddate.calendarday,INTERVAL 52 WEEK) AND DATE_SUB(enddate.calendarday,INTERVAL 51 WEEK) )
                  AND enddately.weekdayname = 'Tuesday'
              JOIN {{ ref('dim_date_current') }} startdately
                  on startdately.calendarday = DATE_SUB(enddately.calendarday,INTERVAL 6 DAY)
        GROUP BY      
                enddate.calendarday , 
                startdate.calendarday
        ORDER BY startdate.calendarday desc
        LIMIT 13
),

-- 
-- ly: basket sales between ly_startdate and ly_enddate with article_hierarchy_all, site, lifestage_segment, price_segment
ly AS (
    SELECT 
    	  ty_enddate,
          ty_startdate,
          ly_startdate,
          ly_enddate,
          weekno, 
          art.sub_brand AS brand,
          art.categorydescription AS category,
          art.sub_categorydescription AS subcategory,
          IFNULL(site.state,'UNKNOW') AS state,
          CASE WHEN sales.postxntype = 'S101' THEN 'In-store' ELSE 'Online' end AS channel_view,
          CASE
              WHEN lseg.lifestagesegmentdescription IS NULL THEN 'NO SEGMENT'
            ELSE
            lseg.lifestagesegmentdescription
          END
            AS lifestage_segment,
          CASE
              WHEN cseg.pricesegmentdescription IS NULL THEN 'UNKNOWN'
          ELSE
            cseg.pricesegmentdescription
          END
            AS affluence,
          COUNT(DISTINCT basketkey) ly_total_basket,
          SUM(sales.retailquantity) ly_units,
          SUM(Sales.RetailAmount) AS ly_amt_inc_gst, 
          SUM(
            CASE
                WHEN POSTXNType = 'S101' THEN Sales.TaxableAmount
                ELSE
                IFNULL(Sales.RetailAmount,0) - IFNULL(Sales.taxamount,0)
            END
            ) AS ly_amt_exc_gst
      FROM 
            {{ ref('smkt_pos_item_line_detail_crn_current') }} sales
      INNER JOIN 
            {{ ref('dim_article_hierarchy_all') }} art
        ON sales.article = art.article
        AND CAST(art.SalesOrg AS STRING) IN ('1005')
        AND sales.postxntype IN ('S101','S111')
        AND  IFNULL(art.sub_brand,'') <> ''
        AND  IFNULL(art.categorydescription,'') <> ''
        AND  IFNULL(art.sub_categorydescription,'') <> ''
      LEFT OUTER JOIN 
            {{ ref('dim_site_current') }}  site
        ON site.site = sales.sitenumber
      LEFT OUTER JOIN
            {{ ref('qtm_cust_lifestage_segment_curr') }} lseg
        ON
          sales.customerregistrationnumber = lseg.crn
      LEFT OUTER JOIN
            {{ ref('qtm_cust_price_segment_curr') }} cseg
      ON
        sales.customerregistrationnumber = cseg.crn
      JOIN calendar
                on sales.businessdate BETWEEN calendar.ly_startdate AND calendar.ly_enddate
    GROUP BY    
                ty_enddate,
                ty_startdate,
                ly_startdate,
                ly_enddate,
                weekno, 
                art.sub_brand,
                art.categorydescription,
                art.sub_categorydescription,
                site.state,
                CASE WHEN sales.postxntype = 'S101' THEN 'In-store' ELSE 'Online' END,
                lseg.lifestagesegmentdescription,
                cseg.pricesegmentdescription
)
SELECT
		    ly.ty_enddate,
        ly.ty_startdate,
        ly.ly_startdate,
        ly.ly_enddate,
        ly.weekno, 
        ly.brand,
        ly.category,
        ly.subcategory,
        ly.state,
        ly.channel_view,
        ly.lifestage_segment,
        ly.affluence,
        ly_total_basket,
        ly_units, 
        ly_amt_inc_gst,
        ly_amt_exc_gst
FROM ly

```


---
## Model 3: staging_au_catsales_lq

Description:  Get the corresponding 13 weeks which are 13 weeks prior to the current last 13 weeks (a week is calculated as Wednesday to Tuesday),  aggregated sales (# of baskets, $sales inc gst, $sales exc gst, # of units) by dimensions (weekno, brand, category, subcategory, state, channel_view, lifestagesegment, pricestagesegment)t, price segment/ affluence, calendar week (startdate, enddate, weekno)

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}

WITH calendar AS (
        SELECT 
                DENSE_RANK() OVER (ORDER BY startdate.calendarday DESC) weekno, 
                
                enddate.calendarday AS ty_enddate, 
                startdate.calendarday AS ty_startdate,

                min(startdatelq.calendarday) AS lq_startdate,
                min(enddatelq.calendarday) AS lq_enddate

        FROM {{ ref('dim_date_current') }} startdate
              JOIN {{ ref('dim_date_current') }} enddate
                  ON enddate.calendarday = DATE_ADD(startdate.calendarday,INTERVAL 6 DAY)
                  AND enddate.weekdayname = 'Tuesday'
                  AND DATE_DIFF(CURRENT_DATE(), enddate.calendarday, DAY) > 0
              JOIN {{ ref('dim_date_current') }} enddatelq
              -- 13 weeks prior to the current matching week.
                  ON (enddatelq.calendarday BETWEEN DATE_SUB(enddate.calendarday,INTERVAL 13 WEEK) AND DATE_SUB(enddate.calendarday,INTERVAL 12 WEEK) )
                  AND enddatelq.weekdayname = 'Tuesday'
              JOIN {{ ref('dim_date_current') }} startdatelq
                  on startdatelq.calendarday = DATE_SUB(enddatelq.calendarday,INTERVAL 6 DAY)
        GROUP BY      
                enddate.calendarday , 
                startdate.calendarday
        ORDER BY startdate.calendarday desc
        LIMIT 13
),
lq AS (
    SELECT
          ty_enddate,
          ty_startdate,
          lq_startdate,
          lq_enddate,
          weekno, 
          art.sub_brand AS brand,
          art.categorydescription AS category,
          art.sub_categorydescription AS subcategory,
          IFNULL(site.state,'UNKNOW') AS state,
          CASE WHEN sales.postxntype = 'S101' THEN 'In-store' ELSE 'Online' end AS channel_view,
          CASE
              WHEN lseg.lifestagesegmentdescription IS NULL THEN 'NO SEGMENT'
            ELSE
            lseg.lifestagesegmentdescription
          END
            AS lifestage_segment,
          CASE
              WHEN cseg.pricesegmentdescription IS NULL THEN 'UNKNOWN'
          ELSE
            cseg.pricesegmentdescription
          END
            AS affluence,
          COUNT(DISTINCT basketkey) lq_total_basket,
          SUM(sales.retailquantity) lq_units,
          SUM(Sales.RetailAmount) AS lq_amt_inc_gst, 
          SUM(
            CASE
                WHEN POSTXNType = 'S101' THEN Sales.TaxableAmount
                ELSE
                IFNULL(Sales.RetailAmount,0) - IFNULL(Sales.taxamount,0)
            END
            ) AS lq_amt_exc_gst
      FROM 
            {{ ref('smkt_pos_item_line_detail_crn_current') }} sales
      INNER JOIN 
            {{ ref('dim_article_hierarchy_all') }} art
        ON sales.article = art.article
        AND CAST(art.SalesOrg AS STRING) IN ('1005')
        AND sales.postxntype IN ('S101','S111')
        AND  IFNULL(art.sub_brand,'') <> ''
        AND  IFNULL(art.categorydescription,'') <> ''
        AND  IFNULL(art.sub_categorydescription,'') <> ''
      LEFT OUTER JOIN 
            {{ ref('dim_site_current') }}  site
        ON site.site = sales.sitenumber
      LEFT OUTER JOIN
            {{ ref('qtm_cust_lifestage_segment_curr') }} lseg
        ON
          sales.customerregistrationnumber = lseg.crn
      LEFT OUTER JOIN
            {{ ref('qtm_cust_price_segment_curr') }} cseg
      ON
        sales.customerregistrationnumber = cseg.crn
      JOIN calendar
                -- basket sales between lq_startdate and lq_enddate.
                ON sales.businessdate BETWEEN calendar.lq_startdate AND calendar.lq_enddate
    GROUP BY    
                ty_enddate,
                ty_startdate,
                lq_startdate,
                lq_enddate,
                weekno, 
                art.sub_brand,
                art.categorydescription,
                art.sub_categorydescription,
                site.state,
                CASE WHEN sales.postxntype = 'S101' THEN 'In-store' ELSE 'Online' END,
                lseg.lifestagesegmentdescription,
                cseg.pricesegmentdescription
)
SELECT  
        lq.ty_enddate,
        lq.ty_startdate,
        lq.lq_startdate,
        lq.lq_enddate,
        lq.weekno, 
        lq.brand,
        lq.category,
        lq.subcategory,
        lq.state,
        lq.channel_view,
        lq.lifestage_segment,
        lq.affluence,
        lq_total_basket,
        lq_units, 
        lq_amt_inc_gst,
        lq_amt_exc_gst
FROM lq

```

---
## Model 4: staging_au_catsales_lh

Description: Get the corresponding 13 weeks which are 26 weeks prior to the current last 13 weeks (a week is calculated as Wednesday to Tuesday),  aggregated sales (# of baskets, $sales inc gst, $sales exc gst, # of units) by dimensions (weekno, brand, category, subcategory, state, channel_view, lifestagesegment, pricestagesegment)

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}
  
WITH calendar AS (
        SELECT 
                DENSE_RANK() OVER (ORDER BY startdate.calendarday DESC) AS weekno, 
                
                enddate.calendarday AS ty_enddate, 
                startdate.calendarday AS ty_startdate,

                min(startdatelh .calendarday) AS lh_startdate,
                min(enddatelh.calendarday) AS lh_enddate

        FROM {{ ref('dim_date_current') }} startdate
              JOIN {{ ref('dim_date_current') }} enddate
                  ON enddate.calendarday = DATE_ADD(startdate.calendarday,INTERVAL 6 DAY)
                  AND enddate.weekdayname = 'Tuesday'
                  AND DATE_DIFF(CURRENT_DATE(), enddate.calendarday, DAY) > 0
              JOIN {{ ref('dim_date_current') }} enddatelh
                  ON (enddatelh.calendarday BETWEEN DATE_SUB(enddate.calendarday,INTERVAL 26 WEEK) AND DATE_SUB(enddate.calendarday,INTERVAL 25 WEEK) )
                  AND enddatelh.weekdayname = 'Tuesday'
              JOIN {{ ref('dim_date_current') }} startdatelh 
                  on startdatelh .calendarday = DATE_SUB(enddatelh.calendarday,INTERVAL 6 DAY)
        GROUP BY      
                enddate.calendarday , 
                startdate.calendarday
        ORDER BY startdate.calendarday desc
        LIMIT 13
),
lh AS (
    SELECT 
    	  ty_startdate,
          ty_enddate,
          lh_startdate,
          lh_enddate,
          weekno, 
          art.sub_brand AS brand,
          art.categorydescription AS category,
          art.sub_categorydescription AS subcategory,
          IFNULL(site.state,'UNKNOW') AS state,
          CASE WHEN sales.postxntype = 'S101' THEN 'In-store' ELSE 'Online' end AS channel_view,
          CASE
              WHEN lseg.lifestagesegmentdescription IS NULL THEN 'NO SEGMENT'
            ELSE
            lseg.lifestagesegmentdescription
          END
            AS lifestage_segment,
          CASE
              WHEN cseg.pricesegmentdescription IS NULL THEN 'UNKNOWN'
          ELSE
            cseg.pricesegmentdescription
          END
            AS affluence,
          COUNT(DISTINCT basketkey) lh_total_basket,
          SUM(sales.retailquantity) lh_units,
          SUM(Sales.RetailAmount) AS lh_amt_inc_gst, 
          SUM(
            CASE
                WHEN POSTXNType = 'S101' THEN Sales.TaxableAmount
                ELSE
                IFNULL(Sales.RetailAmount,0) - IFNULL(Sales.taxamount,0)
            END
            ) AS lh_amt_exc_gst
      FROM 
            {{ ref('smkt_pos_item_line_detail_crn_current') }} sales
      INNER JOIN 
            {{ ref('dim_article_hierarchy_all') }} art
        ON sales.article = art.article
        AND CAST(art.SalesOrg AS STRING) IN ('1005')
        AND sales.postxntype IN ('S101','S111')
        AND  IFNULL(art.sub_brand,'') <> ''
        AND  IFNULL(art.categorydescription,'') <> ''
        AND  IFNULL(art.sub_categorydescription,'') <> ''
      LEFT OUTER JOIN 
            {{ ref('dim_site_current') }}  site
        ON site.site = sales.sitenumber
      LEFT OUTER JOIN
            {{ ref('qtm_cust_lifestage_segment_curr') }} lseg
        ON
          sales.customerregistrationnumber = lseg.crn
      LEFT OUTER JOIN
            {{ ref('qtm_cust_price_segment_curr') }} cseg
      ON
        sales.customerregistrationnumber = cseg.crn
      JOIN calendar
                ON sales.businessdate BETWEEN calendar.lh_startdate AND calendar.lh_enddate
    GROUP BY    
                
                ty_enddate,
                ty_startdate,
                lh_startdate,
                lh_enddate,
                weekno, 
                art.sub_brand,
                art.categorydescription,
                art.sub_categorydescription,
                site.state,
                CASE WHEN sales.postxntype = 'S101' THEN 'In-store' ELSE 'Online' END,
                lseg.lifestagesegmentdescription,
                cseg.pricesegmentdescription
)
SELECT
        lh.ty_enddate,
        lh.ty_startdate,
        lh.lh_startdate,
        lh.lh_enddate,
        lh.weekno, 
        lh.brand,
        lh.category,
        lh.subcategory,
        lh.state,
        lh.channel_view,
        lh.lifestage_segment,
        lh.affluence,
        lh_total_basket,
        lh_units, 
        lh_amt_inc_gst,
        lh_amt_exc_gst
FROM lh

```


---
## Model 5: staging_au_catsales_ty

Description: Aggregate the basket sales (# of baskets, # of units, $sales inc gst, $sales exc gst) of the last 13 weeks by dimensions of ty_startdate/ty_enddate/weekno, brand, category, subcategory, state, channel_view, lifestage_segment, affluence(price_segment).

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}


SELECT
        ty.ty_startdate,
        ty.ty_enddate,
        ty.weekno,
        ty.brand,
        ty.category,
        ty.subcategory,
        ty.state,
        ty.channel_view,
        ty.lifestage_segment,
        ty.affluence,
        COUNT(DISTINCT ty.basket) AS ty_total_basket,
        SUM(ty.units) AS ty_units, 
        SUM(ty.amt_inc_gst) AS ty_amt_inc_gst,
        SUM(ty.amt_exc_gst) AS ty_amt_exc_gst
FROM {{ ref('staging_au_catsales_basket_sales') }}  ty
GROUP BY 
        ty.ty_startdate,
        ty.ty_enddate,
        ty.weekno,
        ty.brand,
        ty.category,
        ty.subcategory,
        ty.state,
        ty.channel_view,
        ty.lifestage_segment,
        ty.affluence
```

---
## Model 6: staging_au_catsales_stage_0

Description:  All dimensions table. Get all dimensions [] from staging_au_catsales_ty,staging_au_catsales_ly,staging_au_catsales_lh,staging_au_catsales_lq

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}


SELECT 
      ty_startdate,
      ty_enddate,
      weekno, 
      brand,
      category,
      subcategory,
      state,
      channel_view,
      lifestage_segment,
      affluence 

FROM (
SELECT 
      ty_startdate,
      ty_enddate,
      weekno, 
      brand,
      category,
      subcategory,
      state,
      channel_view,
      lifestage_segment,
      affluence
FROM {{ ref('staging_au_catsales_ty') }} 
UNION ALL
SELECT 
      ty_startdate,
      ty_enddate,
      weekno, 
      brand,
      category,
      subcategory,
      state,
      channel_view,
      lifestage_segment,
      affluence
FROM {{ ref('staging_au_catsales_ly') }} 
UNION ALL
SELECT 
      ty_startdate,
      ty_enddate,
      weekno, 
      brand,
      category,
      subcategory,
      state,
      channel_view,
      lifestage_segment,
      affluence
FROM {{ ref('staging_au_catsales_lq') }} 
UNION ALL
SELECT 
      ty_startdate,
      ty_enddate,
      weekno, 
      brand,
      category,
      subcategory,
      state,
      channel_view,
      lifestage_segment,
      affluence
FROM {{ ref('staging_au_catsales_lh') }} 
) a
GROUP BY ty_startdate,
      ty_enddate,
      weekno, 
      brand,
      category,
      subcategory,
      state,
      channel_view,
      lifestage_segment,
      affluence
```

---
## Model 7: staging_au_catsales_stage_2

Description: Aggregate sales ($sales exc gst, $sales inc gst, # of baskets) by dimension (category, subcategory, weekno)

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}

select
        category
        ,subcategory
        ,weekno
        ,SUM(amt_exc_gst) ty_amt_exc_gst
        ,SUM(amt_inc_gst) ty_amt_inc_gst
        ,COUNT(DISTINCT basket) ty_total_basket
FROM {{ ref('staging_au_catsales_basket_sales') }}   a 
GROUP BY category
        ,subcategory
        ,weekno
```


---
## Model 8: staging_au_catsales_stage_3

Description: Aggregate sales ($sales exc gst, $sales inc gst, # of baskets) by dimension (category, weekno)

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}

SELECT
        category
        ,weekno
        ,SUM(amt_exc_gst) ty_amt_exc_gst
        ,SUM(amt_inc_gst) ty_amt_inc_gst
        ,COUNT(DISTINCT basket) ty_total_basket
FROM {{ ref('staging_au_catsales_basket_sales') }}   a 
GROUP BY category
        ,weekno
```

---
## Model 9: staging_au_catsales_stage_4

Description: Aggregate sales ($sales exc gst, $sales inc gst, # of baskets) by dimension ( weekno)

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}

SELECT
        weekno
        ,SUM(amt_exc_gst) ty_amt_exc_gst
        ,SUM(amt_inc_gst) ty_amt_inc_gst
        ,COUNT(DISTINCT basket) ty_total_basket
FROM {{ ref('staging_au_catsales_basket_sales') }}   a 
GROUP BY weekno
```


---
## Model 9: staging_au_catsales_stage_5

Description: Aggregate basket sales ($sales exc gst, $sales inc gst) by dimensions ( ty_startdate/ty_enddate/weekno, brand, category, subcategory, state, channel_view, lifestage_segment, affluence(price_segment))

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales_crn","au_catsales"]
)
}}

WITH basketsale AS (

          SELECT
                basket,
                SUM(amt_inc_gst) AS basket_sales_incl_gst,
                SUM(amt_exc_gst) AS basket_sales_excl_gst
          FROM {{ ref('staging_au_catsales_basket_sales') }}   a
          GROUP BY basket

),
dim AS (
          SELECT  
                  ty_startdate,
                  ty_enddate,
                  weekno,
                  brand,
                  category,
                  subcategory,
                  state,
                  channel_view,
                  lifestage_segment,
                  affluence,
                  basket
          FROM {{ ref('staging_au_catsales_basket_sales') }}   a
          GROUP BY
                  ty_startdate,
                  ty_enddate,
                  weekno,
                  brand,
                  category,
                  subcategory,
                  state,
                  channel_view,
                  lifestage_segment,
                  affluence,
                  basket 
)
SELECT 
      ty_startdate,
      ty_enddate,
      weekno,
      brand,
      category,
      subcategory,
      state,
      channel_view,
      lifestage_segment,
      affluence,
      SUM(bs.basket_sales_incl_gst) AS total_basket_sales_incl_gst, 
      SUM(bs.basket_sales_excl_gst) AS total_basket_sales_excl_gst
FROM dim  dim
    JOIN basketsale  bs
      ON dim.basket = bs.basket
GROUP BY ty_startdate,
      ty_enddate,
      weekno,
      brand,
      category,
      subcategory,
      state,
      channel_view,
      lifestage_segment,
      affluence

```

---
## Model 10: staging_au_catsales_stage_0_calendar

Description: All calendar dates table, holding all calendar dates for ty_startdate, ty_enddate, weekno, ly_startdate, ly_enddate, lh_startdate,lh_enddate,lq_startdate,lq_enddate.

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}


SELECT DISTINCT
      dim.weekno,
      ty.ty_startdate,
      ty.ty_enddate,

      ly.ly_startdate,
      ly.ly_enddate,

      lq.lq_startdate,
      lq.lq_enddate,

      lh.lh_startdate,
      lh.lh_enddate
FROM {{ ref('staging_au_catsales_stage_0') }} dim
LEFT OUTER JOIN {{ ref('staging_au_catsales_ty') }} ty
            on 
                dim.brand = ty.brand
            AND dim.category = ty.category
            AND dim.subcategory = ty.subcategory
            AND dim.state = ty.state
            AND dim.channel_view = ty.channel_view
            AND dim.lifestage_segment = ty.lifestage_segment
            AND dim.affluence = ty.affluence
            AND dim.weekno = ty.weekno
LEFT OUTER JOIN {{ ref('staging_au_catsales_ly') }}  ly
            on 
            dim.brand = ly.brand
            AND dim.category = ly.category
            AND dim.subcategory = ly.subcategory
            AND dim.state = ly.state
            AND dim.channel_view = ly.channel_view
            AND dim.lifestage_segment = ly.lifestage_segment
            AND dim.affluence = ly.affluence
            AND dim.weekno = ly.weekno
LEFT OUTER JOIN {{ ref('staging_au_catsales_lh') }}  lh
            on 
            dim.brand = lh.brand
            AND dim.category = lh.category
            AND dim.subcategory = lh.subcategory
            AND dim.state = lh.state
            AND dim.channel_view = lh.channel_view
            AND dim.lifestage_segment = lh.lifestage_segment
            AND dim.affluence = lh.affluence
            AND dim.weekno = lh.weekno
LEFT OUTER JOIN {{ ref('staging_au_catsales_lq') }}  lq
            on 
            dim.brand = lq.brand
            AND dim.category = lq.category
            AND dim.subcategory = lq.subcategory
            AND dim.state = lq.state
            AND dim.channel_view = lq.channel_view
            AND dim.lifestage_segment = lq.lifestage_segment
            AND dim.affluence = lq.affluence
            AND dim.weekno = lq.weekno
WHERE 
            ty.ty_startdate is not null
      AND   ty.ty_enddate  is not null
      AND   ly.ly_startdate  is not null
      AND   ly.ly_enddate  is not null

      AND   lq.lq_startdate  is not null
      AND   lq.lq_enddate  is not null

      AND   lh.lh_startdate  is not null
      AND   lh.lh_enddate  is not null
```

---
## Model 11: staging_au_catsales_stage_1

Description: 
**Full weekly sales including ty, ly, lh, lq by all dimensions.**

 Get aggregated weekly sales (total_basket, $sales inc gst, $sales exc gst, units) for each week in the last 13 weeks, the 13 weeks 52 weeks prior, the 13 weeks 26 weeks prior, the 13 weeks 13 weeks prior, by dimensions (weekno, brand, category, subcategory, state, channel_view, lifestagesegment, pricesegment/affluence)

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}


SELECT 
      calendar.ty_startdate,
      calendar.ty_enddate,

      calendar.ly_startdate,
      calendar.ly_enddate,

      calendar.lq_startdate,
      calendar.lq_enddate,

      calendar.lh_startdate,
      calendar.lh_enddate,

      dim.weekno, 
      dim.brand,
      dim.category,
      dim.subcategory,
      dim.state,
      dim.channel_view,
      dim.lifestage_segment,
      dim.affluence,

      ty_units, 
      ty_amt_exc_gst,
      ty_amt_inc_gst,
      ty_total_basket,

      total_basket_sales.total_basket_sales_incl_gst, 
      total_basket_sales.total_basket_sales_excl_gst,

      ly_units, 
      ly_amt_exc_gst,
      ly_amt_inc_gst,
      ly_total_basket,

      lh_units, 
      lh_amt_exc_gst,
      lh_amt_inc_gst,
      lh_total_basket,

      lq_units, 
      lq_amt_exc_gst,
      lq_amt_inc_gst,
      lq_total_basket
FROM {{ ref('staging_au_catsales_stage_0') }} dim
JOIN {{ ref('staging_au_catsales_stage_0_calendar')}} calendar
      ON dim.weekno = calendar.weekno
LEFT OUTER JOIN {{ ref('staging_au_catsales_ty') }} ty
            ON 
                dim.brand = ty.brand
            AND dim.category = ty.category
            AND dim.subcategory = ty.subcategory
            AND dim.state = ty.state
            AND dim.channel_view = ty.channel_view
            AND dim.lifestage_segment = ty.lifestage_segment
            AND dim.affluence = ty.affluence
            AND dim.weekno = ty.weekno
LEFT OUTER JOIN {{ ref('staging_au_catsales_ly') }}  ly
            ON 
            dim.brand = ly.brand
            AND dim.category = ly.category
            AND dim.subcategory = ly.subcategory
            AND dim.state = ly.state
            AND dim.channel_view = ly.channel_view
            AND dim.lifestage_segment = ly.lifestage_segment
            AND dim.affluence = ly.affluence
            AND dim.weekno = ly.weekno
LEFT OUTER JOIN {{ ref('staging_au_catsales_lh') }}  lh
            ON 
            dim.brand = lh.brand
            AND dim.category = lh.category
            AND dim.subcategory = lh.subcategory
            AND dim.state = lh.state
            AND dim.channel_view = lh.channel_view
            AND dim.lifestage_segment = lh.lifestage_segment
            AND dim.affluence = lh.affluence
            AND dim.weekno = lh.weekno
LEFT OUTER JOIN {{ ref('staging_au_catsales_lq') }}  lq
            ON 
            dim.brand = lq.brand
            AND dim.category = lq.category
            AND dim.subcategory = lq.subcategory
            AND dim.state = lq.state
            AND dim.channel_view = lq.channel_view
            AND dim.lifestage_segment = lq.lifestage_segment
            AND dim.affluence = lq.affluence
            AND dim.weekno = lq.weekno
LEFT OUTER JOIN {{ ref('staging_au_catsales_stage_5') }}  total_basket_sales
            ON 
            dim.brand = total_basket_sales.brand
            AND dim.category = total_basket_sales.category
            AND dim.subcategory = total_basket_sales.subcategory
            AND dim.state = total_basket_sales.state
            AND dim.channel_view = total_basket_sales.channel_view
            AND dim.lifestage_segment = total_basket_sales.lifestage_segment
            AND dim.affluence = total_basket_sales.affluence
            AND dim.weekno = total_basket_sales.weekno            
```


---
## Model 12: fact_au_catsales_overall

Description: Fact table holding sales data ($sales inc gst, $sales exc gst, # of baskets), market share, penetration rate in 13 weeks and 4 weeks. Detailed metrics including:
1. total_basket_sales_incl_gst_13
2. [TODO]

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}


-- depends_on: {{ ref('staging_au_catsales_stage_4') }}
{%- call statement('get_all', fetch_result=True) -%}
        WITH 
        Overall_13 AS 
        (SELECT   1 as ID
                ,SUM(ty_amt_inc_gst) all_amt_inc_gst_13
                ,SUM(ty_amt_exc_gst) all_amt_exc_gst_13
                ,SUM(ty_total_basket) all_total_basket_13
        FROM {{ ref('staging_au_catsales_stage_4') }} a
        WHERE weekno < 14
        ),
        Overall_4 AS 
        (SELECT  1 as ID
                ,SUM(ty_amt_inc_gst) all_amt_inc_gst_4
                ,SUM(ty_amt_exc_gst) all_amt_exc_gst_4
                ,SUM(ty_total_basket) all_total_basket_4
        FROM {{ ref('staging_au_catsales_stage_4') }} a
        WHERE weekno < 5
        )
        SELECT 
                a.all_amt_inc_gst_13
                ,a.all_amt_exc_gst_13
                ,a.all_total_basket_13

                ,b.all_amt_inc_gst_4
                ,b.all_amt_exc_gst_4
                ,b.all_total_basket_4
        FROM Overall_13 a
        JOIN Overall_4 b
                on a.ID = b.ID

{%- endcall -%}

{% if execute %}
        {%- set all_amt_inc_gst_13 = load_result('get_all').table.columns['all_amt_inc_gst_13'].values()[0] -%}
        {%- set all_amt_exc_gst_13 = load_result('get_all').table.columns['all_amt_exc_gst_13'].values()[0] -%}
        {%- set all_total_basket_13 = load_result('get_all').table.columns['all_total_basket_13'].values()[0] -%}

        {%- set all_amt_inc_gst_4 = load_result('get_all').table.columns['all_amt_inc_gst_4'].values()[0] -%}
        {%- set all_amt_exc_gst_4 = load_result('get_all').table.columns['all_amt_exc_gst_4'].values()[0] -%}
        {%- set all_total_basket_4 = load_result('get_all').table.columns['all_total_basket_4'].values()[0] -%}
{% endif %}



WITH 
subcategory_Sale_13 AS 
(
        SELECT  category
                ,subcategory
                ,SUM(ty_amt_exc_gst) ty_amt_exc_gst
                ,SUM(ty_amt_inc_gst) ty_amt_inc_gst
                ,SUM(ty_total_basket) ty_total_basket
        FROM  {{ ref('staging_au_catsales_stage_2') }} a
        WHERE weekno < 14
        GROUP BY category
                ,subcategory
),
category_Sale_13 AS 
(
        SELECT  category
                ,SUM(ty_amt_exc_gst) ty_amt_exc_gst
                ,SUM(ty_amt_inc_gst) ty_amt_inc_gst
                ,SUM(ty_total_basket) ty_total_basket
        FROM {{ ref('staging_au_catsales_stage_2') }} a
        WHERE weekno < 14
        GROUP BY category
),
subcategory_Sale_4 AS 
(
        SELECT  category
                ,subcategory
                ,SUM(ty_amt_exc_gst) ty_amt_exc_gst
                ,SUM(ty_amt_inc_gst) ty_amt_inc_gst
                ,SUM(ty_total_basket) ty_total_basket
        FROM  {{ ref('staging_au_catsales_stage_2') }} a
        WHERE weekno < 5
        GROUP BY category
                ,subcategory
),
category_Sale_4 AS 
(
        SELECT  category
                ,SUM(ty_amt_exc_gst) ty_amt_exc_gst
                ,SUM(ty_amt_inc_gst) ty_amt_inc_gst
                ,SUM(ty_total_basket) ty_total_basket
        FROM {{ ref('staging_au_catsales_stage_2') }} a
        WHERE weekno < 5
        GROUP BY category
),
overall_13  AS 
(
        SELECT 
                ty.brand,
                ty.category,
                ty.subcategory,
                ty.state,
                ty.channel_view,
                ty.lifestage_segment,
                ty.affluence,

                SUM(total_basket_sales_incl_gst) AS total_basket_sales_incl_gst, 
                SUM(total_basket_sales_excl_gst) AS total_basket_sales_excl_gst, 

                SUM(ty_units) AS ty_units, 
                SUM(ty_amt_inc_gst) AS ty_amt_exc_gst, 
                SUM(ty_amt_inc_gst) AS ty_amt_inc_gst, 
                SUM(ty_total_basket) AS ty_total_basket,

                SUM(ly_units) AS ly_units, 
                SUM(ly_amt_inc_gst) AS ly_amt_exc_gst, 
                SUM(ly_amt_inc_gst) AS ly_amt_inc_gst,

                SUM(lq_units) AS lq_units, 
                SUM(lq_amt_inc_gst) AS lq_amt_exc_gst, 
                SUM(lq_amt_inc_gst) AS lq_amt_inc_gst, 

                SUM(lh_units) AS lh_units, 
                SUM(lh_amt_inc_gst) AS lh_amt_exc_gst, 
                SUM(lh_amt_inc_gst) AS lh_amt_inc_gst
        FROM {{ ref('staging_au_catsales_stage_1') }} ty
        WHERE weekno < 14
        GROUP BY ty.brand,
                ty.category,
                ty.subcategory,
                ty.state,
                ty.channel_view,
                ty.lifestage_segment,
                ty.affluence
),
overall_4  AS 
(
        SELECT 
                ty.brand,
                ty.category,
                ty.subcategory,
                ty.state,
                ty.channel_view,
                ty.lifestage_segment,
                ty.affluence,

                SUM(total_basket_sales_incl_gst) AS total_basket_sales_incl_gst, 
                SUM(total_basket_sales_excl_gst) AS total_basket_sales_excl_gst, 

                SUM(ty_units) AS ty_units, 
                SUM(ty_amt_inc_gst) AS ty_amt_exc_gst, 
                SUM(ty_amt_inc_gst) AS ty_amt_inc_gst, 
                SUM(ty_total_basket) AS ty_total_basket,

                SUM(ly_units) AS ly_units, 
                SUM(ly_amt_inc_gst) AS ly_amt_exc_gst, 
                SUM(ly_amt_inc_gst) AS ly_amt_inc_gst,

                SUM(lq_units) AS lq_units, 
                SUM(lq_amt_inc_gst) AS lq_amt_exc_gst, 
                SUM(lq_amt_inc_gst) AS lq_amt_inc_gst, 

                SUM(lh_units) AS lh_units, 
                SUM(lh_amt_inc_gst) AS lh_amt_exc_gst, 
                SUM(lh_amt_inc_gst) AS lh_amt_inc_gst
        FROM {{ ref('staging_au_catsales_stage_1') }} ty
        WHERE weekno < 5
        GROUP BY ty.brand,
                ty.category,
                ty.subcategory,
                ty.state,
                ty.channel_view,
                ty.lifestage_segment,
                ty.affluence
)

SELECT          
                a13.brand,
                a13.category,
                a13.subcategory,
                a13.state,
                a13.channel_view,
                a13.lifestage_segment,
                a13.affluence,

                a13.total_basket_sales_incl_gst AS total_basket_sales_incl_gst_13, 
                a13.total_basket_sales_excl_gst AS total_basket_sales_excl_gst_13, 
                a13.ty_units AS ty_units_13,
                a13.ty_amt_exc_gst AS ty_amt_exc_gst_13,
                a13.ty_amt_inc_gst AS ty_amt_inc_gst_13,
                a13.ty_total_basket AS ty_total_basket_13,

                a13.ly_units AS ly_units_13,
                a13.ly_amt_exc_gst AS ly_amt_exc_gst_13,
                a13.ly_amt_inc_gst AS ly_amt_inc_gst_13,

                a13.lq_units AS lq_units_13,
                a13.lq_amt_inc_gst AS lq_amt_exc_gst_13,
                a13.lq_amt_inc_gst AS lq_amt_inc_gst_13,

                a13.lh_units AS lh_units_13,
                a13.lh_amt_inc_gst AS lh_amt_exc_gst_13,
                a13.lh_amt_inc_gst AS lh_amt_inc_gst_13,

                subcat13.ty_amt_inc_gst AS subcat_amt_exc_gst_13,
                subcat13.ty_amt_inc_gst AS subcat_amt_inc_gst_13,
                subcat13.ty_total_basket AS subcat_basket_13,

                CASE 
                        WHEN NULLIF(subcat13.ty_total_basket,0) IS NULL THEN 0
                        ELSE a13.ty_total_basket/(subcat13.ty_total_basket) 
                END AS subcat_penetration_13,
                
                CASE 
                        WHEN NULLIF(subcat13.ty_amt_exc_gst,0) IS NULL THEN 0
                        ELSE a13.ty_amt_exc_gst/(subcat13.ty_amt_exc_gst) 
                END AS subcat_market_share_exc_gst_13,
                
                CASE 
                        WHEN NULLIF(subcat13.ty_amt_inc_gst,0) IS NULL THEN 0
                        ELSE a13.ty_amt_inc_gst/(subcat13.ty_amt_inc_gst) 
                END AS subcat_market_share_inc_gst_13,

                cat13.ty_amt_exc_gst AS cat_amt_exc_gst_13,
                cat13.ty_amt_inc_gst AS cat_amt_inc_gst_13,
                cat13.ty_total_basket AS cat_basket_13,

                CASE 
                        WHEN NULLIF(cat13.ty_total_basket,0) IS NULL THEN 0
                        ELSE a13.ty_total_basket/(cat13.ty_total_basket) 
                END AS cat_penetration_13,

                CASE 
                WHEN NULLIF(cat13.ty_amt_inc_gst,0) IS NULL THEN 0
                ELSE a13.ty_amt_inc_gst/(cat13.ty_amt_inc_gst) 
                END AS cat_market_share_inc_gst_13,

                CASE 
                WHEN NULLIF(cat13.ty_amt_exc_gst,0) IS NULL THEN 0
                ELSE a13.ty_amt_exc_gst/(cat13.ty_amt_exc_gst)
                END AS cat_market_share_exc_gst_13,

                CAST('{{all_total_basket_13}}' AS FLOAT64)  AS brand_total_basket_13,

                a13.ty_total_basket/CAST('{{all_total_basket_13}}' AS FLOAT64)    AS brand_penetration_13,
                a13.ty_amt_inc_gst/CAST('{{all_amt_inc_gst_13}}' AS FLOAT64)      AS brand_market_share_inc_gst_13,
                a13.ty_amt_exc_gst/CAST('{{all_amt_exc_gst_13}}' AS FLOAT64)      AS brand_market_share_exc_gst_13,

----------

                a4.total_basket_sales_incl_gst AS total_basket_sales_incl_gst_4, 
                a4.total_basket_sales_excl_gst AS total_basket_sales_excl_gst_4, 
                a4.ty_units AS ty_units_4,
                a4.ty_amt_exc_gst AS ty_amt_exc_gst_4,
                a4.ty_amt_inc_gst AS ty_amt_inc_gst_4,
                a4.ty_total_basket AS ty_total_basket_4,


                a4.ly_units AS ly_units_4,
                a4.ly_amt_exc_gst AS ly_amt_exc_gst_4,
                a4.ly_amt_inc_gst AS ly_amt_inc_gst_4,

                a4.lq_units AS lq_units_4,
                a4.lq_amt_inc_gst AS lq_amt_exc_gst_4,
                a4.lq_amt_inc_gst AS lq_amt_inc_gst_4,

                a4.lh_units AS lh_units_4,
                a4.lh_amt_inc_gst AS lh_amt_exc_gst_4,
                a4.lh_amt_inc_gst AS lh_amt_inc_gst_4,


                subcat4.ty_amt_inc_gst AS subcat_amt_exc_gst_4,
                subcat4.ty_amt_inc_gst AS subcat_amt_inc_gst_4,
                subcat4.ty_total_basket AS subcat_basket_4,

                CASE 
                        WHEN NULLIF(subcat4.ty_total_basket,0) IS NULL THEN 0
                        ELSE a4.ty_total_basket/(subcat4.ty_total_basket) 
                END AS subcat_penetration_4,
                
                CASE 
                        WHEN NULLIF(subcat4.ty_amt_exc_gst,0) IS NULL THEN 0
                        ELSE a4.ty_amt_exc_gst/(subcat4.ty_amt_exc_gst) 
                END AS subcat_market_share_exc_gst_4,
                
                CASE 
                        WHEN NULLIF(subcat4.ty_amt_inc_gst,0) IS NULL THEN 0
                        ELSE a4.ty_amt_inc_gst/(subcat4.ty_amt_inc_gst) 
                END AS subcat_market_share_inc_gst_4,

                cat4.ty_amt_exc_gst AS cat_amt_exc_gst_4,
                cat4.ty_amt_inc_gst AS cat_amt_inc_gst_4,
                cat4.ty_total_basket AS cat_basket_4,

                CASE 
                        WHEN NULLIF(cat4.ty_total_basket,0) IS NULL THEN 0
                        ELSE a4.ty_total_basket/(cat4.ty_total_basket) 
                END AS cat_penetration_4,

                CASE 
                        WHEN NULLIF(cat4.ty_amt_inc_gst,0) IS NULL THEN 0
                        ELSE a4.ty_amt_inc_gst/(cat4.ty_amt_inc_gst) 
                END AS cat_market_share_inc_gst_4,

                CASE 
                        WHEN NULLIF(cat4.ty_amt_exc_gst,0) IS NULL THEN 0
                        ELSE a4.ty_amt_exc_gst/(cat4.ty_amt_exc_gst)
                END AS cat_market_share_exc_gst_4,

                CAST('{{all_total_basket_4}}' AS FLOAT64)  AS brand_total_basket_4,

                a4.ty_total_basket/CAST('{{all_total_basket_4}}' AS FLOAT64)    AS brand_penetration_4,
                a4.ty_amt_inc_gst/CAST('{{all_amt_inc_gst_4}}' AS FLOAT64)      AS brand_market_share_inc_gst_4,
                a4.ty_amt_exc_gst/CAST('{{all_amt_exc_gst_4}}' AS FLOAT64)      AS brand_market_share_exc_gst_4
FROM overall_13  a13
LEFT OUTER JOIN overall_4  a4
ON      a13.brand = a4.brand
        AND a13.category = a4.category
        AND a13.subcategory = a4.subcategory
        AND a13.state = a4.state
        AND a13.channel_view = a4.channel_view
        AND a13.lifestage_segment = a4.lifestage_segment
        AND a13.affluence = a4.affluence
JOIN  subcategory_Sale_13 subcat13
        ON  a13.subcategory = subcat13.subcategory
        AND a13.category = subcat13.category
JOIN category_Sale_13 cat13
        ON      a13.category = cat13.category
LEFT OUTER JOIN  subcategory_Sale_4 subcat4
        ON  a4.subcategory = subcat4.subcategory
        AND a4.category = subcat4.category
LEFT OUTER JOIN category_Sale_4 cat4
        ON  a4.category = cat4.category

```


---
## Model 13: staging_au_catsales_rank

Description: Get weekly brand $sale inc gst, $sales exc gst, rank and total brand # for last 13 weeks.

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}

SELECT 
      S.weekno,
      S.brand,
      S.ty_amt_inc_gst,
      S.ty_amt_exc_gst,
      CASE 
            WHEN  S.ty_amt_inc_gst IS NULL THEN 9999
            ELSE  DENSE_RANK() OVER (PARTITION BY S.weekno ORDER BY S.ty_amt_inc_gst DESC) 
      END  AS rank_inc_gst,
      CASE 
            WHEN  S.ty_amt_exc_gst IS NULL THEN 9999
            ELSE  DENSE_RANK() OVER (PARTITION BY S.weekno ORDER BY S.ty_amt_exc_gst DESC) 
      END  AS rank_exc_gst,
      tot.total_brand
  FROM (
        SELECT 
            weekno,
            brand,
            SUM(ty_amt_inc_gst) ty_amt_inc_gst,
            SUM(ty_amt_exc_gst) ty_amt_exc_gst
        FROM  {{ ref('staging_au_catsales_stage_1') }}   a
        GROUP BY weekno, brand
  ) S
  JOIN (
        SELECT 
            weekno,
            COUNT(DISTINCT brand) AS total_brand
        FROM {{ ref('staging_au_catsales_stage_1') }}   a
        WHERE IFNULL(ty_amt_inc_gst,0) > 0
        GROUP BY weekno
  ) tot
  ON S.weekno = tot.weekno
WHERE IFNULL(S.ty_amt_inc_gst,0) > 0


```


---
## Model 14: staging_au_catsales_category_rank

Description: Get weekly brand $sale inc gst, $sales exc gst, rank and total brand # for last 13 weeks in each category.

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}

SELECT 
      S.weekno, 
      S.category ,
      S.brand,
      S.ty_amt_exc_gst,
      S.ty_amt_inc_gst,
      CASE 
            WHEN  S.ty_amt_inc_gst IS NULL THEN 9999
            ELSE  DENSE_RANK() OVER (PARTITION BY S.weekno,S.category ORDER BY S.ty_amt_inc_gst DESC) 
      END  AS rank_inc_gst,
      CASE 
            WHEN  S.ty_amt_exc_gst IS NULL THEN 9999
            ELSE  DENSE_RANK() OVER (PARTITION BY S.weekno,S.category ORDER BY S.ty_amt_exc_gst DESC) 
      END  AS rank_exc_gst,
      tot.total_brand
FROM (
      SELECT 
            weekno, 
            category ,
            brand, 
            SUM(ty_amt_exc_gst) ty_amt_exc_gst,
            SUM(ty_amt_inc_gst) ty_amt_inc_gst
      FROM {{ ref('staging_au_catsales_stage_1') }} a
      GROUP BY weekno, category ,brand
) S
JOIN (
      SELECT weekno,category,COUNT(distinct brand) AS total_brand
      FROM {{ ref('staging_au_catsales_stage_1') }} a
      WHERE IFNULL(ty_amt_inc_gst,0) > 0
      GROUP BY weekno,category
) tot
ON S.weekno = tot.weekno
AND S.category = tot.category
WHERE IFNULL(S.ty_amt_inc_gst,0) > 0


```

---
## Model 15: staging_au_catsales_subcategory_rank

Description: Get weekly brand $sale inc gst, $sales exc gst, rank and total brand # for last 13 weeks in each category, subcategory.

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}

SELECT 
      S.weekno, 
      S.subcategory ,
      S.category ,
      S.brand,
      S.ty_amt_inc_gst,
      S.ty_amt_exc_gst,
      CASE 
            WHEN  S.ty_amt_inc_gst IS NULL THEN 9999
            ELSE  DENSE_RANK() OVER (PARTITION BY S.weekno,S.category, S.subcategory ORDER BY S.ty_amt_inc_gst DESC) 
      END  AS rank_inc_gst,
      CASE 
            WHEN  S.ty_amt_exc_gst IS NULL THEN 9999
            ELSE  DENSE_RANK() OVER (PARTITION BY S.weekno,S.category, S.subcategory ORDER BY S.ty_amt_exc_gst DESC) 
      END  AS rank_exc_gst,
      tot.total_brand
  FROM (
        SELECT 
            weekno, 
            category, 
            subcategory,
            brand,
            SUM(ty_amt_inc_gst) ty_amt_inc_gst,
            SUM(ty_amt_exc_gst) ty_amt_exc_gst
        FROM  {{ ref('staging_au_catsales_stage_1') }}   a
        GROUP BY weekno, category, subcategory ,brand
  ) S
  JOIN (
        SELECT 
            weekno,
            category,
            subcategory,
            COUNT(DISTINCT brand) AS total_brand
        FROM {{ ref('staging_au_catsales_stage_1') }}   a
        WHERE IFNULL(ty_amt_inc_gst,0) > 0
        GROUP BY weekno,category, subcategory
  ) tot
  ON S.weekno = tot.weekno
  AND S.subcategory = tot.subcategory
  AND S.category = tot.category
WHERE IFNULL(S.ty_amt_inc_gst,0) > 0




```



---
## Model 16: fact_au_catsales

Description: This table contains data for sales transactions brand/catgeory/subcategory that occur each week [TODO]

Dependencies: [list any models or sources the model depends on/ upstream models or sources]

SQL Code:
```sql
{{ config(
  tags= ["au_catsales"]
)
}}


SELECT          ty_startdate AS ty_startdate,
                ty_enddate AS ty_enddate,

                ly_startdate AS ly_startdate,
                ly_enddate AS ly_enddate,

                lq_startdate AS lq_startdate,
                lq_enddate AS lq_enddate,

                lh_startdate AS lh_startdate,
                lh_enddate AS lh_enddate,

                LPAD(CAST(a.weekno AS STRING), 2, '0') AS weekno,
                a.brand,
                a.category,
                a.subcategory,
                a.state,
                a.channel_view,
                a.lifestage_segment,
                a.affluence,

                a.ty_units,
                a.ty_amt_exc_gst,
                a.ty_amt_inc_gst,
                a.ty_total_basket,

                a.total_basket_sales_incl_gst, 
                a.total_basket_sales_excl_gst,

                a.ly_units,
                a.ly_amt_exc_gst,
                a.ly_amt_inc_gst,
                a.ly_total_basket,

                a.lh_units,
                a.lh_amt_exc_gst,
                a.lh_amt_inc_gst,
                a.lh_total_basket,

                a.lq_units,
                a.lq_amt_exc_gst,
                a.lq_amt_inc_gst,
                a.lq_total_basket,

                subcat.ty_amt_inc_gst AS subcat_amt_inc_gst,
                subcat.ty_amt_exc_gst AS subcat_amt_exc_gst,
                subcat.ty_total_basket AS subcat_basket,

                CASE 
                        when NULLIF(subcat.ty_total_basket,0) is null then 0 
                        else a.ty_total_basket/(subcat.ty_total_basket)
                END AS subcat_penetration,

                CASE 
                        when NULLIF(subcat.ty_amt_inc_gst,0) is null then 0 
                        else a.ty_amt_inc_gst/(subcat.ty_amt_inc_gst)
                END AS subcat_market_share_inc_gst,

                CASE 
                        when NULLIF(subcat.ty_amt_exc_gst,0) is null then 0 
                        else a.ty_amt_exc_gst/(subcat.ty_amt_exc_gst)
                END AS subcat_market_share_exc_gst,

                cat.ty_amt_inc_gst AS cat_amt_inc_gst,
                cat.ty_amt_exc_gst AS cat_amt_exc_gst,
                cat.ty_total_basket AS cat_basket,

                CASE 
                        when NULLIF(cat.ty_total_basket,0) is null then 0 
                        else a.ty_total_basket/(cat.ty_total_basket)
                END AS cat_penetration,

                CASE 
                        when NULLIF(cat.ty_amt_inc_gst,0) is null then 0 
                        else a.ty_amt_inc_gst/(cat.ty_amt_inc_gst)
                END AS cat_market_share_inc_gst,

                CASE 
                        when NULLIF(cat.ty_amt_exc_gst,0) is null then 0 
                        else a.ty_amt_exc_gst/(cat.ty_amt_exc_gst)
                END AS cat_market_share_exc_gst,

                b.ty_amt_inc_gst AS brand_amt_inc_gst,
                b.ty_amt_exc_gst AS brand_amt_exc_gst,
                b.ty_total_basket AS brand_basket,

                CASE 
                        when NULLIF(b.ty_total_basket,0) is null then 0 
                        else a.ty_total_basket/(b.ty_total_basket)
                END AS brand_penetration,

                CASE 
                        when NULLIF(b.ty_amt_inc_gst,0) is null then 0 
                        else a.ty_amt_inc_gst/(b.ty_amt_inc_gst)
                END AS brand_market_share_inc_gst,

                CASE 
                        when NULLIF(b.ty_amt_exc_gst,0) is null then 0 
                        else a.ty_amt_exc_gst/(b.ty_amt_exc_gst)
                END AS brand_market_share_exc_gst,

                subcat_r.rank_inc_gst AS subcat_rank_inc_gst,
                subcat_r.rank_exc_gst AS subcat_rank_exc_gst,
                subcat_r.total_brand AS subcat_brand_count,

                cat_r.rank_exc_gst AS cat_rank_exc_gst,
                cat_r.rank_inc_gst AS cat_rank_inc_gst,
                cat_r.total_brand AS cat_brand_count,

                r.rank_exc_gst AS brand_rank_exc_gst,
                r.rank_inc_gst AS brand_rank_inc_gst,
                r.total_brand AS brand_count

FROM {{ ref('staging_au_catsales_stage_1') }}   a 
JOIN {{ ref('staging_au_catsales_stage_2') }} subcat
        on      a.subcategory = subcat.subcategory
                AND a.weekno = subcat.weekno
JOIN  {{ ref('staging_au_catsales_subcategory_rank') }} subcat_r
         on a.subcategory = subcat_r.subcategory
         AND a.category = subcat_r.category
         AND a.brand = subcat_r.brand
         AND a.weekno = subcat_r.weekno
JOIN  {{ ref('staging_au_catsales_category_rank') }}  cat_r
         on a.category = cat_r.category
         AND a.brand = cat_r.brand
        AND a.weekno = cat_r.weekno
JOIN {{ ref('staging_au_catsales_stage_3') }} cat
        on      a.category = cat.category
                AND a.weekno = cat.weekno
JOIN {{ ref('staging_au_catsales_stage_4') }} b
        on      a.weekno = b.weekno
JOIN {{ ref('staging_au_catsales_rank') }} r
        on      a.weekno = r.weekno
        and     a.brand = r.brand
        
```