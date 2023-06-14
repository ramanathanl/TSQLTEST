--Changed Name
DELETE FROM DBT_BICC_CTCT.WK_BOT_STALE6M_INTERFACE ALL;

INSERT INTO  DBT_BICC_CTCT.WK_BOT_STALE6M_INTERFACE
     SELECT  rc.rn_regionname  "Region_Name",
             rc.rn_subregionname "Subregion_Name",
             "DGF Company"    "DGF_Company",
             "Job Branch"    "Job_Branch",
             "Job Dept"       "Job_Dept",
	     "Job Open Date"  "Job Open Date",
             "Job Open Month YYYYMM"  "Job_Open_Month_YYYYMM",
	     "Job Recog Day YYYYMMDD" "Job Recog Day YYYYMMDD",
             SUBSTR("Job Recog Day YYYYMMDD", 1,6) "Job_Recog_Month_YYYYMM",
             "Job Num"    "Job_Num",
             cn.currency_cd "Rep_Currency",
             COALESCE(ROUND(SUM("WIP Amount") OVER (PARTITION BY  "DGF Company",  "Job Num"), 3), 0)   "WIP_Amount_Total",
             COALESCE(ROUND(SUM("ACR Amount") OVER (PARTITION BY  "DGF Company",  "Job Num"), 3), 0)   "ACR_Amount_Total",
             COALESCE(ROUND(SUM("WIP Amount EUR")  OVER (PARTITION BY  "DGF Company",  "Job Num"), 3), 0)  "WIP_Amount_Total_NER",
             COALESCE(ROUND(SUM("ACR Amount EUR")  OVER (PARTITION BY  "DGF Company",  "Job Num") ,3), 0) "ACR_Amount_Total_NER",
             jh_status_cd, 
	     CURRENT_DATE, 
	     'R'
     FROM DB_CW1_OPREP_PRD.V_SICKFILE_KPI al
     LEFT JOIN DBT_DVAULT_ADJ_PRD.REF_FWN_COUNTRY cn
            ON SUBSTR("DGF Company", 1,2)=cn.country_cd
	  JOIN DBT_DVAULT_PRD.REF_COUNTRY rc
			ON rc.rn_subregion_cd =cn.sub_region_cd    			
    WHERE  CURRENT_DATE > CAST ( "Job Recog Day YYYYMMDD"  AS DATE FORMAT 'YYYYMMDD')  + INTERVAL '180' DAY -- added only not to forget
           AND "Job Recog Indicator" NOT LIKE('%Not Recognized%')
           AND jh_status_cd NOT IN('WHL', 'IHL', 'CLS', 'JRC')
           AND  (SUBSTR("Job Num", 1, 2)='WI'  OR SUBSTR("Job Num", 1, 1)='B' OR SUBSTR("Job Num", 1, 1)='S' )
           AND EXISTS (SELECT 1 FROM  DB_CW1_ST_PRD.V_LOAD_CW1_JOBHEADER jh   --check whether data is most recent
                                 JOIN DB_CW1_ST_PRD.V_LOAD_CW1_GLBCOMPANY gc 
                                   ON jh.jh_gc=gc.gc_pk
                                  AND jh.del_process_id IS NULL
                                 JOIN DB_CW1_ST_PRD.V_LOAD_CW1_GLBBRANCH gb 
                                   ON jh.jh_gb=gb.gb_pk 
                        WHERE      al."Job Num"=jh.jh_jobnum
                                  AND al."DGF Company"=gc.gc_code
                                  AND al."Job Branch"=gb.gb_code
                                  AND al.jh_status_cd=jh.jh_status) 
            AND NOT EXISTS( SELECT 1
                          FROM  DB_DVAULT_FWN_PRD.V_HB_FACT_OP op 
                           JOIN DB_DVAULT_FWN_PRD.V_DIM_BRANCH_PNL pnl 
                             ON op.pnl_brnloc_sid = pnl.pnl_branch_sid  
                            AND al."Job Branch"=pnl.pnl_branch_cd
                           JOIN DB_DVAULT_FWN_PRD.V_HB_FACT_FIN_SUM su 
                             ON op.fwnjob_sid = su.fwnjob_sid 	
			  WHERE  al."Job Num"=op.job_num 
                            AND al."DGF Company"=op.company_cd
			    AND CURRENT_DATE<=acct_dt+ INTERVAL '28' DAY)
           AND al.del_process_id IS NULL
           AND NOT EXISTS (SELECT 1 
                   FROM DB_CW1_ST_PRD.V_LOAD_CW1_GENCUSTOMADDONVALUE GEN
                         JOIN DBT_BICC_CTCT.WK_BOT_SERVICECODEMAPPING SC
	                   ON sc.ServiceCode=GEN.xv_data
                         JOIN DB_CW1_ST_PRD.V_LOAD_CW1_JOBHEADER jh
                           ON GEN.xv_parentid=jh.jh_parentid
                         JOIN DB_CW1_ST_PRD.V_LOAD_CW1_GLBCOMPANY gc 
                           ON jh.jh_gc=gc.gc_pk
                          AND jh.del_process_id IS NULL
                WHERE      al."Job Num"=jh.jh_jobnum
                          AND al."DGF Company"=gc.gc_code
                         AND al.jh_status_cd=jh.jh_status)
QUALIFY ROW_NUMBER() OVER(PARTITION BY "DGF_Company", "Job_Num", "Job_Branch", "Job Dept" ORDER BY 1) = 1
        AND (ABS(WIP_Amount_Total_NER)=0 AND ABS(ACR_Amount_Total_NER)=0);

