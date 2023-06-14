DELETE FROM   DBT_BICC_CTCT.WK_BOTCAPITALIZE_INTERFACE ALL;

LOCKING TABLE DBT_CW1_ST_PRD.JOBHEADER FOR ACCESS
LOCKING TABLE DBT_CW1_ST_PRD.GLBCOMPANY FOR ACCESS
LOCKING TABLE DBT_CW1_ST_PRD.GLBBRANCH FOR ACCESS
INSERT INTO DBT_BICC_CTCT.WK_BOTCAPITALIZE_INTERFACE
            ( 
                        job_num, 
                        dgf_company, 
                        job_branch, 
                        job_department 
            ) 

SELECT   jh_jobnum, 
         gc.gc_code, 
         gb.gb_code, 
         Substr(orig_prod_cd,1,3) job_department 
FROM     DBT_CW1_ST_PRD.JOBHEADER JH 

JOIN     DBT_CW1_ST_PRD.GLBCOMPANY GC 
ON       ( 
                  jh.jh_gc = gc.gc_pk) 

JOIN     DBT_CW1_ST_PRD.GLBBRANCH GB 
ON       ( 
                  jh.jh_gb=gb.gb_pk) 

JOIN     DB_DVAULT_FWN_PRD.V_HB_FACT_OP op 
ON       jh.jh_jobnum=op.job_num 
AND      gc.gc_code=op.company_cd 

JOIN     DB_DVAULT_FWN_PRD.V_DIM_BRANCH_PNL pnl 
ON       op.pnl_brnloc_sid=pnl.pnl_branch_sid 
AND      gb.gb_code=pnl.pnl_branch_cd 

WHERE    jh_status<> UPPER(jh_status) (CASESPECIFIC) 
AND      UPPER(jh_status) IN('CLS',  'CMP') AND  CAST( jh_a_jop AS DATE)>='2020-11-01'
QUALIFY ROW_NUMBER() OVER (PARTITION BY gc_code, gb_code, jh_jobnum ORDER BY 1 DESC)=1;

SELECT dgf_company "DGF_Company", 
       job_num "Job_Num", 
       job_branch "Job_Branch", 
       job_department "Job_Department" 
FROM   DBT_BICC_CTCT.WK_BOTCAPITALIZE_INTERFACE; 
