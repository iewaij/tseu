SELECT
    gvkey,
    LEAST(datadate + '3 months'::INTERVAL, pdate + '2 days'::INTERVAL)::DATE AS date,
    loc,
    sich AS sic,
    LEFT(to_char(sich, '9999'), 3) AS sic_2,
    -- Assets
    rect,act,che,ch,ivst,ppegt,invt,aco,intan,ao,ppent,gdwl,icapt,ivaeq,ivao,mib,mibn,mibt,at AS att,
    -- Liabilities
    lse,lct,dlc,dltt,dltr,dltis,dlcch,ap,lco,lo,txdi,lt as ltt,
    -- Equities and Others
    teq,seq,ceq,pstk,emp,
    -- Income Statement
    sale,revt,cogs,xsga,dp,xrd,ib,ebitda,ebit,nopi,spi,pi,txp,nicon,txt,xint,dvc,dvt,sstk,
    -- Cash Flow Statement and Others
    capx,oancf,fincf,ivncf,prstkc,dv
FROM
    comp.g_funda
WHERE
    exchg = ANY (ARRAY [104, 107, 132, 151, 154, 171, 192, 194, 201, 209, 256, 257, 273, 276, 286])
    AND curcd = 'EUR'
    AND datafmt = 'HIST_STD'
    AND consol = 'C'
    AND datadate >= '1999-01-01'
    AND at IS NOT NULL
    AND nicon IS NOT NULL
ORDER BY
    gvkey,
    datadate;
