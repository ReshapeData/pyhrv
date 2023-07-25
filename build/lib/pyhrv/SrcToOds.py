#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrda.dbms.rds import RdClient


def main(FToken):
    '''
    src到ods同步
    :param FToken:
    :return:
    '''
    app2 = RdClient(FToken)

    sql = """
    --研发工时_研发类数据源  SRC到 ODS同步
insert into   rds_hrv_ods_ds_rddetail
SELECT [FNO]
      ,[FSalaryType]
      ,[FYear]
      ,[FMonth]
      ,[FOldDept]
      ,[FHightechDept]
      ,[FStaffName]
      ,[FExpenseOrgID]
      ,[FTaxDeclarationOrg]
      ,[FNumber]
      ,[FRdProject]
      ,[FRdProjectCost]
  FROM rds_hrv_src_ds_rddetail  A
   where  A.FNumber  NOT  IN 
   (SELECT FNumber FROM rds_hrv_ods_ds_rddetail)

  --非研发工时_研发类数据源  SRC到 ODS同步
insert into   rds_hrv_ods_ds_nonrddetail
  SELECT [FNO]
      ,[FSalaryType]
      ,[FYear]
      ,[FMonth]
      ,[FOldDept]
      ,[FHightechDept]
      ,[FStaffName]
      ,[FExpenseOrgID]
      ,[FTaxDeclarationOrg]
      ,[FNumber]
      ,[FNonRdCost]
      ,[FRdProject]
  FROM rds_hrv_src_ds_nonrddetail A
  where  A.FNumber  NOT  IN 
   (SELECT FNumber FROM rds_hrv_ods_ds_nonrddetail)

  ---工资模板表 SRC到 ODS同步
insert into   rds_hrv_ods_ds_salary
  SELECT [FExpenseOrgID]
      ,[FTaxDeclarationOrg]
      ,[FBankType]
      ,[FAccount]
      ,[FHightechDept]
      ,[FRdProject]
      ,[FCpayAmount]
      ,[FFixdCost]
      ,[FScraprateCost]
      ,[FSocialSecurityAmt]
      ,[FAccumulationFundAmt]
      ,[FOtherAmt]
      ,[FIncomeTaxAmt]
      ,[FActualAmount]
      ,[FYear]
      ,[FMonth]
      ,[FVoucher]
      ,[FCategoryType]
      ,[FNumber]
      ,[FSeq]
      ,[FDate]
      ,[FOldDept]
      ,FNotePeriod
  FROM rds_hrv_src_ds_salary  A
  where  A.FNumber  NOT  IN 
   (SELECT FNumber FROM rds_hrv_ods_ds_salary)

  ---社保公积金  SRC到 ODS同步
insert into rds_hrv_ods_ds_socialsecurity
  SELECT [FExpenseOrgID]
      ,[FTaxDeclarationOrg]
      ,[FBankType]
      ,[FHightechDept]
      ,[FAccount]
      ,[FRdProject]
      ,[FComPensionBenefitsAmt]
      ,[FComMedicareAmt]
      ,[FComMedicareOfSeriousAmt]
      ,[FComDisabilityBenefitsAmt]
      ,[FComOffsiteElseAmt]
      ,[FComWorklessInsuranceAmt]
      ,[FComInjuryInsuranceAmt]
      ,[FComMaternityInsuranceAmt]
      ,[FComAllSocialSecurityAmt]
      ,[FComAccumulationFundAmt]
      ,[FComAllSoSeAcFuAmt]
      ,[FEmpPensionBenefitsAmt]
      ,[FEmpMedicareAmt]
      ,[FEmpMedicareOfSeriousAmt]
      ,[FEmpWorklessInsuranceAmt]
      ,[FEmpAllSocialSecurityAmt]
      ,[FEmpAccumulationFundAmt]
      ,[FEmpAllSoSeAcFuAmt]
      ,[FAllSocialSecurityAmt]
      ,[FAllAccumulationFundAmt]
      ,[FAllAmount]
      ,[FManagementAmount]
      ,[FYear]
      ,[FMonth]
      ,[FVoucher]
      ,[FCategoryType]
      ,[FNumber]
      ,[FSeq]
      ,[FDate]
      ,[FOldDept]
      ,FNotePeriod
  FROM rds_hrv_src_ds_socialsecurity   A
  where  A.FNumber  NOT  IN 
   (SELECT FNumber FROM rds_hrv_ods_ds_socialsecurity)


  -- 科目 SRC到 ODS同步
insert  into rds_hrv_ods_md_acct
  SELECT [FAccountNumber]
      ,[FAccountName]
      ,[FAccountNameComplete]
      ,[FBalanceType]
      ,[FAccountType]
      ,[FCurrencyTranslation]
      ,[FLexitemProperty]
      ,[FStatus]
      ,[FFirstAcct]
  FROM rds_hrv_src_md_acct  A
  WHERE A.FAccountNumber  NOT IN
  (SELECT FAccountNumber FROM rds_hrv_ods_md_acct)
  --摘要  SRC到 ODS同步
insert  into   rds_hrv_ods_md_fnote
  SELECT [FOrgType]
      ,[FCategoryType]
      ,[Fexample]
  FROM rds_hrv_src_md_fnote  A
  where not exists
  (SELECT * FROM rds_hrv_src_md_fnote  B
  WHERE A.FOrgType =B.FOrgType
  AND A.FCategoryType=B.FCategoryType
  AND A.Fexample =B.Fexample  )

 --核算维度-部门对照 SRC到 ODS同步
insert  into   rds_hrv_ods_md_dept
  SELECT [FDepNameManual]
      ,[FNumber]
      ,[FDepName]
      ,[FDepNameComplete]
      ,[FUserOrg]
      ,[FNotes]
  FROM rds_hrv_src_md_dept  A
   WHERE A.FNumber  NOT IN
  (SELECT FNumber FROM rds_hrv_ods_md_dept)

 --核算维度-重分类  SRC到 ODS同步
insert  into  rds_hrv_ods_md_acctreclass
  SELECT [FNumber]
        ,[FAccountItemActual]
        ,[FAccountItem]
  FROM rds_hrv_src_md_acctreclass  A
    WHERE A.FNumber  NOT IN
  (SELECT FNumber FROM rds_hrv_ods_md_acctreclass)

 --核算维度-责任中心  SRC到 ODS同步
insert  into  rds_hrv_ods_md_workcenter
  SELECT [FNumber]
      ,[FDept]
  FROM rds_hrv_src_md_workcenter  A
    WHERE A.FNumber  NOT IN
  (SELECT FNumber FROM rds_hrv_ods_md_workcenter)

 --研发项目对照  SRC到 ODS同步
insert into rds_hrv_ods_md_rditem
 SELECT  [FOrg]
      ,[FRDProjectManual]
      ,[FRDProject]
  FROM rds_hrv_src_md_rditem  A
  WHERE NOT EXISTS 
  (SELECT * FROM  rds_hrv_src_md_rditem  B
  WHERE A.FOrg = B.FOrg
  AND A.FRDProject = B.FRDProject
  AND A.FRDProjectManual = B.FRDProjectManual)


 --凭证模板  SRC到 ODS同步
insert  into rds_hrv_ods_tpl_voucher
  SELECT [FNumber]
      ,[FName]
      ,[FCategoryType]
      ,[FSeq]
      ,[FNotes]
      ,[FSubjectNumber]
      ,[FSubjectName]
      ,[FAccountNumber]
      ,[FLexitemProperty]
      ,[FObtainSource]
      ,[FAccountBorrow]
      ,[FAccountLoan]
      ,[FSettleMethod]
      ,[FSettleNumber]
      ,[FAccountBookID]
      ,[FFirstAcct]
      ,[FAccountBorrowSql]
      ,[FAccountLoanSql]
  FROM rds_hrv_src_tpl_voucher
  WHERE FNumber NOT IN
  (SELECT FNumber FROM rds_hrv_ods_tpl_voucher)
 

 --规则表  SRC到 ODS同步

insert into  rds_hrv_ods_rule_voucher
  SELECT [FNumber]
      ,[FName]
      ,[FExpenseOrgID]
      ,[FTaxDeclarationOrg]
      ,[FBankType]
      ,[FCategoryType]
  FROM rds_hrv_src_rule_voucher
  WHERE FNumber NOT  IN 
  (SELECT FNumber FROM rds_hrv_ods_rule_voucher)
    """

    app2.update(sql)

