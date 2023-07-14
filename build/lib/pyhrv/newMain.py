#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrda.dbms.rds import RdClient
import pandas as pd
import math
import numpy as np


def voucher_query(app, FNumber):
    '''
    凭证模板表与科目表结合
    :param app:
    :param FNumber:
    :return:
    '''

    sql = f"""
        select a.FNumber,a.FName,a.FCategoryType,a.FSeq,a.FNotes,
        a.FSubjectNumber,a.FSubjectName,a.FAccountNumber,a.FLexitemProperty,
        a.FObtainSource,a.FAccountBorrow,a.FAccountLoan,a.FAccountBorrowSql,FAccountLoanSql,
        a.FSettleMethod,a.FSettleNumber,a.FAccountBookID,a.FFirstAcct,
        b.FFirstAcct as FAccount,b.FLexitemProperty,b.FAccountName
        from rds_hrv_src_tpl_voucher a
        inner join rds_hrv_src_md_acct b
        on a.FSubjectNumber=b.FAccountNumber 
        where a.FNumber = '{FNumber}'
        order by FSeq asc
    """

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def salaryBill_getRuleVars(app, FBillNumber):
    '''
    通过单据编号从数据源获取规则变量：单据编号，费用承担组织，个税申报组织，银行，业务类型
    :param FToken: 口令
    :param FBillNumber: 单据编号
    :return: 单据编号，费用承担组织，个税申报组织，银行，业务类型
    '''
    sql = f"""select FNumber, FExpenseOrgID, FTaxDeclarationOrg, FBankType, FCategoryType from rds_hrv_src_ds_salary where FNumber = '{FBillNumber}'"""

    res = app.select(sql)
    return res


def socialsecurityBill_getRuleVars(app, FBillNumber):
    '''
    通过单据编号从数据源获取规则变量：单据编号，费用承担组织，个税申报组织，银行，业务类型
    :param FToken:
    :param FBillNumber:
    :return: dataframe
    '''
    sql = f"""select FNumber, FExpenseOrgID, FTaxDeclarationOrg, FBankType, FCategoryType from rds_hrv_src_ds_socialsecurity where FNumber = '{FBillNumber}'"""

    res = app.select(sql)
    return res


def voucherRule_query(app, FExpenseOrgID, FTaxDeclarationOrg, FBankType, FCategoryType):
    '''
    规则表查询：根据任务单据的  费用承担组织，个税申报组织，银行，业务类型  获取凭证模版序号
    :param FToken: 口令
    :param FExpenseOrgID: 费用承担组织
    :param FTaxDeclarationOrg: 个税申报组织
    :param FBankType: 银行
    :param FCategoryType: 业务类型
    :return:凭证模版序号
    '''

    sql = f"""select FNumber from rds_hrv_src_rule_voucher where  FExpenseOrgID = '{FExpenseOrgID}' and 
    FTaxDeclarationOrg = '{FTaxDeclarationOrg}' and FBankType = '{FBankType}' and FCategoryType = '{FCategoryType}'"""

    res = app.select(sql)
    return res


def salaryBill_query(app, FNumber):
    '''
    通过单据编号获取工资数据
    :param FToken: token
    :param FNumber: 单据编号
    :return: 工资数据源
    '''

    sql = f"""select * from rds_hrv_ods_ds_salary_bak_20230526 where FNumber = '{FNumber}'"""

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def socialsecurityBill_query(app, FNumber):
    '''
    通过单据编号，从社保公积金表，获取数据
    :param FToken: token
    :param FNumber: 单据编号
    :return: 单据数据
    '''

    sql = f"""select * from rds_hrv_src_ds_socialsecurity_bak_20230526 where FNumber = '{FNumber}'"""

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def dept_query(app):
    '''
    部门查询
    :param FToken: token
    :param FName: 部门名称
    :return: 返回值
    '''

    sql = f"""select * from rds_hrv_src_md_dept """

    data = app.select(sql)

    res = pd.DataFrame(data)

    return res


def acctreclass_query(app):
    '''
    重分类查询
    :param FToken: token
    :param FAccountItemActual: 实际费用类别
    :return: 返回值
    '''

    sql = f"""select * from rds_hrv_src_md_acctreclass"""

    data = app.select(sql)

    res = pd.DataFrame(data)

    return res


def workcenter_query(app):
    '''
    责任中心查询
    :param FToken: token
    :param FDept: 部门名称
    :return: 重分类编码
    '''

    sql = f"""select * from rds_hrv_src_md_workcenter"""
    data = app.select(sql)
    res = pd.DataFrame(data)
    return res


def rditem_query(app):
    '''
    研发项目查询
    :param FToken: token
    :param FOldDept:原部门
    :param FHightechDept:新部门
    :param FRDProjectManual:研发项目
    :return: 返回值
    '''

    sql = f"""select * from rds_hrv_src_ds_detail"""
    data = app.select(sql)
    res = pd.DataFrame(data)
    return res


def acct_query(app):
    '''
    研发项目查询
    :param FToken: token
    :param FOldDept:原部门
    :param FHightechDept:新部门
    :param FRDProjectManual:研发项目
    :return: 返回值
    '''

    sql = f"""select * from rds_hrv_src_md_acct"""
    data = app.select(sql)
    res = pd.DataFrame(data)
    return res


def project_query(app):
    '''
    项目对照查询
    :param df:
    :return:
    '''

    sql = "select * from rds_hrv_src_md_rditem"

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def lb(x):
    '''
    摘要替换辅助函数
    :param x:
    :return:
    '''

    return x['FNotes'].replace('{会计期间}', str(int(x["FMonth"])) if x['FMonth'] != '' else "").replace('{会计年度}', str(
        int(x['FYear'])) if x['FYear'] != '' else "").replace('{部门}', str(x['FHightechDept']))


def notes_repalce(df):
    '''
    摘要替换
    :param df:
    :return:
    '''

    res = df.apply(lb, axis=1)

    df["FNotes"] = res

    return df


def deptName_repalce(FName):
    '''
    部门名字替换
    :param FName:
    :return:
    '''

    dept = {
        '工艺研发部': "工程设备部",
        '国际业务部': "外贸销售部",
        '国内业务部': "内贸销售部",
        '商务部': "商务支持部",
        '行政人资部': "人事行政部",
    }

    if FName in dept:
        FName = dept[FName]

    return FName


def dept_replace(df):
    '''
    部门替换
    :param df:
    :return:
    '''
    # deptdf

    for i in df.index:
        deptName = df.loc[i]["FHightechDept"]

        deptNumber = (deptdf[deptdf["FDepName"] == deptName_repalce(deptName)]).iloc[0]["FNumber"]

        df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FDeptNumber"] = deptNumber

    return df


def acctreclass_replace(df):
    '''
    重分类替换
    :param df:
    :return:
    '''
    FAccountName = voucherTpldf.iloc[0]["FAccountName"]

    acctreclass = (acctreclassdf[acctreclassdf["FAccountItem"] == FAccountName]).iloc[0]["FNumber"]

    df.loc[df.index.tolist(), "FAcctreClassNumber"] = acctreclass

    return df


def workcenter_repalce(df):
    '''
    责任中心替换
    :param df:
    :return:
    '''

    for i in df.index:

        deptName = df.loc[i]["FHightechDept"]

        if deptName != "研发部":
            deptNumber = (workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptName)]).iloc[0]["FNumber"]

            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterNumber"] = deptNumber

    return df


def rditem_repalce(df):
    '''
    研发项目
    :param df:
    :return:
    '''

    for i in df.index:

        if df.loc[i]["FRdProject"] != '':
            projectNumbercode = df.loc[i]["FRdProject"]

            print(projectNumbercode)

            reProjectNumbercode="-".join(projectNumbercode.split("_"))

            projectNumber = (projectdf[projectdf["FRDProjectManual"] == reProjectNumbercode]).iloc[0]["FRDProject"]

            df.loc[df[df["FRdProject"] == projectNumbercode].index.tolist(), "FProjectNumber"] = projectNumber

    return df


def subfunction1(df, acct, fAccountBorrowSql, fAccountLoanSql, fSeq):
    '''
    ['部门', '责任中心', '重分类']
    :return:
    '''

    rename=""

    BorrowLoan = ""

    if fAccountBorrowSql != "":
        BorrowLoan = fAccountBorrowSql.split(".")[1]

        rename = "allamountBorrow"

    if fAccountLoanSql != "":
        BorrowLoan = fAccountLoanSql.split(".")[1]

        rename = "allamountLoan"

    datadf = df[df["FAccount"] == acct][
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FRdProject", "FYear", "FMonth",
         "FAccount",BorrowLoan]]

    newdf = pd.DataFrame(columns=["FDeptNumber", "FWorkCenterNumber", "FAcctreClassNumber"])

    res = pd.concat([datadf, newdf])

    res["FSeq"] = fSeq

    res.rename(columns={BorrowLoan: rename}, inplace=True)


    deptafter = dept_replace(res)

    workcenterdf = workcenter_repalce(deptafter)

    res = acctreclass_replace(workcenterdf)

    return res


def subfunction2(df, acct, fAccountBorrowSql, fAccountLoanSql, fSeq):
    '''
    ['部门']
    :return:
    '''

    rename=""

    BorrowLoan = ""

    if fAccountBorrowSql != "":
        BorrowLoan = fAccountBorrowSql.split(".")[1]

        rename = "allamountBorrow"

    if fAccountLoanSql != "":
        BorrowLoan = fAccountLoanSql.split(".")[1]

        rename = "allamountLoan"

    datadf = df[df["FAccount"] == acct][
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FRdProject", "FYear", "FMonth",
         "FAccount",BorrowLoan]]

    newdf = pd.DataFrame(columns=["FDeptNumber"])

    res = pd.concat([datadf, newdf])

    res.rename(columns={BorrowLoan: rename}, inplace=True)

    res["FSeq"] = fSeq

    deptafter = dept_replace(res)

    return deptafter


def subfunction3(df, acct, fAccountBorrowSql, fAccountLoanSql, fSeq):
    '''
    ['研发项目', '责任中心', '重分类']
    :return:
    '''

    rename = ""

    BorrowLoan = ""

    if fAccountBorrowSql != "":
        BorrowLoan = fAccountBorrowSql.split(".")[1]

        rename = "allamountBorrow"

    if fAccountLoanSql != "":
        BorrowLoan = fAccountLoanSql.split(".")[1]

        rename = "allamountLoan"

    datadf = df[df["FAccount"] == acct][
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FRdProject", "FYear", "FMonth",
         "FAccount",BorrowLoan]]

    newdf = pd.DataFrame(columns=["FProjectNumber", "FWorkCenterNumber", "FAcctreClassNumber"])

    res = pd.concat([datadf, newdf])

    res.rename(columns={BorrowLoan: rename}, inplace=True)

    res["FSeq"] = fSeq

    rditemafter = rditem_repalce(res)

    workcenterdf = workcenter_repalce(rditemafter)

    res = acctreclass_replace(workcenterdf)

    return res


def subfunction4(df, fAccountBorrowSql, fAccountLoanSql, fSeq):
    '''
    部门
    :param df:
    :param fAccountBorrowSql:
    :param fAccountLoanSql:
    :return:
    '''

    rename = ""

    BorrowLoan = ""

    if fAccountBorrowSql != "":
        BorrowLoan = fAccountBorrowSql.split(".")[1]

        rename = "allamountBorrow"

    if fAccountLoanSql != "":
        BorrowLoan = fAccountLoanSql.split(".")[1]

        rename = "allamountLoan"

    grouped2 = df.groupby(
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth"])[BorrowLoan].sum().to_frame()

    grouped2 = grouped2.reset_index()

    grouped2.columns = ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", rename]

    # grouped2.drop('count', axis=1, inplace=True)

    newdf = pd.DataFrame(columns=["FDeptNumber"])

    res = pd.concat([grouped2, newdf])

    # res.rename(columns={BorrowLoan: rename}, inplace=True)

    res["FSeq"] = fSeq

    deptafter = dept_replace(res)

    return deptafter


def defult_f(df, acct, fAccountBorrowSql, fAccountLoanSql, fSeq):
    list = [{
        "FNumber": "",
        "FExpenseOrgID": "",
        "FTaxDeclarationOrg": "",
        "FHightechDept": "",
        "FYear": "",
        "FMonth": ""
    }]

    res = pd.DataFrame(list)

    res["FSeq"] = fSeq

    res["FYear"] = df.loc[0]["FYear"]
    res["FMonth"] = df.loc[0]["FMonth"]

    BorrowLoan = ""

    if fAccountBorrowSql != "":
        BorrowLoan = fAccountBorrowSql.split(".")[1]

    if fAccountLoanSql != "":
        BorrowLoan = fAccountLoanSql.split(".")[1]

    if BorrowLoan != "":

        if fAccountBorrowSql != "":
            res["allamountBorrow"] = abs(df[BorrowLoan.strip()].sum())

        if fAccountLoanSql != "":
            res["allamountLoan"] = abs(df[BorrowLoan.strip()].sum())

        return res


def switch(arg, df, acct, fAccountBorrowSql, fAccountLoanSql, fSeq):
    sw = {23: subfunction1,
          17: subfunction2,
          30: subfunction3,
          1: subfunction4}

    if arg == 1:

        return sw.get(arg, defult_f)(df, fAccountBorrowSql, fAccountLoanSql, fSeq)

    else:

        return sw.get(arg, defult_f)(df, acct, fAccountBorrowSql, fAccountLoanSql, fSeq)


def permutation(oldList):
    s = pd.Series(["部门", "责任中心", "重分类", "研发项目", "一级科目"])

    res = s.isin(oldList).to_frame()

    res.columns = ["juge"]

    oldList = res[res["juge"] == True].index.tolist()

    arg = recursion(oldList)

    return arg


def recursion(oldList):
    sum = 0

    for i in oldList:
        sum = sum + int(math.pow(2, i))

    return sum


def judgement(row, df):
    fLexitemProperty = row["FLexitemProperty"].split("/")
    fAcct = row["FAccount"]
    fFirstAcct = row["FFirstAcct"]
    fAccountNumber = row["FAccountNumber"]
    fAccountBorrowSql = row["FAccountBorrowSql"]
    fAccountLoanSql = row["FAccountLoanSql"]
    fSeq = row["FSeq"]

    if fFirstAcct == 1:
        # 一级科目，需要传递科目

        fLexitemProperty.append("一级科目")

        arg = permutation(fLexitemProperty)

        return switch(arg, df, fAcct, fAccountBorrowSql, fAccountLoanSql, fSeq)

        pass

    if fFirstAcct == 0 and fAccountNumber != 0:
        arg = permutation(fLexitemProperty)

        res = switch(arg, df, fAcct, fAccountBorrowSql, fAccountLoanSql, fSeq)

        return res

    if fFirstAcct == 0 and fAccountNumber == 0:
        arg = permutation(fLexitemProperty)

        res = switch(arg, df, fAcct, fAccountBorrowSql, fAccountLoanSql, fSeq)

        return res


def data_deal(df, datadf):
    resultdf = pd.DataFrame()

    for i in df.index:
        resultdf = pd.concat([resultdf, judgement(df.iloc[i], datadf)])

    return resultdf


FToken = '1ED05534-A0EE-4BAF-89B2-8F1F5A0ABE70'
app = RdClient(token=FToken)
voucherTpldf=""
deptdf = dept_query(app)
acctreclassdf = acctreclass_query(app)
workcenterdf = workcenter_query(app)
rditemdf = rditem_query(app)
acctdf = acct_query(app)
projectdf = project_query(app)



def action(FToken, FCategory, FYear, FMonth, FNumber):
    app = RdClient(FToken)

    if ("工资" in FCategory) or ("年终奖" in FCategory):

        # sList=salaryList_query(app,FYear,FMonth)

        ruleVars = salaryBill_getRuleVars(app, FNumber)

        voucherRuleRes = voucherRule_query(app, ruleVars[0]["FExpenseOrgID"], ruleVars[0]["FTaxDeclarationOrg"],
                                           ruleVars[0]["FBankType"], ruleVars[0]["FCategoryType"])

        global voucherTpldf

        voucherTpldf = voucher_query(app, voucherRuleRes[0]["FNumber"])

        FSourceData = salaryBill_query(app, FNumber)

        res = data_deal(voucherTpldf, FSourceData)

        df4 = pd.merge(voucherTpldf, res, how="inner", on="FSeq")

        result = notes_repalce(df4)

        result.to_excel(r'C:\Users\志\Desktop\a1.xlsx')


    elif ("社保" in FCategory) or ("公积金" in FCategory):

        # res=socialsecurityList_query(app,FYear,FMonth)

        ruleVars = socialsecurityBill_getRuleVars(app, FNumber)

        voucherRuleRes = voucherRule_query(app, ruleVars[0]["FExpenseOrgID"], ruleVars[0]["FTaxDeclarationOrg"],
                                           ruleVars[0]["FBankType"], ruleVars[0]["FCategoryType"])



        voucherTpldf = voucher_query(app, voucherRuleRes[0]["FNumber"])

        FSourceData = socialsecurityBill_query(app, FNumber)

        res = data_deal(voucherTpldf, FSourceData)

        df4 = pd.merge(voucherTpldf, res, how="inner", on="FSeq")

        result = notes_repalce(df4)

        result.to_excel(r'C:\Users\志\Desktop\a1.xlsx')

    else:

        print("其他业务")



