#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrda.dbms.rds import RdClient
import pandas as pd
import numpy as np
import time
from . import OdsToStd
from . import SrcToOds
from . import DataIntoERP


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
        from rds_hrv_ods_tpl_voucher a
        inner join rds_hrv_ods_md_acct b
        on a.FSubjectNumber=b.FAccountNumber 
        where a.FNumber = '{FNumber}'
        order by FSeq asc
    """

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def tableName_query(app, FCategory):
    '''
    表名查询
    :param app:
    :param FCategory:
    :return:
    '''

    sql = f"""select FTableName from rds_hrv_ods_md_categoryTypeTable 
    where FCategoryType='{FCategory}'"""

    res = app.select(sql)

    if res:
        return res[0]["FTableName"]


def category_query(app, FNumber):
    '''
    单据类型查询
    :param app:
    :param FNumber:
    :return:
    '''

    sql = f"select FCategoryType from rds_hrv_ods_ds_documentNumber where FNumber='{FNumber}'"

    res = app.select(sql)

    return res


def categorySecond_query(app, FYear, FMonth):
    '''
    单据类型查询
    :param app:
    :param FNumber:
    :return:
    '''

    sql = f"select FCategoryType,FNumber from rds_hrv_ods_ds_documentNumber where FYear='{FYear}' and FMonth='{FMonth}'"

    res = app.select(sql)

    return res


def datasource_query(app, FTableName, FNumber):
    '''
    查询数据源
    :param app:
    :param FTableName:
    :param FNumber:
    :return:
    '''

    sql = f"""select * from {FTableName} where FNumber='{FNumber}'"""

    res = app.select(sql)

    dataSourcedf = pd.DataFrame(res)

    return dataSourcedf


def dept_query(app):
    '''
    部门查询
    :param FToken: token
    :param FName: 部门名称
    :return: 返回值
    '''

    sql = f"""select * from rds_hrv_ods_md_dept """

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

    sql = f"""select * from rds_hrv_ods_md_acctreclass"""

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

    sql = f"""select * from rds_hrv_ods_md_workcenter"""
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

    sql = f"""select * from rds_hrv_ods_ds_detail"""
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

    sql = f"""select * from rds_hrv_ods_md_acct"""
    data = app.select(sql)
    res = pd.DataFrame(data)
    return res


def project_query(app):
    '''
    项目对照查询
    :param df:
    :return:
    '''

    sql = "select * from rds_hrv_ods_md_rditem"

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def getRuleVars(app, FTableName, FBillNumber):
    '''
    通过单据编号从数据源获取规则变量：单据编号，费用承担组织，个税申报组织，银行，业务类型
    :param FToken: 口令
    :param FBillNumber: 单据编号
    :return: 单据编号，费用承担组织，个税申报组织，银行，业务类型
    '''
    sql = f"""select FNumber, FExpenseOrgID, FTaxDeclarationOrg, FBankType, FCategoryType from {FTableName} where FNumber = '{FBillNumber}'"""

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

    sql = f"""select FNumber from rds_hrv_ods_rule_voucher where  FExpenseOrgID = '{FExpenseOrgID}' and 
    FTaxDeclarationOrg = '{FTaxDeclarationOrg}' and FBankType = '{FBankType}' and FCategoryType = '{FCategoryType}'"""

    res = app.select(sql)

    return res


def permutation(oldList):
    s = pd.Series(["部门", "责任中心", "重分类", "研发项目", "银行账号", "往来单位", "供应商"])

    res = s.isin(oldList).to_frame()

    res.columns = ["judge"]

    res.loc[res['judge'] == True, 'judge'] = 1
    res.loc[res['judge'] == False, 'judge'] = 0

    return res


def fetchNumber_byFAcct(df, acct, borrowLoanSql):
    '''
    通过科目取数
    :param df: 数据源
    :param acct:科目
    :param borrowLoanSql:取值字段
    :return:
    '''

    # "FSeqNew"

    datadf = df[df["FAccount"] == acct][
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FRdProject", "FYear", "FMonth", "FDate",
         "FOldDept","FNotePeriod", borrowLoanSql.strip()]]

    return datadf


def deptOldName_query(app2):
    sql = "select * from rds_hrv_ods_md_deptcomparison"

    res = app2.select(sql)

    df = pd.DataFrame(res)

    return df


def deptName_repalce(deptOldName, FName):
    '''
    部门名字替换
    :param FName:
    :return:
    '''

    oldname = FName

    if deptOldName[deptOldName["FDept"] == FName].empty != True:
        oldname = deptOldName[deptOldName["FDept"] == FName]["FOldDept"].tolist()[-1]

    return oldname


def dept_replace(df, deptdf, deptOldName):
    '''
    部门替换
    :param df:
    :return:
    '''
    # deptdf

    for i in df.index:

        deptName = df.loc[i]["FHightechDept"]

        FNewName = deptName_repalce(deptOldName, deptName)

        if deptdf[deptdf["FDepName"] == FNewName].empty != True:
            deptNumber = (deptdf[deptdf["FDepName"] == FNewName]).iloc[0]["FNumber"]

            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FDeptNumber"] = deptNumber
            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FDeptName"] = FNewName

    return df


def acctreclass_replace(df, voucherTpldf, acctreclassdf):
    '''
    重分类替换
    :param df:
    :return:
    '''
    # FAccountName = voucherTpldf.iloc[0]["FAccountName"]

    FAccountName = voucherTpldf.iloc[19]

    if acctreclassdf[acctreclassdf["FAccountItem"] == FAccountName].empty != True:
        acctreclass = (acctreclassdf[acctreclassdf["FAccountItem"] == FAccountName]).iloc[0]["FNumber"]

        df.loc[df.index.tolist(), "FAcctreClassNumber"] = acctreclass

        df.loc[df.index.tolist(), "FAcctreClassName"] = FAccountName

    return df


def workcenter_repalce(df, workcenterdf, deptOldName):
    '''
    责任中心替换
    :param df:
    :return:
    '''

    for i in df.index:

        deptName = df.loc[i]["FHightechDept"]

        if workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptOldName, deptName)].empty != True:

            deptNumber = (workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptOldName, deptName)]).iloc[0][
                "FNumber"]

            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterNumber"] = deptNumber

            df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterName"] = deptName

        else:

            oldDeptName = df.loc[i]["FOldDept"]

            if workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptOldName, oldDeptName)].empty != True:
                deptNumber = \
                    (workcenterdf[workcenterdf["FDept"] == deptName_repalce(deptOldName, oldDeptName)]).iloc[0][
                        "FNumber"]

                df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterNumber"] = deptNumber

                df.loc[df[df["FHightechDept"] == deptName].index.tolist(), "FWorkCenterName"] = oldDeptName

    return df


def rditem_repalce(df, projectdf):
    '''
    研发项目
    :param df:
    :return:
    '''

    for i in df.index:

        if df.loc[i]["FRdProject"] != '':
            projectNumbercode = df.loc[i]["FRdProject"]

            reProjectNumbercode = "-".join(projectNumbercode.split("_"))

            if projectdf[projectdf["FRDProjectManual"] == reProjectNumbercode].empty != True:
                projectNumber = (projectdf[projectdf["FRDProjectManual"] == reProjectNumbercode]).iloc[0]["FRDProject"]

                df.loc[df[df["FRdProject"] == projectNumbercode].index.tolist(), "FProjectNumber"] = projectNumber

    return df


def lowgradeFunction(data, columns, fSeq, rename, borrowLoanSql):
    newdf = pd.DataFrame(columns=columns)

    res = pd.concat([data, newdf])

    res["FSeq"] = fSeq

    res.rename(columns={borrowLoanSql.strip(): rename, "FNumber": "FBillNO"}, inplace=True)

    return res


def noFirstAcctData_deal(df, borrowLoanSql):
    '''
    不是一级科目数据处理
    :param df:
    :param borrowLoanSql:
    :return:
    '''

    # "FSeqNew"

    res = df.groupby(
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", "FDate", "FOldDept","FNotePeriod"])[
        borrowLoanSql.strip()].sum().to_frame()

    res = res.reset_index()

    res.columns = ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", "FDate",
                   "FOldDept","FNotePeriod",
                   borrowLoanSql.strip()]

    res[borrowLoanSql.strip()] = res[borrowLoanSql.strip()].astype(float)

    res[borrowLoanSql.strip()] = res[borrowLoanSql.strip()].round(2)

    return res


def noFirstAcctDataDefualt_deal(df, borrowLoanSql, BorrowLoan, defultvalue, FNumberTpl, fSeq, rename):
    '''
    不是一级科目数据处理
    :param df:
    :param borrowLoanSql:
    :return:
    '''

    # res = df.groupby(
    #     ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth","FDate","FOldDept","FSeqNew"])[
    #     borrowLoanSql.strip()].sum().to_frame()
    #
    # res = res.reset_index()
    #
    # res.columns = ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth","FDate","FOldDept","FSeqNew",
    #                borrowLoanSql.strip()]

    # d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]
    #
    # if BorrowLoan != "":
    #     res[rename] = abs(float(d))
    #
    # return res

    # "FSeqNew"

    res = df.groupby(
        ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", "FDate", "FOldDept","FNotePeriod"])[
        "FComPensionBenefitsAmt"].sum().to_frame()

    res = res.reset_index()

    res.columns = ["FNumber", "FExpenseOrgID", "FTaxDeclarationOrg", "FHightechDept", "FYear", "FMonth", "FDate",
                   "FOldDept","FNotePeriod",
                   "FComPensionBenefitsAmt"]

    res.drop(['FComPensionBenefitsAmt'], axis=1, inplace=True)

    d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

    if BorrowLoan != "":
        res[rename] = round(abs(float(d)), 2)

    return res


def totalAmount_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl):
    '''
    总额处理
    :param df:
    :param fSeq:模板序号
    :return:
    '''

    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod":df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        rename: ""
        # rename:abs(df[BorrowLoan.strip()].astype(np.float64).sum())
    }]

    res = pd.DataFrame(list)

    if BorrowLoan.strip() != 'FDefaultAmt':
        res[rename] = round(abs(df[BorrowLoan.strip()].astype(np.float64).sum()), 2)

    if BorrowLoan.strip() == 'FDefaultAmt':
        d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

        res[rename] = round(abs(float(d)), 2)

    return res


def bankAmount_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl, row):
    '''
    银行账号
    :param df:
    :param fSeq:模板序号
    :return:
    '''

    FBankAccount = field_split(row["FObtainSource"], "/")

    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        "FBankAccount": FBankAccount,
        rename: ""
        # rename:abs(df[BorrowLoan.strip()].astype(np.float64).sum())
    }]

    res = pd.DataFrame(list)

    if BorrowLoan.strip() != 'FDefaultAmt':
        res[rename] = round(abs(df[BorrowLoan.strip()].astype(np.float64).sum()), 2)

    if BorrowLoan.strip() == 'FDefaultAmt':
        d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

        res[rename] = round(abs(float(d)), 2)

    return res


def field_split(field, symbol):
    '''
    字段拆分
    :param field:字段
    :param symbol:拆分符号
    :return:
    '''

    res = field.split(symbol)

    if res:
        return res[-1]


def field_splitCount(field, symbol):
    '''
    字段拆分
    :param field:字段
    :param symbol:拆分符号
    :return:
    '''

    res = field.split(symbol)

    return res


def NotesFiscalYear_repalce(df):
    '''
    会计年度替换
    :param df:
    :return:
    '''

    res = df.apply(lbNY, axis=1)

    df["FNotes"] = res

    return df


def lbNA(x):
    '''
    摘要替换辅助函数
    :param x:
    :return:
    '''

    # if x['FCategoryType'].__contains__('发放'):
    #
    #     FMonth = int(x["FMonth"]) - 1
    #
    #     if int(x["FMonth"]) == 1:
    #         FMonth = 12
    #
    #     return x['FNotes'].replace('{会计期间}', str(FMonth))
    #
    # return x['FNotes'].replace('{会计期间}', str(int(x["FMonth"])) if x['FMonth'] != '' else "")

    # res[1].split("月")[0]

    return x['FNotes'].replace('{摘要期间}', str(int((str(x["FNotePeriod"]).split("年")[1]).split("月")[0])) if x["FNotePeriod"] != '' else "")




def lbNY(x):
    '''
    摘要替换辅助函数
    :param x:
    :return:
    '''

    # if x['FCategoryType'].__contains__('发放'):
    #
    #     FYear = int(x["FYear"])
    #
    #     if int(x["FMonth"]) == 1:
    #         FYear = int(x["FYear"]) - 1
    #
    #     return x['FNotes'].replace('{会计年度}', str(FYear))
    #
    # return x['FNotes'].replace('{会计年度}', str(int(x["FYear"])) if x['FYear'] != '' else "")


    return x['FNotes'].replace('{会计年度}', str(int(str(x["FNotePeriod"]).split("年")[0])) if x["FNotePeriod"] != '' else "")


def lbDept(x):
    '''
    摘要替换辅助函数
    :param x:
    :return:
    '''

    return x['FNotes'].replace('{部门}', str(x["FDeptName"]) if str(x["FDeptName"]) != "" else str(x["FHightechDept"]))


def NotesAccountingPeriod_repalce(df):
    '''
    会计期间替换
    :param df:
    :return:
    '''

    res = df.apply(lbNA, axis=1)

    df["FNotes"] = res

    return df


def NotesDept_repalce(df):
    '''
    摘要部门替换
    :param df:
    :return:
    '''

    df = df.fillna("")

    res = df.apply(lbDept, axis=1)

    df["FNotes"] = res

    return df


def defult_query(app):
    '''
    默认值查询
    :param app:
    :return:
    '''

    sql = "select * from rds_hrv_ods_tpl_defaultValue"

    res = app.select(sql)

    df = pd.DataFrame(res)

    return df


def totalValue_deal(df, fsql):
    '''
    合计取值
    :param df:
    :param fsql:
    :return:
    '''

    add = field_splitCount(fsql, "+")
    subtract = field_splitCount(fsql, "-")

    if len(add) > 1:

        df[fsql] = 0

        df[fsql] = df[fsql].astype(float)

        for i in add:
            df[i] = df[i].astype(float)

            df[fsql] = abs(df[fsql].round(2) + df[i].round(2))

    if len(subtract) > 1:

        df[fsql] = 0

        df[fsql] = df[fsql].astype(float)

        for i in subtract:
            df[i] = df[i].astype(float)

            df[fsql] = abs(df[fsql].round(2) - df[i].round(2))

    return df


def fixedValue_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl):
    '''
    管理费用固定值处理
    :param df:
    :param fSeq:
    :param BorrowLoan:
    :param rename:
    :return:
    '''

    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        rename: ""
    }]

    res = pd.DataFrame(list)

    d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

    if BorrowLoan != "":
        res[rename] = round(abs(float(d)), 2)

    return res


def dealingUnit_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl):
    '''
    往来单位处理
    :param df:
    :param fSeq:
    :param BorrowLoan:
    :param rename:
    :param defultvalue:
    :param FNumberTpl:
    :return:
    '''

    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        rename: "",
        "FDealingUnitNumber": "",
        "FDealingUnitName": ""
    }]

    res = pd.DataFrame(list)

    FNumber = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        "FDefaultNumber"].tolist()

    FName = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        "FDefaultName"].tolist()

    res["FDealingUnitNumber"] = FNumber
    res["FDealingUnitName"] = FName

    if BorrowLoan.strip() != 'FDefaultAmt':
        res[rename] = round(abs(df[BorrowLoan.strip()].astype(np.float64).sum()), 2)

    if BorrowLoan.strip() == 'FDefaultAmt':
        d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

        res[rename] = round(abs(float(d)), 2)

    return res


def supplier_deal(df, fSeq, BorrowLoan, rename, defultvalue, FNumberTpl):
    '''
    供应商处理
    :param df:
    :param fSeq:
    :param BorrowLoan:
    :param rename:
    :param defultvalue:
    :param FNumberTpl:
    :return:
    '''

    list = [{
        "FBillNO": df.loc[0]["FNumber"],
        "FExpenseOrgID": df.loc[0]["FExpenseOrgID"],
        "FTaxDeclarationOrg": df.loc[0]["FTaxDeclarationOrg"],
        "FHightechDept": "",
        "FYear": df.loc[0]["FYear"],
        "FMonth": df.loc[0]["FMonth"],
        "FDate": df.loc[0]["FDate"],
        "FOldDept": df.loc[0]["FOldDept"],
        "FNotePeriod": df.loc[0]["FNotePeriod"],
        # "FSeqNew":df.loc[0]["FSeqNew"],
        "FSeq": fSeq,
        rename: "",
        "FSupplierNumber": "",
        "FSupplierName": ""
    }]

    res = pd.DataFrame(list)

    FNumber = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        "FDefaultNumber"].tolist()

    FName = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))][
        "FDefaultName"].tolist()

    res["FSupplierNumber"] = FNumber
    res["FSupplierName"] = FName

    if BorrowLoan.strip() != 'FDefaultAmt':
        res[rename] = round(abs(df[BorrowLoan.strip()].astype(np.float64).sum()), 2)

    if BorrowLoan.strip() == 'FDefaultAmt':
        d = defultvalue[(defultvalue["FNumber"] == str(FNumberTpl)) & (defultvalue["FSeq"] == int(fSeq))]["FDefaultAmt"]

        res[rename] = round(abs(float(d)), 2)

    return res


def subfunction(df, acct, fSql, fSeq, fDept, FFirstAcct, fAccountNumber, FWorkCenter, FRclass, FRDItem,
                deptdf, acctreclassdf, workcenterdf, rditemdf, acctdf, projectdf, voucherTpldf, rename, defultvalue,
                FNumberTpl, deptOldName, Bankaccount, row, dealingUnit, supplier):
    '''

    :param df:
    :param acct:
    :param fSql:
    :param fSeq:
    :param fDept:
    :param FFirstAcct:
    :param FWorkCenter:
    :param FRclass:
    :param FRDItem:
    :return:
    '''

    if FFirstAcct == 1 and fAccountNumber != 0 and fDept == 1 and FWorkCenter == 1 and FRclass == 1 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # ['部门', '责任中心', '重分类']

        columns = ["FDeptNumber", "FDeptName", "FWorkCenterNumber", "FWorkCenterName", "FAcctreClassNumber",
                   "FAcctreClassName"]

        dataAcct = fetchNumber_byFAcct(df, acct, fSql)

        res = lowgradeFunction(data=dataAcct, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        deptafter = dept_replace(res, deptdf, deptOldName)

        workcenterdf = workcenter_repalce(deptafter, workcenterdf, deptOldName)

        # res = acctreclass_replace(workcenterdf, voucherTpldf, acctreclassdf)

        res = acctreclass_replace(workcenterdf, row, acctreclassdf)

        return res

    if FFirstAcct == 1 and fAccountNumber != 0 and fDept == 1 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # ['部门']

        columns = ["FDeptNumber", "FDeptName"]

        dataAcct = fetchNumber_byFAcct(df, acct, fSql)

        res = lowgradeFunction(data=dataAcct, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        res = dept_replace(res, deptdf, deptOldName)

        return res

    if FFirstAcct == 1 and fAccountNumber != 0 and fDept == 0 and FWorkCenter == 1 and FRclass == 1 and FRDItem == 1 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # ['研发项目', '责任中心', '重分类']

        columns = ["FProjectNumber", "FWorkCenterNumber", "FWorkCenterName", "FAcctreClassNumber", "FAcctreClassName"]

        dataAcct = fetchNumber_byFAcct(df, acct, fSql)

        res = lowgradeFunction(data=dataAcct, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        rditemafter = rditem_repalce(res, projectdf)

        workcenterdf = workcenter_repalce(rditemafter, workcenterdf, deptOldName)

        res = acctreclass_replace(workcenterdf, row, acctreclassdf)

        # res = acctreclass_replace(workcenterdf, voucherTpldf, acctreclassdf)

        return res

    if FFirstAcct == 0 and fAccountNumber == 1 and fDept == 1 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # 部门 不是一级科目分配

        columns = ["FDeptNumber", "FDeptName"]

        noFirstAcctData = noFirstAcctData_deal(df=df, borrowLoanSql=fSql)

        res = lowgradeFunction(data=noFirstAcctData, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        res = dept_replace(res, deptdf, deptOldName)

        return res

    if FFirstAcct == 0 and fAccountNumber == 0 and fDept == 0 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 0:
        # 总额

        res = totalAmount_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                               FNumberTpl=FNumberTpl)

        return res

    if FFirstAcct == 0 and fAccountNumber == 0 and fDept == 0 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 1 and dealingUnit == 0 and supplier == 0:
        # 银行账号

        res = bankAmount_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                              FNumberTpl=FNumberTpl, row=row)

        return res

    if FFirstAcct == 0 and fAccountNumber == 3 and fSql == "FDefaultAmt" and dealingUnit == 0 and supplier == 0 and fDept == 1 and FWorkCenter == 1 and FRclass == 1:
        # 服务费
        columns = ["FDeptNumber", "FDeptName", "FWorkCenterNumber", "FWorkCenterName", "FAcctreClassNumber",
                   "FAcctreClassName"]

        noFirstAcctData = noFirstAcctDataDefualt_deal(df=df, borrowLoanSql=fSql, BorrowLoan=fSql,
                                                      defultvalue=defultvalue, FNumberTpl=FNumberTpl, fSeq=fSeq,
                                                      rename=rename)

        res = lowgradeFunction(data=noFirstAcctData, columns=columns, fSeq=fSeq, rename=rename, borrowLoanSql=fSql)

        deptafter = dept_replace(res, deptdf, deptOldName)

        workcenterdf = workcenter_repalce(deptafter, workcenterdf, deptOldName)

        # res = acctreclass_replace(workcenterdf, voucherTpldf, acctreclassdf)

        # rowdf=pd.DataFrame(row)

        # print(rowdf.iloc[19])

        res = acctreclass_replace(workcenterdf, row, acctreclassdf)

        # res=fixedValue_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename,defultvalue=defultvalue,FNumberTpl=FNumberTpl)

        return res

    if FFirstAcct == 0 and fAccountNumber == 0 and fDept == 0 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 1 and supplier == 0:
        # 往来单位

        res = dealingUnit_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                               FNumberTpl=FNumberTpl)

        return res

    if FFirstAcct == 0 and fAccountNumber == 0 and fAccountNumber == 0 and fDept == 0 and FWorkCenter == 0 and FRclass == 0 and FRDItem == 0 and Bankaccount == 0 and dealingUnit == 0 and supplier == 1:
        # 供应商

        res = supplier_deal(df=df, fSeq=fSeq, BorrowLoan=fSql, rename=rename, defultvalue=defultvalue,
                            FNumberTpl=FNumberTpl)

        return res


def judgement(row, df, deptdf, acctreclassdf, workcenterdf, rditemdf, acctdf, projectdf, voucherTpldf, defultvalue,
              deptOldName):
    fLexitemProperty = row["FLexitemProperty"].split("/")
    fAcct = row["FAccount"]
    fFirstAcct = row["FFirstAcct"]
    fAccountNumber = row["FAccountNumber"]
    fAccountBorrowSql = row["FAccountBorrowSql"]
    fAccountLoanSql = row["FAccountLoanSql"]
    fSeq = row["FSeq"]
    FNumberTpl = row["FNumber"]
    rename = ""
    fSql = ""

    # df.rename(columns={"FSeqNew":"FNewSeq"},inplace=True)

    if fAccountBorrowSql != "":
        fSql = field_split(fAccountBorrowSql, ".")

        df = totalValue_deal(df, fSql)

        rename = "allamountBorrow"

    if fAccountLoanSql != "":
        fSql = field_split(fAccountLoanSql, ".")

        df = totalValue_deal(df, fSql)

        rename = "allamountLoan"

    arg = permutation(fLexitemProperty)

    res = subfunction(df=df, acct=fAcct, fSeq=fSeq, fSql=fSql, fDept=arg.loc[0]["judge"], FFirstAcct=fFirstAcct,
                      fAccountNumber=fAccountNumber, FWorkCenter=arg.loc[1]["judge"],
                      FRclass=arg.loc[2]["judge"], FRDItem=arg.loc[3]["judge"], deptdf=deptdf,
                      acctreclassdf=acctreclassdf, workcenterdf=workcenterdf, rditemdf=rditemdf, acctdf=acctdf,
                      projectdf=projectdf, voucherTpldf=voucherTpldf, rename=rename, defultvalue=defultvalue,
                      FNumberTpl=FNumberTpl, deptOldName=deptOldName, Bankaccount=arg.loc[4]["judge"], row=row,
                      dealingUnit=arg.loc[5]["judge"], supplier=arg.loc[6]["judge"])

    return res


def data_deal(voucherTpldf, datadf, deptdf, acctreclassdf, workcenterdf, rditemdf, acctdf, projectdf, defultvalue,
              deptOldName):
    resultdf = pd.DataFrame()

    for i in voucherTpldf.index:
        resultdf = pd.concat([resultdf,
                              judgement(voucherTpldf.iloc[i], datadf, deptdf, acctreclassdf, workcenterdf, rditemdf,
                                        acctdf, projectdf, voucherTpldf, defultvalue, deptOldName)])

    return resultdf


def result_deal(df):
    '''
    对过程表进行处理
    :param df:
    :return:
    '''

    df.drop(['FAccountBorrow', 'FAccountLoan', 'FAccountBorrowSql', 'FAccountLoanSql', 'FFirstAcct', 'FAccount',
             'FAccountName', 'FAccountNumber', 'FLexitemProperty', 'FObtainSource', "FOldDept","FNotePeriod"], axis=1, inplace=True)

    return df


def sqlSplicing(df):
    df = df.reset_index(drop=True)

    df = df.fillna("")

    # df["allamountBorrow"] = df["allamountBorrow"].round(2)
    #
    # df["allamountLoan"] = df["allamountLoan"].round(2)

    df["allamountBorrow"] = df["allamountBorrow"].astype(str)
    df["allamountLoan"] = df["allamountLoan"].astype(str)
    df["FDate"] = df["FDate"].astype(str)

    col = ",".join(df.columns.tolist())

    sql = """insert into rds_hrv_src_ds_middleTable_filtration(""" + col + """) values"""

    for i in df.index:

        if i == len(df) - 1:

            sql = sql + str(tuple(df.iloc[i]))

        else:

            sql = sql + str(tuple(df.iloc[i])) + ""","""

    return sql


def src_clear(app, tablename):
    '''
    将SRC表中的数据清空
    :param app:
    :return:
    '''

    sql = f"truncate table {tablename}"

    app.update(sql)


def errorData_clear(app, FYear, FMonth):
    '''
    将中间表异常数据清理
    :param app:
    :return:
    '''

    FYear = str(FYear) + ".0"
    FMonth = str(FMonth) + ".0"

    sql = f"delete from rds_hrv_ods_ds_middleTable where FIsdo=2 and FYear='{FYear}' and FMonth='{FMonth}'"

    app.update(sql)


# def errorData_insert(app):
#     '''
#     将中间表异常数据插入异常表
#     :param app:
#     :return:
#     '''
#
#     sql="""
#     INSERT INTO rds_hrv_ods_ds_middleTable_error
#                 SELECT
# 				FDate,
# 				FYear,
# 				FMonth,
# 				FBillNO,
# 				FSeq,
# 				FNumber,
# 				FName,
# 				FTaxDeclarationOrg,
# 				FExpenseOrgID,
# 				FCategoryType,
# 				FNotes,
# 				FAccountBookID,
# 				FDealingUnitName,
# 				FDealingUnitNumber,
# 				FSupplierName,
# 				FSupplierNumber,
# 				FAccountName,
# 				FHightechDept,
# 				FSubjectNumber,
# 				FSubjectName,
# 				FLexitemProperty,
# 				FDeptNumber,
# 				FDeptName,
# 				FRdProject,
# 				FProjectNumber,
# 				FWorkCenterNumber,
# 				FAcctreClassNumber,
# 				FBankAccount,
# 				allamountBorrow,
# 				allamountLoan,
# 				FSettleMethod,
# 				FSettleNumber,
# 				FMessage,
# 				FIsdo,
# 				FWorkCenterName,
# 				FAcctreClassName,
# 				FSeqNew
#                 from rds_hrv_ods_ds_middleTable a
#                 where a.FIsdo=2
#     """
#
#     app.update(sql)


def middleTableSrctoOds(app):
    '''
    中间表SRC-ODS
    :param app:
    :return:
    '''

    sql = """
    INSERT INTO rds_hrv_ods_ds_middleTable  
                SELECT 
				FDate,
				FYear,
				FMonth,
				FBillNO,
				FSeq,
				FNumber,
				FName,
				FTaxDeclarationOrg,
				FExpenseOrgID,
				FCategoryType,
				FNotes,
				FAccountBookID,
				FDealingUnitName,
				FDealingUnitNumber,
				FSupplierName,
				FSupplierNumber,
				FAccountName,
				FHightechDept,
				FSubjectNumber,
				FSubjectName,
				FLexitemProperty,
				FDeptNumber,
				FDeptName,
				FRdProject,
				FProjectNumber,
				FWorkCenterNumber,
				FAcctreClassNumber,
				FBankAccount,
				allamountBorrow,
				allamountLoan,
				FSettleMethod,
				FSettleNumber,
				FMessage,
				FIsdo,
				FWorkCenterName,
				FAcctreClassName,
				FSeqNew
                from (select * from rds_hrv_src_ds_middleTable where FIsdo=0) a
                where not exists
                (select * from rds_hrv_ods_ds_middleTable b 
                    where A.FBillNO = B.FBillNO)
    """
    app.update(sql)

    src_clear(app, "rds_hrv_src_ds_middleTable")


def middleTableSrc(app):
    '''
    中间表SRC-ODS
    :param app:
    :return:
    '''

    sql = """
    INSERT INTO rds_hrv_src_ds_middleTable  
                SELECT 
				FDate,
				FYear,
				FMonth,
				FBillNO,
				FSeq,
				FNumber,
				FName,
				FTaxDeclarationOrg,
				FExpenseOrgID,
				FCategoryType,
				FNotes,
				FAccountBookID,
				FDealingUnitName,
				FDealingUnitNumber,
				FSupplierName,
				FSupplierNumber,
				FAccountName,
				FHightechDept,
				FSubjectNumber,
				FSubjectName,
				FLexitemProperty,
				FDeptNumber,
				FDeptName,
				FRdProject,
				FProjectNumber,
				FWorkCenterNumber,
				FAcctreClassNumber,
				FBankAccount,
				allamountBorrow,
				allamountLoan,
				FSettleMethod,
				FSettleNumber,
				FWorkCenterName,
				FAcctreClassName,
				Row_number() OVER(partition by a.FBIllNO order by a.FDate desc) as FSeqNew,
				0 as FIsdo,
				'' as FMessage,
				'' as FSrcSeq,
				'' as FStdSeq
                from rds_hrv_src_ds_middleTable_filtration a
                where not exists
                (select * from rds_hrv_src_ds_middleTable b 
                    where A.FBillNO = B.FBillNO)
    """
    app.update(sql)

    src_clear(app, "rds_hrv_src_ds_middleTable_filtration")


def datatable_into(app, sql):
    '''
    将中间表插入数据库
    :param app:
    :param df:
    :return:
    '''

    app.insert(sql)


def erpKey_query(app, FCategory):
    '''
    查询erp密钥
    :param FCategory:
    :return:
    '''

    sql = f"select acct_id,user_name,app_id,app_sec,server_url,FToken from rds_erp_key where FCategory='{FCategory}'"

    res = app.select(sql)

    return res


def dept_update(app3):
    '''
    更新部门
    :param FToken:
    :return:
    '''

    sql = """
    update a set a.FHightechDept=a.FOldDept  
    from rds_hrv_std_ds_salary  a 
    where a.FHightechDept=''
    
    update a set a.FHightechDept=a.FOldDept 
    from rds_hrv_std_ds_socialsecurity a 
    where a.FHightechDept=''
    
    """

    app3.update(sql)


def action(FToken, FYear, FMonth, FOpthon):
    # time.sleep(70)

    appKey = RdClient(FToken)

    SrcToOds.main(FToken)

    OdsToStd.main(FToken)

    erpkey = erpKey_query(appKey, FOpthon)

    app = RdClient(erpkey[0]['FToken'])

    dept_update(app)

    src_clear(app, "rds_hrv_src_ds_middleTable")
    src_clear(app, "rds_hrv_src_ds_middleTable_filtration")

    deptdf = dept_query(app)
    acctreclassdf = acctreclass_query(app)
    workcenterdf = workcenter_query(app)
    rditemdf = rditem_query(app)
    acctdf = acct_query(app)
    projectdf = project_query(app)
    defultvalue = defult_query(app)

    deptOldName = deptOldName_query(app)

    categoryName = categorySecond_query(app, FYear, FMonth)

    if erpkey:

        if categoryName:

            for values in categoryName:

                tableNameRes = tableName_query(app, values["FCategoryType"])

                if tableNameRes is not None:

                    ruleVars = getRuleVars(app, tableNameRes, values["FNumber"])

                    if ruleVars:

                        voucherRuleRes = voucherRule_query(app, ruleVars[0]["FExpenseOrgID"],
                                                           ruleVars[0]["FTaxDeclarationOrg"],
                                                           ruleVars[0]["FBankType"], ruleVars[0]["FCategoryType"])

                        if voucherRuleRes:

                            for i in voucherRuleRes:
                                voucherTpldf = voucher_query(app, i["FNumber"])

                                dataSourceDF = datasource_query(app, tableNameRes, values["FNumber"])

                                res = data_deal(voucherTpldf, dataSourceDF, deptdf, acctreclassdf, workcenterdf,
                                                rditemdf, acctdf,
                                                projectdf, defultvalue, deptOldName)

                                df4 = pd.merge(voucherTpldf, res, how="inner", on="FSeq")

                                result = NotesFiscalYear_repalce(df4)

                                result = NotesAccountingPeriod_repalce(result)

                                result = NotesDept_repalce(result)

                                result_deal(result)

                                res = sqlSplicing(
                                    result[(result["allamountBorrow"] != 0) & (result["allamountLoan"] != 0)])

                                datatable_into(app, res)

                                # DataIntoERP.voucher_save(FToken, erpkey)

                                # result[(result["allamountBorrow"] != 0) & (result["allamountLoan"] != 0)].to_excel(r'C:\\Users\\志\\Desktop\\test'+"\\"+result["FBillNO"]+"_"+i["FNumber"]+".xlsx")

                                # result[result["allamountBorrow"]!=0].to_excel(r''+FPath+"\\"+FNumber+"_"+i["FNumber"]+".xlsx")

                                # result.to_excel(r''+FPath+"\\"+FNumber+"_"+i["FNumber"]+".xlsx")

                                print(values["FNumber"] + "_" + i["FNumber"] + "生成凭证完成")

                        else:

                            print(values["FNumber"] + "没有查到相应的模板编号")

                else:

                    print("没有找到对应的表名")

            else:

                print("请检查单据编号,未查到对应的单据类型")

    else:

        print("ERP权限查询失败")

    middleTableSrc(app)

    precheckData(app)

    precheckData2(app)

    LineNumber_get(app)

    templateNumber_deal(app, FYear, FMonth)


def vch_save(FToken, FYear, FMonth, FOpthon):
    '''
    凭证生成
    :param app:
    :return:
    '''

    appKey = RdClient(FToken)

    erpkey = erpKey_query(appKey, FOpthon)

    DataIntoERP.voucher_save(FToken=FToken, FYear=FYear, FMonth=FMonth, option1=erpkey)

    pass


def precheckData(app):
    sql = """
    --核算维度部门的

        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，部门未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='部门' and a.FDeptNumber=''
        
        
        -- 部门/责任中心/重分类
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，部门未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='部门/责任中心/重分类' and (FDeptName='')
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，责任中心未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='部门/责任中心/重分类' and (FWorkCenterNumber='')
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，重分类未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='部门/责任中心/重分类' and (FAcctreClassNumber='' )
        
        
        --供应商
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，供应商未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='供应商' and (FSupplierNumber='')
        
        
        --往来单位
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，往来单位未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='往来单位' and (FDealingUnitNumber='')
        
        --研发项目/责任中心/重分类
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，研发项目未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='研发项目/责任中心/重分类' and (FRdProject='')
        
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，研发项目编码未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='研发项目/责任中心/重分类' and (FProjectNumber='')
        
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，责任中心未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='研发项目/责任中心/重分类' and (FWorkCenterNumber='')
        
        
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，重分类未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='研发项目/责任中心/重分类' and (FAcctreClassNumber='' )
        
        
        --银行账号
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，银行账号未匹配到，请检查！',a.FIsdo=2 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_ods_tpl_voucher b
        on a.FNumber=b.FNumber and a.FSeq=b.FSeq
        where b.FLexitemProperty='银行账号' and (FBankAccount='')
        
        
        update a set a.FMessage=a.FMessage+'第'+a.FSeqNew+'行，固定值，请注意！' from rds_hrv_src_ds_middleTable a
        inner join (
        select FNumber,FSeq from rds_hrv_ods_tpl_defaultValue where FDefaultAmt!=0) c
        on a.FNumber=c.FNumber and a.FSeq=c.FSeq
        
        
        update a set a.FIsdo=2 from rds_hrv_src_ds_middleTable a
        inner join 
        (select b.FBillNO from rds_hrv_src_ds_middleTable b where b.FIsdo=2) c
        on a.FBillNO=c.FBillNO
    """

    app.update(sql)


def precheckData2(app):
    '''
    数据预检查
    :param app:
    :return:
    '''

    sql = "select * from rds_hrv_src_ds_middleTable"
    res = app.select(sql)

    df = pd.DataFrame(res)

    if not df.empty:

        df["allamountBorrow"] = df["allamountBorrow"].replace("", "0")
        df["allamountLoan"] = df["allamountLoan"].replace("", "0")
        df["allamountBorrow"] = df["allamountBorrow"].astype(np.float64)
        df["allamountLoan"] = df["allamountLoan"].astype(np.float64)

        dfB = df.groupby(["FBillNO"])["allamountBorrow"].sum()

        dfB = dfB.reset_index()

        dfL = df.groupby(["FBillNO"])["allamountLoan"].sum()

        dfL = dfL.reset_index()

        dfRes = pd.merge(dfB, dfL, how="inner", on="FBillNO")

        dfRes["allamountBorrow"] = dfRes["allamountBorrow"].round(2)

        dfRes["allamountLoan"] = dfRes["allamountLoan"].round(2)

        result = dfRes[dfRes["allamountBorrow"] != dfRes["allamountLoan"]]["FBillNO"].tolist()

        result = list(set(result))

        for i in result:
            sql = f"update a set a.FIsdo=2 , a.FMessage=a.FMessage+'借贷不平，请检查！' from rds_hrv_src_ds_middleTable a where a.FBillNO='{i}'"

            app.update(sql)


def LineNumber_get(app):
    '''
    更新std,src行号,单据编号
    :param app:
    :return:
    '''

    sql = """
    update a set a.FSrcSeq=b.FSeq,a.FStdSeq=b.FSeqNew 
        from rds_hrv_src_ds_middleTable a
        inner join rds_hrv_linenumber3 b
        on a.FHightechDept=b.FHightechDept and a.FBillNO=b.FNumber
        
    update a set a.FBillNO=FBillNO+'-'+FNumber from 
    rds_hrv_src_ds_middleTable a where FNumber='A20'
    """
    app.update(sql)


def templateNumber_deal(app, FYear, FMonth):
    '''
    将未匹配到的模板信息插入到预览表中
    :return:
    '''

    sql = f"""
    select FNumber as FBillNO,'2' as FIsdo from 
    (select 
    FCategoryType,FNumber,FYear,FMonth
    from rds_hrv_ods_ds_documentNumber ) a
    left join (
    select distinct FBillNO from rds_hrv_src_ds_middleTable) b
    on a.FNumber=b.FBILLNO
    where b.FBillNO is null and (a.FYear='{FYear}' and a.FMonth='{FMonth}')
    """

    res = app.select(sql)

    df = pd.DataFrame(res)

    if not df.empty:

        df["FMessage"] = "模板未找到，请核对查询模板条件！"
        df["FYear"] = str(FYear) + ".0"
        df["FMonth"] = str(FMonth) + ".0"

        col = ",".join(df.columns.tolist())

        sql = """insert into rds_hrv_src_ds_middleTable(""" + col + """) values"""

        for i in df.index:

            if i == len(df) - 1:

                sql = sql + str(tuple(df.iloc[i]))

            else:

                sql = sql + str(tuple(df.iloc[i])) + ""","""

        app.insert(sql)
