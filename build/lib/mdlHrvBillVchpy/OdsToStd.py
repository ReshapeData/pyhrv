'''
1，查询工资，社保，工时表数据源

2，工时表的工资类别列转行

3，1，工资关联工时
3.2，工资总额 - 研发总额
3.3，第一行保留总额，其他行其他列金额为0
3.4，合并1,3

4.1，社保关联工时
4.2，社保总额 - 研发社保总额；
    公积金总额 - 研发公积金总额；
4.3，第一行保留总额，其他行其他列金额为0；
4.4，合并1,3

5，去重插入std
'''

from pyrda.dbms.rds import RdClient
import pandas as pd


# 1,读数据，读工资表，社保表，研发工时表，非研发工时表
def sql_query(FToken, table):
    app = RdClient(token=FToken)
    sql = f"""select * from {table}"""
    data = app.select(sql)
    res = pd.DataFrame(data)
    return res


# 2，工时表列转行，将工资类别（工资，社保，公积金）作为列名
def detail_specification(detail, condition, FRdProjectCost):
    detail = detail.groupby(condition)[FRdProjectCost].sum().to_frame().reset_index()
    detail = detail.set_index(condition)[FRdProjectCost].unstack()
    detail.columns.name = None
    detail = detail.reset_index()
    return detail


# 3.1，工资关联工时
def salary_merge_rddtail(salary, detail, condition):
    salary = salary.drop('FRdProject', axis=1)
    salary = salary.drop('FHightechDept', axis=1)
    data = pd.merge(salary, detail, how='inner', on=condition)
    data = data.reset_index(drop=True)

    data['FRdProject'] = data['FRdProject'].fillna('')
    data['工资'] = data['工资'].fillna(0)

    if '奖金' in data.columns:
        data['奖金'] = data['奖金'].fillna(0)
    else:
        data['奖金'] = 0

    data['FCpayAmount'] = data['工资'] + data['奖金']
    data = data.drop(labels=['工资', '社保', '公积金', '奖金'], axis=1)
    data['FAccount'] = '研发支出'
    return data


# 3.2，总额 - 研发
def salary_orther(salary, detail, condition):
    detail = detail.drop('FHightechDept', axis=1)
    data = pd.merge(salary, detail, how='left', on=condition)
    data = data.reset_index(drop=True)

    data['FRdProject'] = data['FRdProject'].fillna('')
    data['工资'] = data['工资'].fillna(0)

    if '奖金' in data.columns:
        data['奖金'] = data['奖金'].fillna(0)
    else:
        data['奖金'] = 0

    data['FCpayAmount'] = data['FCpayAmount'] - data['工资'] + data['奖金']
    data = data.loc[data['FCpayAmount'] != 0]
    data = data.drop(labels=['工资', '社保', '公积金', '奖金'], axis=1)

    return data


# 4.1，工资关联工时
def socialsecurity_merge_rddtail(socialsecurity, detail, condition):
    socialsecurity = socialsecurity.drop('FRdProject', axis=1)
    socialsecurity = socialsecurity.drop('FHightechDept', axis=1)
    data = pd.merge(socialsecurity, detail, how='inner', on=condition)
    data = data.reset_index(drop=True)

    data['FRdProject'] = data['FRdProject'].fillna('')

    data['FComAllSocialSecurityAmt'] = data['社保']
    data['FComAccumulationFundAmt'] = data['公积金']

    if '奖金' in data.columns:
        data['奖金'] = data['奖金'].fillna(0)
    else:
        data['奖金'] = 0

    data = data.drop(labels=['工资', '社保', '公积金', '奖金'], axis=1)
    data['FAccount'] = '研发支出'
    return data


# 4.2，社保：总额 - 研发
def socialsecurity_orther(socialsecurity, detail, condition):
    detail = detail.drop('FHightechDept', axis=1)
    data = pd.merge(socialsecurity, detail, how='left', on=condition)
    data = data.reset_index(drop=True)

    data['FRdProject'] = data['FRdProject'].fillna('')
    data['社保'] = data['社保'].fillna(0)
    data['公积金'] = data['公积金'].fillna(0)

    data['FComAllSocialSecurityAmt'] = data['FComAllSocialSecurityAmt'] - data['社保']
    data['FComAccumulationFundAmt'] = data['FComAccumulationFundAmt'] - data['公积金']

    if '奖金' in data.columns:
        data['奖金'] = data['奖金'].fillna(0)
    else:
        data['奖金'] = 0

    data = data.loc[data['FComAllSocialSecurityAmt'] != 0]
    data = data.loc[data['FComAccumulationFundAmt'] != 0]

    data = data.drop(labels=['工资', '社保', '公积金', '奖金'], axis=1)

    return data


def to_sql(FToken, data, table):
    app = RdClient(token=FToken)
    data.loc[:, 'FDate'] = data.loc[:, 'FDate'].astype(str)
    data.loc[:, ['FBankType', 'FOldDept', 'FRdProject']] = data.loc[:, ['FBankType', 'FOldDept', 'FRdProject']].fillna(
        '')

    data.iloc[:, 4:-11] = data.iloc[:, 4:-11].astype('float64')
    # if billtype == '工资':
    #     data.loc[:, 5:-10] = data.loc[:, 5:-10] .astype('float64')
    # elif billtype == '社保':
    #     data.loc[:, 5:-10] = data.loc[:, 5:-10].astype('float64')

    if not data.empty:
        datarows = data.shape[0]
        limit = 100
        pages = datarows // limit

        for page in range(1, (pages + 2)):

            datatask = data[(int(page) - 1) * int(limit): (int(page) * int(limit))]

            datatask = datatask.fillna('')
            keys = tuple(datatask.columns)
            keys = "(" + ','.join(keys) + ")"
            values = ''
            for row in datatask.itertuples():
                value = list(row)[1:]
                value = tuple(value)
                value = str(value)
                values = values + value + ','
            values = values[:-1]
            sql = f"""insert into {table} {keys} values {values}"""
            # print(sql)
            try:
                app.insert(sql)
            except Exception as e:
                print(e)
                return False
    return True


# 工时处理
def rddetail_deal(FToken):
    # 查研发工时
    rddetail = sql_query(FToken, 'rds_hrv_ods_ds_rddetail')

    # 工时表列转行，将工资类别（工资，社保，公积金）作为列名，研发项目维度
    detailcondition = ['FNumber', 'FYear', 'FMonth', 'FHightechDept', 'FExpenseOrgID', 'FTaxDeclarationOrg',
                       'FRdProject', 'FOldDept', 'FSalaryType']
    rddetaily = detail_specification(rddetail, detailcondition, 'FRdProjectCost')

    # 工时表列转行，将工资类别（工资，社保，公积金）作为列名，部门维度
    detailorthercondition = ['FNumber', 'FYear', 'FMonth', 'FHightechDept', 'FExpenseOrgID', 'FTaxDeclarationOrg',
                             'FOldDept', 'FSalaryType']
    rddetailorther = detail_specification(rddetail, detailorthercondition, 'FRdProjectCost')

    return rddetaily, rddetailorther


def salary_deal(FToken, rddetaily, rddetailorther, rddetailcondition):
    # 查工资
    salary = sql_query(FToken, 'rds_hrv_ods_ds_salary')
    salary['FAccount'] = salary['FAccount'].replace('研发费用', '研发支出')

    # 研发：工资关联工时
    salaryy = salary_merge_rddtail(salary, rddetaily, rddetailcondition)
    # 非研发 = 总额 - 研发
    salaryn = salary_orther(salary, rddetailorther, rddetailcondition)
    # 合并研发非研发
    salaryall = pd.concat([salaryy, salaryn])
    # 提出工资为0部分
    salaryres = salaryall.loc[salaryall['FCpayAmount'] != 0]
    # 按单据编号，原部门分组，保留第一行总额，其他行为0
    salaryres['FSeqNew'] = salaryres.groupby(['FNumber', 'FOldDept'])['FRdProject'].rank().astype('int64')
    salaryres.loc[salaryres['FSeqNew'] != 1, ['FFixdCost', 'FScraprateCost', 'FSocialSecurityAmt',
                                          'FAccumulationFundAmt',
                                          'FOtherAmt', 'FIncomeTaxAmt', 'FActualAmount']] = 0

    salaryresstd = sql_query(FToken, 'rds_hrv_std_ds_salary')

    # salaryresdiff = pd.concat([salaryres, salaryresstd, salaryresstd]).drop_duplicates(keep=False)
    if not salaryresstd.empty:
        salarystdfnumber = salaryresstd[['FNumber']].drop_duplicates(keep='first')
        res = pd.merge(salaryres, salarystdfnumber['FNumber'], on='FNumber', how='left', indicator=True)
        res = res[res._merge == 'left_only']
        res = res.drop('_merge', axis=1)
    else:
        res = salaryres


    salaryresdiff = res.reset_index(drop=True)
    salaryresdiff['indexid'] = range(len(salaryresdiff))
    salaryresdiff['FSeqNew'] = salaryresdiff.groupby(['FNumber'])['indexid'].rank().astype('int64')
    salaryresdiff = salaryresdiff.drop('indexid', axis=1)

    to_sql(FToken, salaryresdiff, 'rds_hrv_std_ds_salary')

    return salaryres


# 社保处理
def socialsecurity_deal(FToken, rddetaily, rddetailorther, rddetailcondition):
    # 查社保公积金
    socialsecurity = sql_query(FToken, 'rds_hrv_ods_ds_socialsecurity')
    socialsecurity['FAccount'] = socialsecurity['FAccount'].replace('研发费用', '研发支出')

    # 研发：工资关联工时
    socialsecurityy = socialsecurity_merge_rddtail(socialsecurity, rddetaily, rddetailcondition)
    # 非研发 = 总额 - 研发
    socialsecurityn = socialsecurity_orther(socialsecurity, rddetailorther, rddetailcondition)
    # 合并研发非研发
    socialsecurityall = pd.concat([socialsecurityy, socialsecurityn])
    socialsecurityall.loc[:, ['FComAllSocialSecurityAmt', 'FComAccumulationFundAmt']] = socialsecurityall.loc[:, ['FComAllSocialSecurityAmt', 'FComAccumulationFundAmt']].fillna(0)
    # 提出工资为0部分
    socialsecurityres = socialsecurityall.loc[(socialsecurityall['FComAllSocialSecurityAmt'] != 0) | (socialsecurityall['FComAccumulationFundAmt'] != 0)]
    # 按单据编号，原部门分组，保留第一行总额，其他行为0
    socialsecurityres['FSeqNew'] = socialsecurityres.groupby(['FNumber', 'FOldDept'])['FRdProject'].rank().astype('int64')
    socialsecurityres.loc[socialsecurityres['FSeqNew'] != 1, ['FComPensionBenefitsAmt', 'FComMedicareAmt', 'FComMedicareOfSeriousAmt',
     'FComDisabilityBenefitsAmt',
     'FComOffsiteElseAmt', 'FComWorklessInsuranceAmt', 'FComInjuryInsuranceAmt',
     'FComMaternityInsuranceAmt',
     'FComAllSoSeAcFuAmt', 'FEmpPensionBenefitsAmt', 'FEmpMedicareAmt',
     'FEmpMedicareOfSeriousAmt', 'FEmpWorklessInsuranceAmt',
     'FEmpAllSocialSecurityAmt', 'FEmpAccumulationFundAmt', 'FEmpAllSoSeAcFuAmt',
     'FAllSocialSecurityAmt',
     'FAllAccumulationFundAmt', 'FAllAmount', 'FManagementAmount']] = 0

    socialsecuritystd = sql_query(FToken, 'rds_hrv_std_ds_socialsecurity')
    # socialsecuritydiff = pd.concat([socialsecurityres, socialsecuritystd, socialsecuritystd]).drop_duplicates(keep=False)
    # socialsecuritydiff = socialsecuritydiff.reset_index(drop=True)

    # 去重
    if not socialsecuritystd.empty:
        socialsecuritystd = socialsecuritystd[['FNumber']].drop_duplicates(keep='first')
        res = pd.merge(socialsecurityres, socialsecuritystd['FNumber'], on='FNumber', how='left', indicator=True)
        res = res[res._merge == 'left_only']
        res = res.drop('_merge', axis=1)
    else:
        res = socialsecurityres

    # 编号
    socialsecuritydiff = res.reset_index(drop=True)
    socialsecuritydiff['indexid'] = range(len(socialsecuritydiff))
    socialsecuritydiff['FSeqNew'] = socialsecuritydiff.groupby(['FNumber'])['indexid'].rank().astype('int64')
    socialsecuritydiff = socialsecuritydiff.drop('indexid', axis=1)

    to_sql(FToken, socialsecuritydiff, 'rds_hrv_std_ds_socialsecurity')

    return socialsecurityres


# 总函数
def main(FToken):
    # 工时处理
    rddetaily, rddetailorther = rddetail_deal(FToken)

    # 数据源关联工时的关联关系
    rddetailcondition = ['FNumber', 'FYear', 'FMonth', 'FOldDept', 'FExpenseOrgID', 'FTaxDeclarationOrg']

    # 工资处理
    salaryres = salary_deal(FToken, rddetaily, rddetailorther, rddetailcondition)

    # 社保处理
    socialsecurityres = socialsecurity_deal(FToken, rddetaily, rddetailorther, rddetailcondition)

    # salaryres.to_excel('工资std.xlsx')
    # socialsecurityres.to_excel('社保std.xlsx')
    # return salaryres, socialsecurityres
    return '同步成功'


if __name__ == '__main__':
    FToken = '057A7F0E-F187-4975-8873-AF71666429AB'
    main(FToken)
