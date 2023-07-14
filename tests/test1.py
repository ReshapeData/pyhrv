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


# 工资关联研发工时
def salary_merge_rddtail(salary, detail, condition):
    salary = salary.loc[(salary['FAccount'] == '研发支出') & (salary['FCategoryType'].str.contains('计提'))]
    detail = detail.loc[(detail['FOldDept'] != '生产部')]
    salary = salary.drop('FOldDept', axis=1)
    data = pd.merge(salary, detail, how='left', on=condition)
    data = data.reset_index(drop=True)

    data['FRdProject'] = data['FRdProject'].fillna('')

    data['工资'] = data['工资'].fillna(0)
    data['奖金'] = data['奖金'].fillna(0)
    data['rnk'] = data.loc[(data['工资'] != 0) | (data['奖金'] != 0)].groupby(['FNumber', 'FHightechDept'])[
        'FRdProject'].rank().astype('int64')
    data['rnk'].fillna(0)
    data.loc[(data['rnk'] != 1) & (data['rnk'] != 0), ['FFixdCost', 'FScraprateCost', 'FSocialSecurityAmt',
                                                       'FAccumulationFundAmt',
                                                       'FOtherAmt', 'FIncomeTaxAmt', 'FActualAmount']] = 0
    data = data.drop(labels='rnk', axis=1)
    return data


# 工资社保生产部的研发部分
def salary_merge_nonrddtail(salary, detail, nonrddetailcondition):
    salary = salary.loc[(salary['FAccount'] == '生产成本')]
    salary.loc[salary['FAccount'] == '生产成本', ['FOldDept']] = '生产部'
    detail = detail.loc[(detail['FOldDept'] == '生产部')]
    salary = salary.drop('FHightechDept', axis=1)
    data = pd.merge(salary, detail, how='left', on=nonrddetailcondition)
    data = data.reset_index(drop=True)

    data.loc[data.index.tolist(),"FHightechDept"]="研发部"

    data.loc[data.index.tolist(), "FAccount"] = "研发支出"

    return data

# 生产成本中的成本处理   总额-研发支持
def productionCost_deal(df,totalAmountDF):
    '''
    工资生产成本中的成本处理   总额-研发支持
    :return:
    '''

    newdf=df.loc[df.FAccount=="生产成本"]

    res=pd.merge(newdf,totalAmountDF,how="left",on="FNumber")

    res['FCpayAmount2'].fillna(0, inplace=True)

    res['FCpayAmount']=res['FCpayAmount']-res['FCpayAmount2']


    return res



def productionCostResearch_query(df):
    '''
    工资生产成本中研发支出的费用
    :param df:
    :return:
    '''

    newdf=df.groupby(["FNumber"])["FCpayAmount"].sum()

    # res=pd.DataFrame(newdf,columns=["FBillNO","FCpayAmount"])
    res = newdf.to_frame().reset_index()

    res.rename(columns={"FCpayAmount":"FCpayAmount2"},inplace=True)

    return res



def productionCost_deal2(df,totalAmountDF):
    '''
    社保表生产成本中的成本处理   总额-研发支出
    :return:
    '''

    newdf=df.loc[df.FAccount=="生产成本"]

    res=pd.merge(newdf,totalAmountDF,how="left",on="FNumber")

    res['FComAllSocialSecurityAmt1'].fillna(0, inplace=True)
    res['FComAccumulationFundAmt1'].fillna(0, inplace=True)

    res['FComAllSocialSecurityAmt']=res['FComAllSocialSecurityAmt']-res['FComAllSocialSecurityAmt1']
    res['FComAccumulationFundAmt'] = res['FComAccumulationFundAmt'] - res['FComAccumulationFundAmt1']


    return res


def productionCostResearch_query2(df):
    '''
    社保表生产成本中研发支出的费用
    :param df:
    :return:
    '''

    newdf1=df.groupby(["FNumber"])["FComAllSocialSecurityAmt"].sum()
    newdf2 = df.groupby(["FNumber"])["FComAccumulationFundAmt"].sum()
    newdf1.to_frame().reset_index()
    newdf2.to_frame().reset_index()



    res=pd.merge(newdf1,newdf2,how="inner",on="FNumber")

    res.rename(columns={"FComAllSocialSecurityAmt":"FComAllSocialSecurityAmt1",
                        "FComAccumulationFundAmt":"FComAccumulationFundAmt1"},
               inplace=True)

    return res


# 社保关联研发工时
def socialsecurity_merge_rddtail(socialsecurity, detail, condition):
    socialsecurity = socialsecurity.loc[(socialsecurity['FAccount'] == '研发支出') & (socialsecurity['FCategoryType'].str.contains('计提'))]
    detail = detail.loc[(detail['FOldDept'] != '生产部')]
    socialsecurity = socialsecurity.drop('FOldDept', axis=1)
    data = pd.merge(socialsecurity, detail, how='left', on=condition)
    data['FRdProject'] = data['FRdProject'].fillna('')
    data['社保'] = data['社保'].fillna(0)
    data['公积金'] = data['公积金'].fillna(0)
    data['rnk1'] = data.loc[data['社保'] != 0].groupby(['FNumber', 'FHightechDept'])['FRdProject'].rank().astype(
        'int64')
    data['rnk2'] = data.loc[data['公积金'] != 0].groupby(['FNumber', 'FHightechDept'])['FRdProject'].rank().astype(
        'int64')
    data['rnk1'] = data['rnk1'].fillna(0)
    data['rnk2'] = data['rnk2'].fillna(0)
    data['rnk1'] = data['rnk1'].astype('int64')
    data['rnk2'] = data['rnk2'].astype('int64')
    data.loc[(data['rnk1'] != 1) & (data['rnk1'] != 0) | (data['rnk2'] != 1) & (data['rnk2'] != 0),
    ['FComPensionBenefitsAmt', 'FComMedicareAmt', 'FComMedicareOfSeriousAmt',
     'FComDisabilityBenefitsAmt',
     'FComOffsiteElseAmt', 'FComWorklessInsuranceAmt', 'FComInjuryInsuranceAmt',
     'FComMaternityInsuranceAmt',
     'FComAllSoSeAcFuAmt', 'FEmpPensionBenefitsAmt', 'FEmpMedicareAmt',
     'FEmpMedicareOfSeriousAmt', 'FEmpWorklessInsuranceAmt',
     'FEmpAllSocialSecurityAmt', 'FEmpAccumulationFundAmt', 'FEmpAllSoSeAcFuAmt',
     'FAllSocialSecurityAmt',
     'FAllAccumulationFundAmt', 'FAllAmount', 'FManagementAmount']] = 0
    data = data.drop(labels='rnk1', axis=1)
    data = data.drop(labels='rnk2', axis=1)
    return data


def main1(FToken):
    # 查工资
    salary = sql_query(FToken, 'rds_hrv_ods_ds_salary where (FYear=2023 and FMonth!=2) or (FYear!=2023)')
    salary['FAccount'] = salary['FAccount'].replace('研发费用', '研发支出')
    salary = salary.drop('FRdProject', axis=1)

    # 查社保公积金
    socialsecurity = sql_query(FToken, 'rds_hrv_ods_ds_socialsecurity where (FYear=2023 and FMonth!=2) or (FYear!=2023)')
    socialsecurity['FAccount'] = socialsecurity['FAccount'].replace('研发费用', '研发支出')
    socialsecurity = socialsecurity.drop('FRdProject', axis=1)

    detailcondition = ['FNumber', 'FYear', 'FMonth', 'FHightechDept', 'FExpenseOrgID', 'FTaxDeclarationOrg',
                       'FRdProject', 'FOldDept', 'FSalaryType']
    rddetailcondition = ['FNumber', 'FYear', 'FMonth', 'FHightechDept', 'FExpenseOrgID', 'FTaxDeclarationOrg']
    nonrddetailcondition = ['FNumber', 'FYear', 'FMonth', 'FOldDept', 'FExpenseOrgID', 'FTaxDeclarationOrg']

    # 查研发工时
    rddetail = sql_query(FToken, 'rds_hrv_ods_ds_rddetail')
    rddetail = detail_specification(rddetail, detailcondition, 'FRdProjectCost')



    # 工资
    # 非生产部研发支出的
    salary_rddetail = salary_merge_rddtail(salary, rddetail, rddetailcondition)
    salary_rddetail['工资'] = salary_rddetail['工资'].fillna(0)
    salary_rddetail['奖金'] = salary_rddetail['奖金'].fillna(0)
    salary_rddetail['FCpayAmount'] = salary_rddetail['工资'] + salary_rddetail['奖金']
    salary_rddetail = salary_rddetail.loc[(salary_rddetail['工资'] != 0) | (salary_rddetail['奖金'] != 0)]
    salary = salary.loc[(salary['FAccount'] != '研发支出') | (
            (salary['FAccount'] == '研发支出') & salary['FCategoryType'].str.contains('发放'))]

    # 生产部研发支出的
    salary_nonrddetail = salary_merge_nonrddtail(salary, rddetail, nonrddetailcondition)
    salary_nonrddetail['工资'] = salary_nonrddetail['工资'].fillna(0)
    salary_nonrddetail['奖金'] = salary_nonrddetail['奖金'].fillna(0)
    salary_nonrddetail['FCpayAmount'] = salary_nonrddetail['工资'] + \
                                        salary_nonrddetail['奖金']

    # 工资生产成本中的生产成本
    newdf=salary_nonrddetail.loc[salary_nonrddetail.FCpayAmount!=0]
    productionCostResearch=productionCostResearch_query(newdf)
    productionCost=productionCost_deal(salary,productionCostResearch)


    # 不需要处理的
    salary = salary.loc[(salary['FAccount'] != '生产成本')]
    salaryrrddetail = pd.concat([salary_rddetail, salary_nonrddetail,productionCost])
    salaryrrddetail = salaryrrddetail.reset_index(drop=True)
    salaryrrddetail = salaryrrddetail.drop(labels=['工资', '社保', '公积金', '奖金',"FCpayAmount2"], axis=1)
    salaryres = pd.concat([salary, salaryrrddetail])
    salaryres = salaryres.reset_index(drop=True)
    salaryres['FRdProject'] = salaryres['FRdProject'].replace('rds', '')
    salaryres.loc[:, ['FHightechDept', 'FBankType', 'FOldDept', 'FRdProject']] = salaryres.loc[:,
                                                                                 ['FHightechDept', 'FBankType',
                                                                                  'FOldDept',
                                                                                  'FRdProject']].fillna('')
    salaryresods = sql_query(FToken, 'rds_hrv_std_ds_salary')
    salaryresdiff = pd.concat([salaryres, salaryresods, salaryresods]).drop_duplicates(keep=False)
    salaryresdiff = salaryresdiff.reset_index(drop=True)



    # 社保公积金
    # 社保研发只出 计提
    socialsecurity_rddetail = socialsecurity_merge_rddtail(socialsecurity, rddetail, rddetailcondition)
    socialsecurity_rddetail['社保'] = socialsecurity_rddetail['社保'].fillna(0)
    socialsecurity_rddetail['公积金'] = socialsecurity_rddetail['公积金'].fillna(0)
    socialsecurity_rddetail['FComAllSocialSecurityAmt'] = socialsecurity_rddetail['社保']
    socialsecurity_rddetail['FComAccumulationFundAmt'] = socialsecurity_rddetail['公积金']
    socialsecurity_rddetail = socialsecurity_rddetail.loc[
        (socialsecurity_rddetail['社保'] != 0) | (socialsecurity_rddetail['公积金'] != 0)]
    socialsecurity = socialsecurity.loc[(socialsecurity['FAccount'] != '研发支出')
                                        | ((socialsecurity['FAccount'] == '研发支出') & socialsecurity[
        'FCategoryType'].str.contains('发放'))]


    # 社保表生产成本的研发支出
    socialsecurity_nonrddetail = salary_merge_nonrddtail(socialsecurity, rddetail, nonrddetailcondition)
    socialsecurity_nonrddetail['社保'] = socialsecurity_nonrddetail['社保'].fillna(0)
    socialsecurity_nonrddetail['公积金'] = socialsecurity_nonrddetail['公积金'].fillna(0)
    socialsecurity_nonrddetail['FComAllSocialSecurityAmt'] = socialsecurity_nonrddetail['社保']
    socialsecurity_nonrddetail['FComAccumulationFundAmt'] = socialsecurity_nonrddetail['公积金']


    # 社保生产成本中的生产成本
    socialsecurityNewdf=socialsecurity_nonrddetail.loc[(socialsecurity_nonrddetail.FComAllSocialSecurityAmt!=0)
                                         | (socialsecurity_nonrddetail.FComAccumulationFundAmt!=0)]
    productionCostResearch=productionCostResearch_query2(socialsecurityNewdf)
    productionCost=productionCost_deal2(socialsecurity,productionCostResearch)


    # 不需要修改的
    socialsecurity = socialsecurity.loc[(socialsecurity['FAccount'] != '生产成本')]
    socialsecurityrddetail = pd.concat([socialsecurity_rddetail,socialsecurity_nonrddetail,productionCost])
    socialsecurityrddetail = socialsecurityrddetail.reset_index(drop=True)


    socialsecurityrddetail = socialsecurityrddetail.drop(labels=['工资', '社保', '公积金', '奖金',"FComAllSocialSecurityAmt1","FComAccumulationFundAmt1"], axis=1)

    socialsecurityres = pd.concat([socialsecurity, socialsecurityrddetail])
    socialsecurityres = socialsecurityres.reset_index(drop=True)

    socialsecurityres['FRdProject'] = socialsecurityres['FRdProject'].replace('rds', '')
    socialsecurityres.loc[:, ['FHightechDept', 'FBankType', 'FOldDept', 'FRdProject']] = socialsecurityres.loc[:,
                                                                                         ['FHightechDept', 'FBankType',
                                                                                          'FOldDept',
                                                                                          'FRdProject']].fillna('')
    socialsecurityods = sql_query(FToken, 'rds_hrv_std_ds_socialsecurity')
    socialsecuritydiff = pd.concat([socialsecurityres, socialsecurityods, socialsecurityods]).drop_duplicates(
        keep=False)
    socialsecuritydiff = socialsecuritydiff.reset_index(drop=True)


    to_sql(FToken, salaryresdiff, 'rds_hrv_std_ds_salary')
    to_sql(FToken, socialsecuritydiff, 'rds_hrv_std_ds_socialsecurity')
    return salaryres, socialsecurityres





def to_sql(FToken, data, table):
    app = RdClient(token=FToken)
    data.loc[:, 'FDate'] = data.loc[:, 'FDate'].astype(str)
    data.loc[:, ['FBankType', 'FOldDept', 'FRdProject']] = data.loc[:, ['FBankType', 'FOldDept', 'FRdProject']].fillna(
        '')

    data.iloc[:, 5:-9] = data.iloc[:, 5:-9].astype('float64')
    # if billtype == '工资':
    #     data.loc[:, 5:-10] = data.loc[:, 5:-10] .astype('float64')
    # elif billtype == '社保':
    #     data.loc[:, 5:-10] = data.loc[:, 5:-10].astype('float64')

    if not data.empty:
        datarows = data.shape[0]
        limit = 100
        pages = datarows // limit
        for page in range(1, pages + 2):

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
                print(0)

            except Exception as e:
                print(1)
                return False
    return True

if __name__ == '__main__':
    FToken = '057A7F0E-F187-4975-8873-AF71666429AB'
    main(FToken)


