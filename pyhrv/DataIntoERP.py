#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pyrda.dbms.rds import RdClient
from k3cloud_webapi_sdk.main import K3CloudApiSdk
import json
from . import Main

def Fentry_model(data):
    '''
    分录数据格式
    :return:
    '''

    entry={
            "FEXPLANATION": str(data["FNotes"]),

            "FACCOUNTID": {
                "FNumber": str(data["FSubjectNumber"])
            },

            "FDetailID": {
                "FDETAILID__FFlex5": {
                    # 部门
                    "FNumber": str(data["FDeptNumber"])
                },
                "FDETAILID__FF100015": {
                    # 责任中心
                    "FNumber": str(data["FWorkCenterNumber"])
                },
                "FDETAILID__FF100016": {
                    # 重分类
                    "FNumber": str(data["FAcctreClassNumber"])
                },
                "FDETAILID__FF100002": {
                    # 银行账号
                    "FNumber": str(data["FBankAccount"])
                },
                "FDETAILID__FF100005": {
                    # 研发项目
                    "FNumber": str(data["FProjectNumber"])
                },
                "FDETAILID__FF100006": {
                    # 往来单位
                    "FNumber": str(data["FDealingUnitNumber"])
                },
                "FDETAILID__FFlex4": {
                    # 供应商
                    "FNumber": str(data["FSupplierNumber"])
                }
            },
            "FCURRENCYID": {
                "FNumber": "PRE001"
            },
            "FEXCHANGERATETYPE": {
                "FNumber": "HLTX01_SYS"
            },
            "FEXCHANGERATE": 1.0,
            "FDEBIT": data["allamountBorrow"],
            # 借方金额
            "FCREDIT": data["allamountLoan"]
            #贷方金额
        }


    return entry


def data_splicing(data):
    '''
    将订单内的物料进行遍历组成一个列表，然后将结果返回给 FSaleOrderEntry
    :param data:
    :return:
    '''

    list=[]

    for i in data:

        result=Fentry_model(i)

        if result:

            list.append(result)

        else:

            return []

    return list


def model(api_sdk,data,app):
    '''
    数据格式
    :return: 
    '''

    for i in data:
    
        json_model={
                "Model": {
                    "FVOUCHERID": 0,
                    "FAccountBookID": {
                        "FNumber": str(i[0]['FAccountBookID'])
                    },
                    "FDate": str(i[0]['FDate']),
                    "FBUSDATE": str(i[0]['FDate']),
                    # "FDate": "2023-05-31",
                    # "FBUSDATE": "2023-05-31",
                    "FVOUCHERGROUPID": {
                        "FNumber": "PRE001"
                    },
                    "FVOUCHERGROUPNO": "540",
                    "FISADJUSTVOUCHER": False,
                    "FYEAR": int(float(i[0]['FYear'])),
                        "FDocumentStatus": "Z",
                    "FSourceBillKey": {
                        "FNumber": "78050206-2fa6-40e3-b7c8-bd608146fa38"
                    },
                    "FPERIOD": int(float(i[0]['FMonth'])),
                    # "FPERIOD": 5,
                    "FISREDWRITEOFF": False,
                    "FHasAttachments": False,
                    "FEntity": data_splicing(i)
                }
        }

        res=json.loads(api_sdk.Save("GL_VOUCHER",json_model))

        print(type(res['Result']['ResponseStatus']['IsSuccess']))


        if bool(res['Result']['ResponseStatus']['IsSuccess']):

            log_upload(app,str(i[0]['FBillNO']),"1","记账凭证已生成")

        else:
            log_upload(app, str(i[0]['FBillNO']), "2", "记账凭证生成失败"+res['Result']['ResponseStatus']['Errors'][0]['Message'])


    
def dataSourceBillNo_query(app,FYear,FMonth):
    '''
    数据源查询
    :return:
    '''

    FYear=str(FYear)+".0"

    FMonth =str(FMonth) + ".0"

    sql=f"select distinct FBillNO from rds_hrv_ods_ds_middleTable where FIsdo=0 and FYear='{FYear}' and FMonth='{FMonth}'"

    res=app.select(sql)

    return res



def getClassfyData(app3, code):
    '''
    获得分类数据
    :param app2:
    :param code:
    :return:
    '''


    sql = f"""select * from rds_hrv_ods_ds_middleTable where FBillNO='{code['FBillNO']}'"""

    print(sql)

    res = app3.select(sql)

    return res


def fuz(app3, codeList):
    '''
    通过编码分类，将分类好的数据装入列表
    :param app2:
    :param codeList:
    :return:
    '''


    singleList = []

    for i in codeList:

        data = getClassfyData(app3, i)

        singleList.append(data)

    return singleList


def classification_process(app3, data):
    '''
    将编码进行去重，然后进行分类
    :param data:
    :return:
    '''

    res = fuz(app3, data)

    return res


def log_upload(app,FNumber,FIsdo,FMessage):
    '''
    更新ods表状态
    :param app:
    :param FNumber:
    :return:
    '''

    sql=f"""update a set a.FIsdo='{FIsdo}',a.FMessage='{FMessage}'  
    from rds_hrv_ods_ds_middleTable a where a.FBillNO='{FNumber}'"""

    app.update(sql)



def voucher_save(FToken,FYear,FMonth,option1):

    app = RdClient(FToken)

    api_sdk = K3CloudApiSdk()

    # 新账套

    Main.middleTableSrctoOds(app)

    # Main.src_clear(app,"")


    api_sdk.InitConfig(option1[0]['acct_id'], option1[0]['user_name'], option1[0]['app_id'],
                       option1[0]['app_sec'], option1[0]['server_url'])

    FBillNoRes=dataSourceBillNo_query(app,FYear,FMonth)

    if FBillNoRes != []:

        dataSource = classification_process(app, FBillNoRes)

        model(api_sdk=api_sdk,data=dataSource,app=app)



