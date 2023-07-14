from pyrda.dbms.rds import RdClient
import pandas as pd
import numpy as np

app = RdClient("057A7F0E-F187-4975-8873-AF71666429AB")


sql="select * from rds_hrv_src_ds_middleTable"
res=app.select(sql)

df=pd.DataFrame(res)

df["allamountBorrow"]=df["allamountBorrow"].replace("","0")
df["allamountLoan"] = df["allamountLoan"].replace("","0")
df["allamountBorrow"] = df["allamountBorrow"].astype(np.float64)
df["allamountLoan"] = df["allamountLoan"].astype(np.float64)

dfB=df.groupby(["FBillNO"])["allamountBorrow"].sum()

dfB=dfB.reset_index()

dfL=df.groupby(["FBillNO"])["allamountLoan"].sum()

dfL=dfL.reset_index()

dfRes=pd.merge(dfB,dfL,how="inner",on="FBillNO")

dfRes["allamountBorrow"]=dfRes["allamountBorrow"].round(2)

dfRes["allamountLoan"]=dfRes["allamountLoan"].round(2)


print(dfRes[dfRes["allamountBorrow"]!=dfRes["allamountLoan"]])

# result=dfRes[dfRes["allamountBorrow"]!=dfRes["allamountLoan"]]["FBillNO"].tolist()
#
# for i in result:
#
#     sql=f"update a set a.FIsdo=2 , a.FMessage=a.FMessage+'借贷不平，请检查！' from rds_hrv_src_ds_middleTable a where a.FBillNO='{i}'"
#
#     res=app.update(sql)







