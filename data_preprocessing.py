"""
# preprocessing the csv file
# replacing all the "[",",",";","{","}","(",")","\n","\t","=","]" to "_"
"""
import pandas as pd

chars = ["[",",",";","{","}","(",")","\n","\t","=","]"," ","."]
file = "combined_subject_vs_lab_with_sdoh_12Aug2021.csv"
file_copy = "combined_subject_vs_lab_with_sdoh_copy.csv"
#file_copy = "HT_Pros_test_data_new_copy.csv"
#file = "HT_Pros_test_data_2020May09_1210.csv"
"""
df = pd.read_csv(file)
print(df.shape)

count = 0
i = 1
for col in df.columns:
    s = list(col)
    for j in range(len(s)):
        if s[j] in chars:
            s[j] = "_"
    new_col = "".join(s)
    if new_col != col:
        count += 1
        print(str(i) + "th column: "+ col + "->" + new_col)
        df.rename(columns={col:new_col},inplace=True)
    i += 1
print("now checking...")
for col in df.columns:
    for j in range(len(col)):
        if col[j] in chars:
            print("WRONG")
print("checking complete")
print(str(count) + " changes have been made")
print("writing to file...")
df.to_csv(file_copy,index=False)

print(df.shape)

df_copy = pd.read_csv(file_copy,nrows=1)
for col in df_copy.columns:
    s = list(col)
    for j in range(len(s)):
        if s[j] == " ":
            print("WRONG")

"""
"""
count = 0
i = 0
df_column = pd.read_csv(file,nrows=1)
df_data = pd.read_csv(file,skiprows=1)
print("read data done")
read_columns = []
duplication = []
for col in df_column.columns:
    s = list(col)
    for j in range(len(s)):
        if s[j] in chars:
            s[j] = "_"
    new_col = "".join(s)
    if new_col != col:
        if new_col.lower() in read_columns:
            duplication.append(i)
        else:
            read_columns.append(new_col.lower())
            count += 1
            print(str(i) + "th column: "+ col + "->" + new_col.lower())
            df_column.rename(columns={col:new_col.lower()},inplace=True)
    i += 1
print(str(count) + " changes have been made")
print(len(duplication))
df_column = df_column.drop(df_column.columns[duplication],axis=1)
df_data = df_data.drop(df_data.columns[duplication],axis=1)
print("now checking...")
for col in df_column.columns:
    for j in range(len(col)):
        if col[j] in chars:
            print("WRONG")
print("checking complete")
print("writing column name to file...")
df_column.to_csv(file_copy,index=False)
print("writing data to file...")
df_data.to_csv(file_copy,index=False,header=False,mode="a")
print("success")
print("column shape: ")
print(df_column.shape)
print(df_data.shape)
"""
#print("second checking...")

print("fill na with median")
df_copy = pd.read_csv(file_copy)
print(df_copy.shape)
df_copy = df_copy.drop(["county","state","ppv_RespFail_flag","combnd_ppv_flag"],axis=1)
df_copy.dropna(thresh = int(0.5 * df_copy.shape[0]),axis=1,inplace=True)
print(df_copy.shape)
#df_columns = list(df_copy.columns)
#df_columns.remove("patient_id")
df_copy = df_copy.fillna(df_copy.median())
print(df_copy.shape)
print("na filled")
df_copy.to_csv("combined_subject_vs_lab_with_sdoh_copy_fillna.csv",index=False)
"""

df_copy = pd.read_csv(file_copy,nrows=1)
for col in df_copy.columns:
    s = list(col)
    for j in range(len(s)):
        if s[j] in chars:
            print(col)
            print("WRONG")
print("complete!")
"""

#check duplication
"""
print("checking duplication")
df = pd.read_csv(file_copy)
print(df.shape)


df = df.T.drop_duplicates().T
print(df.shape)
"""
#check shape
"""
df = pd.read_csv(file_copy,nrows=1000)
for col in df.columns:
    for row in df[col]:
        try:
            float(row)
        except:
            print(col + " : " + row)
            break
#df.to_csv("columns.csv",index=False)
"""
#check na
print("check nan...")
df = pd.read_csv("combined_subject_vs_lab_with_sdoh_copy_fillna.csv",nrows=20000)
print(df.isnull().values.any())
