import pandas as pd 
import numpy as np
import jinja2

directory = 'E:/A-Plan/A-Plan October 2020/'
file = 'filecountsOct20.csv'

df = pd.read_csv(directory + file)
table1 =  pd.pivot_table(df, index='List', columns='Product', values='Count', aggfunc='sum', margins=True, margins_name= 'Total')
table1.reset_index(inplace=True)
print(table1)
table1.to_csv(directory + "List_breakdown.csv")

clean = df[df['Type'] != 'remove']

table2 = pd.pivot_table(clean, index='List', columns='Product', values='Count', aggfunc='sum', margins=True, margins_name= 'Total')
#table1 = table1.style.format('{0:,.0f}')

#print(table1.render())
print(table2)

table2.to_csv(directory + "List_breakdown_postclean.csv")

table3 = pd.pivot_table(df, index=['List', 'Type'], columns='Product', values='Count', aggfunc='sum', margins=True, margins_name= 'Total')
#table3 = pd.pivot_table(df, index=['List', 'Type'], columns='Product', values='Count', aggfunc='sum')
table3['% of Type'] = (table3.Total /table3.groupby(level=0).Total.transform(sum) * 100).round(1).astype(str) + '%'
table3.reset_index(inplace=True)
print(table3)
print(list(table3))
#table3sum = table3.groupby('List').sum().reset_index().assign(type='Test').append(df,ignore_index=True)
#table3sum['Type'] = 'Total'
#table4 = table3.stack('Product')
#table3 = table3.append(table3sum)

#print(table4)

#df1 = df.groupby('List').sum()
#df1.index = pd.MultiIndex.from_arrays([df1.index + '_total', len(df1.index) * ['']])
table1['Type'] = 'Total'
table1['% of Type'] = ''
print(table3.shape)

table4 = table1[['List', 'Type', 'Home', 'LocalCar', 'NationalCar', 'Total', '% of Type']]
table4 = table4.astype({'Home' : 'int64','LocalCar': 'int64', 'NationalCar': 'int64', 'Total': 'int64'})

print(table4.dtypes)
print(table3.dtypes)
df1 = pd.concat([table3,table4],axis=0,ignore_index='True').sort_values(by=['List', 'Type'],ascending=False)
print(df1)
df1.to_csv(directory + "Cleaning_breakdown.csv", index=False)