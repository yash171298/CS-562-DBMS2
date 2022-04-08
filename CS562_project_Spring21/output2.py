# Import and Connecting DB
import psycopg2
import sys
from collections import defaultdict

try:
    connection = psycopg2.connect(user='postgres', password='yash1712', host='localhost', port='5432', database='DBMS2', )
    query = "SELECT * FROM sales"
    pointer = connection.cursor()
    pointer.execute(query)
    if (pointer.rowcount) == 0:
        print("Connection failed !")
        sys.exit(0)
                    
    dataItems = pointer.fetchall()
except(Exception, psycopg2.Error) as error:
    print("Connection failed to database ==>", error)
    sys.exit(0)

                    
# Initializing MF_Vector
MFVector = defaultdict(lambda: {'cust': '', 'prod': '', '1_avg_quant': 0.0, '1_sum_quant': 0.0, '1_count_quant': 0, '2_avg_quant': 0.0, '2_sum_quant': 0.0, '2_count_quant': 0})
MF_idx=0
MFMap = {}

# Initializing Map for MF_Structure
for i in range(len(dataItems)):
	temp = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],dataItems[i])}

	checker =str(temp['cust'])+str(temp['prod'])
	if (MFMap.get(checker))== None:
		MFVector[MF_idx]['cust']=temp['cust']
		MFVector[MF_idx]['prod']=temp['prod']
		MFMap[str(temp['cust'])]=MF_idx
		MFMap[str(temp['cust']) + str(temp['prod'])]=MF_idx
		MFMap[str(temp['prod']) + str(temp['cust'])]=MF_idx
		MFMap[str(temp['prod'])]=MF_idx
		MF_idx+=1 # new idx


# Updating for MF_Structure
for groupingvariable in ['1', '2']:
	if (groupingvariable) == '1':
		for i in range(len(dataItems)):
			temp = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],dataItems[i])}
			index = MFMap[str(temp['cust'])+str(temp['prod'])]
			if ((temp['cust']==MFVector[index]['cust']) and (temp['prod']==MFVector[index]['prod'])):
				MFVector[index]['1_count_quant']+=1
				MFVector[index]['1_sum_quant']+=temp['quant']
				MFVector[index]['1_avg_quant']=MFVector[index]['1_sum_quant']/MFVector[index]['1_count_quant']

	if (groupingvariable) == '2':
		for i in range(len(dataItems)):
			temp = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],dataItems[i])}
			index = MFMap[str(temp['prod'])]
			if ((temp['cust']!=MFVector[index]['cust']) and (temp['prod']==MFVector[index]['prod'])):
				MFVector[index]['2_count_quant']+=1
				MFVector[index]['2_sum_quant']+=temp['quant']
				MFVector[index]['2_avg_quant']=MFVector[index]['2_sum_quant']/MFVector[index]['2_count_quant']


# Outputing
print('='*100)
print('{:<8}|{:>17}|{:>17}|{:>17}|{:>17}|'.format('index','cust','prod','1_avg_quant','2_avg_quant'))
print('='*100)
# Having
output=0
for index in range(len(MFVector)):
	 if MFVector[index]['1_avg_quant']!=0:
			output += 1
			print(end='{:<6}|'.format(output))

			print('{:<17}|'.format(MFVector[index]['cust']), end='')
			print('{:<17}|'.format(MFVector[index]['prod']), end='')
			print('{:>17.2f}|'.format(MFVector[index]['1_avg_quant']), end='')
			print('{:>17.2f}|'.format(MFVector[index]['2_avg_quant']), end='')
			print('')
