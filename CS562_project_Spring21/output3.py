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
MFVector = defaultdict(lambda: {'prod': '', 'quant': 0.0, '1_count_prod': 0, '2_count_prod': 0})
MF_idx=0
MFMap = {}

# Initializing Map for MF_Structure
for i in range(len(dataItems)):
	temp = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],dataItems[i])}

	checker =str(temp['prod'])+str(temp['quant'])
	if (MFMap.get(checker))== None:
		MFVector[MF_idx]['prod']=temp['prod']
		MFVector[MF_idx]['quant']=temp['quant']
		MFMap[str(temp['prod'])]=MF_idx
		MFMap[str(temp['prod']) + str(temp['quant'])]=MF_idx
		MFMap[str(temp['quant']) + str(temp['prod'])]=MF_idx
		MFMap[str(temp['quant'])]=MF_idx
		MF_idx+=1 # new idx


# Updating for MF_Structure
for groupingvariable in ['1', '2']:
	if (groupingvariable) == '1':
		for i in range(len(dataItems)):
			temp = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],dataItems[i])}
			index = MFMap[str(temp['prod'])]
			if ((temp['prod']==MFVector[index]['prod'])):
				MFVector[index]['1_count_prod']+=1

	if (groupingvariable) == '2':
		for i in range(len(dataItems)):
			temp = {x[0]:x[1] for x in zip(['cust', 'prod', 'day', 'month', 'year', 'state', 'quant'],dataItems[i])}
			index = MFMap[str(temp['prod'])]
			if ((temp['prod']==MFVector[index]['prod']) and (temp['quant']<MFVector[index]['quant'])):
				MFVector[index]['2_count_prod']+=1


# Outputing
print('='*100)
print('{:<8}|{:>17}|{:>17}|'.format('index','prod','quant'))
print('='*100)
# Having
output=0
for index in range(len(MFVector)):
	 if MFVector[index]['2_count_prod']==MFVector[index]['1_count_prod']/2:
			output += 1
			print(end='{:<6}|'.format(output))

			print('{:<17}|'.format(MFVector[index]['prod']), end='')
			print('{:>17.2f}|'.format(MFVector[index]['quant']), end='')
			print('')
