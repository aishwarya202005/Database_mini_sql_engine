import csv
import sys
import os
import re
import sqlparse
import operator

sys.path.insert(0,os.getcwd())


columns=[]	# contains column names from the query
tables=[]	# contains table names from the query
attributes={}	# contains column names for each table from the metadata file
tablecontent=[]	# contains data of the tables mentioned in the query

######################################################################################

def ColumnNameList0(table1_name):
	global joincol
	
	for x in attributes.get(table1_name):
		joincol.append(x)

	global jointab
	
	for x in attributes.get(table1_name):
		jointab.append(table1_name+"."+x)

	global finalcolumns
	
	for x in attributes.get(table1_name):
		y=table1_name+"."+x
		finalcolumns[y]=y

######################################################################################

def TableContent0(table1_name):
	global tablecontent

	csvfile=open('./'+table1_name+'.csv')
	rows=csv.reader(csvfile)

	for row in rows:
		tup=[]
		for x in row:
			if(x[0]=='\"'):
				x=x[1:len(x)-1]
			tup.append(int(x))
	 	tablecontent.append(tup)
	csvfile.close()

######################################################################################

def LoadZeroTable():
	length=len(tables)
	global tablecontent

	table1_name=tables[0]
	
	ColumnNameList0(table1_name)
	TableContent0(table1_name)
	
######################################################################################

def ColumnNameList1(table_name):
	global joincol

	for x in attributes.get(table_name):
		joincol.append(x)

	global finalcolumns
	
	for x in attributes.get(table_name):
		y=table_name+"."+x
		finalcolumns[y]=y

	global jointab
	
	for x in attributes.get(table_name):
		jointab.append(table_name+"."+x)

######################################################################################

def TableContent1(table_name):
	global tablecontent

	tup=[]
	for z in tablecontent:
		csvfile=open('./'+table_name+'.csv')
		rows=csv.reader(csvfile)
		for row in rows:
			x=list(z)
			l=len(row)
			for j in range(0,l):
				y=row[j]
				if(y[0]=='\"'):
					y=y[1:len(y)-1]
				x.append(int(y))
			tup.append(x)
		csvfile.close()
	tablecontent=list(tup)

######################################################################################

def LoadTablesData():
	length=len(tables)
	global tablecontent

	LoadZeroTable()

	if(length<=1):
		return
	else:
		for i in range(1,length):
			table_name=tables[i]
			TableContent1(table_name)
			ColumnNameList1(table_name)

######################################################################################

def AggregateFunction(function_name, col_name):
	ind=jointab.index(col_name)
	output=tablecontent[0][ind]

	for i in range(1, len(tablecontent)):
		x=tablecontent[i][ind]
		if(function_name.lower()=='sum'):
			output+=x
		elif(function_name.lower()=='avg'):
			output+=x
		elif(function_name.lower()=='min'):
			output=min(output,x)
		elif(function_name.lower()=='max'):
			output=max(output,x)
	func=function_name.lower()
	if(func=='avg'):
		x=float(len(tablecontent)) 
		y=float(output)
		z=x/y
		output=float("{0:.3f}".format(z))

	return output


######################################################################################

def printoutput(dist):

	function_name=['avg','sum','min','max']
	func=""
	name=columns[0]
	if(len(tablecontent) == 0):
		print "No matching records."
	elif('(' in name):
		splitting= name.split('(')
		func=splitting[0].strip()

		name=splitting[1].split(')')[0].strip()
		if(name.isdigit()):
			print "Aggregate functions don't take constant value."
			sys.exit(0)
		else:
			name=checktype(name)
			if( func.lower() == 'dist' or func.lower() == 'distinct'): #NO NEED
				dist=1
			if(func in function_name):
				output=AggregateFunction(func, name)
				print func,"(",name,")"        
				print output

			elif(func.lower() == 'count'):
				print len(tablecontent )
			else:
				print "\"",func,"() \" function does not exist."
	else:
		column_name=[]
		column=[]
		if(name=='*'):
			for x in jointab:
				if( finalcolumns.get(x) == x):
					column_name.append(x)
					column.append(x)
		else:
			column_name=[checktype(z) for z in columns]
			for x in columns:
				if(x.isdigit()):
					print "Column names cannot be integer."
					sys.exit(0)
				else:
					n=checktype(x)
					while( finalcolumns.get(n) != n ):
						n=finalcolumns.get(n)
					column.append(n)

		indexlist=[]
		for x in column:
			ind=jointab.index(x)
			indexlist.append(ind)

		outputlist=[]
		for x in tablecontent:
			tup=[]
			for ind in indexlist:
				tup.append(x[ind])
			if(dist==1):
				if (tup not in outputlist):
					outputlist.append(tup)
			else:
				outputlist.append(tup)

		st=""
		for i in range(0, len(column_name)):
			if(i== (len(column_name)-1) ):
				st=st+""+column_name[i]				#Display Headers
			else:
				st=st+""+column_name[i]+"\t"
		print st
		for x in outputlist:
			st=""
			for i in range(0, len(x)):
				if(i== (len(x)-1) ):
					st=st+""+str(x[i])
				else:
					st=st+""+str(x[i])+"\t"
			print st

######################################################################################

def conditionchecking(opera, a, b):

	if(opera == '>'):
		return operator.gt(a,b)
	elif(opera == '>='):
		return operator.ge(a,b)
	elif(opera == '<'):
		return operator.lt(a,b)
	elif(opera == '<='):
		return operator.le(a,b)
	elif(opera == '<>' or opera == '!='):
		return operator.ne(a,b)
	elif(opera == '='):
		return operator.eq(a,b)
	else:
		print "Wrong operator used. Supported operators are : >, >=, <, <=, <>, !=, = ."
		sys.exit(0)


######################################################################################

def evaluate(operandlist,operatorlist, andop, orop):

	# print operandlist,operatorlist
	if(andop==1 and orop==1):
		print "Query should not contain both AND and OR. It may contain multiple ANDs or multiple ORs."
		sys.exit(0)

	if(len(operandlist) == 0):
		print "Need some condition in where clause."
		sys.exit(0)

	if( (len(operatorlist)*2) != len(operandlist)):
		print "Conditions are not properly formed in where clause."
		sys.exit(0)

	global tablecontent

	tup=[]
	for x in tablecontent:
		flag=True
		for i in range(0, len(operatorlist)):
			a= operandlist[i*2]
			b= operandlist[(i*2)+1]

			if(isinstance(a,str) and isinstance(b,str)):
				aind= jointab.index(a)
				bind= jointab.index(b)
				a_value=x[aind]
				b_value=x[bind]
			elif( isinstance(a,str) and isinstance(b,int)):
				aind= jointab.index(a)
				a_value=x[aind]
				b_value=b
			elif( isinstance(b,str) and isinstance(a,int)):
				bind= jointab.index(b)
				b_value=x[bind]
				a_value=a
			else:
				print "Both the operands in a condition cannot be a constant."
				sys.exit(0)

			if( conditionchecking(operatorlist[i],a_value, b_value) ):
				if(orop==1):
					flag=True
					break
			else:
				flag=False
				if(andop==1):						
					break
		if(flag):
			tup.append(x)
	tablecontent=tup


	for i in range(0, len(operatorlist) ):
		if( operatorlist[i] == '='):
			if(isinstance(operandlist[i*2+1], str) and isinstance(operandlist[i*2], str)):
				finalcolumns[operandlist[i*2+1]]=operandlist[i*2]
				

######################################################################################

def whereclause(clause, ind):

	clause=str(clause[ind]).strip() + " "

	ind=clause.find(' ')
	flag=0
	andop=0
	orop=0
	relop=['<','>','=','!']		#DIFFERENT
	st=""
	op=""
	i=ind+1
	operandlist=[]
	operatorlist=[]

	while(i < len(clause) ):

		if(clause[i] == '('):
			if(flag == 1):
				print "Query not properly formed. Missing a right parantheses."
				sys.exit(0)
			flag=1
		elif(clause[i] == ')'):
			if(flag == 1):
				flag=0
			else:
				print "Query not properly formed. Missing a left parantheses."
				sys.exit(0)

		elif(clause[i] == ' '):
			if(len(st)>0):
				if(st=='AND'):
					andop=1
				elif(st=='OR'):
					orop=1
				else:
					st=checktype(st)
					operandlist.append(st)
				st=""
		elif(clause[i] in relop):
			op=op+clause[i]
			if(clause[i+1] in relop):
				op=op+clause[i+1]
				i+=1
			if(len(st)>0):
				st=checktype(st)
				operandlist.append(st)
				st=""
			operatorlist.append(op)
			op=""
			
		else:
			st=st+clause[i]
		i+=1

	if(flag==1):
		print "Query not properly formed. Missing a right parantheses."
		sys.exit(0)

	evaluate(operandlist,operatorlist, andop, orop)

######################################################################################

def FetchMetadata():
	if(not(os.path.isfile('./metadata.txt'))):
		print "Metadata file is Missing."
		sys.exit(0)

	file=open('./metadata.txt')
	i=0
	lines=file.readlines()
	while(i<len(lines)):
		line=lines[i].strip(' \t\n\r')
		if(line=="<begin_table>"):
			att=[]
			filename=lines[i+1].strip(' \t\n\r')
			i+=2
			line=lines[i].strip(' \t\n\r')
			while( line != "<end_table>"):
				att.append(lines[i].strip(' \t\n\r'))
				i=i+1
				line=lines[i].strip(' \t\n\r')
			attributes[filename]=att
		else:
			i=i+1



##################################################################################

dist=-1
agg=-1
cols=-1
table=-1
query=-1

######################################################################################

def checktype(col_name):

	if(col_name.isdigit()): 
		col_name=int(col_name)
	elif(col_name in joincol):
		if( joincol.count(col_name) > 1):
			print "Column",col_name,"exits in more than one table. Give table name also along with the column."
			sys.exit(0)
		else:
			for x in tables:
				if (col_name in attributes.get(x)):
					col_name=x+"."+col_name
					break
	elif(col_name not in jointab):
		print "Column",col_name,"does not exits."
		sys.exit(0)

	return col_name

######################################################################################

def CheckArguements(argument_length):
	if(argument_length != 2):
		print "Wrong arguments. Should write the query in double quotes."
		sys.exit(0)
	return

######################################################################################

def ParseString(string_query):
	if(string_query.endswith(';')):
		token=sqlparse.parse(string_query[:-1])[0]
	else:
		print "Missing Semicolon at end of query."
		sys.exit(0)
	return token

######################################################################################

def TokenizeQuery(token):
	for x in token:
		z=str(x).strip(' ')
		if(len(z) != 0):
			select_query.append(z)
	return select_query	

######################################################################################

def CalculateIndexes(select_query):
	global cols
	global dist
	global table
	global query

	for i in range(0, len(select_query)):
		part_query=select_query[i].lower()

		if(part_query == "from"):
			table=i+1
		elif(part_query == "select"):
			cols=i+1
		elif(part_query == "distinct"):
			dist=1
			cols=i+1
		else:
			clause=select_query[i].split()
			clause_part=clause[0].lower()
			if(clause_part=="where"):
				query=i

######################################################################################

def FileExistsCheck():
	for x in tables:
		if( x not in attributes ):
			print x,"is not in metadata.txt"
			sys.exit(0)
		else:
			if(not (os.path.isfile('./'+x+'.csv')) ):
				print x," does not exists."
				sys.exit(0)
######################################################################################

select_query=[]	# contains parts of query
jointab=[]
finalcolumns={}
joincol=[]

##################################################################################			

def main():

	argument_length= len(sys.argv)

	global cols
	global dist
	global table
	global query

	CheckArguements(argument_length)
		
	string_query=sys.argv[1]

	token=ParseString(string_query)
	
	select_query=TokenizeQuery(token)

	query_length=len(select_query)

	if(query_length < 4):
		print "Query is not properly formed."
		sys.exit(0)

	CalculateIndexes(select_query)
	
	if( cols==-1 or table==-1):
		print "Query is not properly formed."
		sys.exit(0)

	if( cols != 1 and query_length> 5 and query == -1):
			print "Query is not properly formed."
			sys.exit(0)

	#Adding queried Columns to columns list
	token=str(select_query[cols]).split(',')
	for x in token:
		y=x.strip()
		columns.append(y)

	#Adding queried Tables to tables list
	token=str(select_query[table]).split(',')
	for x in token:
		y=x.strip()
		tables.append(y)
	
	FetchMetadata()

	FileExistsCheck()
	
	LoadTablesData()
	
	if(query != -1):
		whereclause(select_query,query)

	printoutput(dist)
	
if __name__=="__main__":
	main()
