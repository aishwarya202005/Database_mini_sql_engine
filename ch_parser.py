import operator
import csv
import sys
import os
import re
sys.path.insert(0,os.getcwd()) #3

# sys.path is a list, and so, of course, supports all list methods with their exact semantics.

# But for sys.path specifically, element 0 is the path containing the script, and so using index 1 causes Python to search that path first and then the inserted pat

import sqlparse

columns=[]	# contains column names from the query
tables=[]	# contains table names from the query
attributes={}	# contains column names for each table from the metadata file
tablecontent=[]	# contains data of the tables mentioned in the query
dist=-1
agg=-1
cols=-1
table=-1
query=-1
querycontent=[]	# contains parts of query
jointab=[]
finalcolumns={}
joincol=[]

######################################################################################

def fillColumnList(token, index):

	if(index != -1):
		token=str(token[index]).split(',')
		for x in token:
			columns.append(x.strip())
		# print 'col',columns

######################################################################################

def fillTableList(token, index):

	if(index != -1):
		token=str(token[index]).split(',')
		for x in token:
			tables.append(x.strip())
		# print tables


######################################################################################

def checkFile():

	for x in tables:
		if( x in attributes ):
			if ( not (os.path.isfile('./'+x+'.csv')) ):
				print x," does not exists."
				sys.exit(0)
		else:
			print x,"is not in metadata.txt"
			sys.exit(0)

######################################################################################

def getMetadata():

	if( not (os.path.isfile('./metadata.txt'))):
		print "Need the metadata file"
		sys.exit(0)

	file=open('./metadata.txt')
	i=0
	lines=file.readlines()
	while(i<len(lines)):
		line=lines[i].strip(' \t\n\r')
		if(line=="<begin_table>"):
			filename=lines[i+1].strip(' \t\n\r')
			att=[]
			i+=2
			line=lines[i].strip(' \t\n\r')
			while( line != "<end_table>"):
				att.append(lines[i].strip(' \t\n\r'))
				i=i+1
				line=lines[i].strip(' \t\n\r')
			attributes[filename]=att
		else:
			i=i+1

	# for x in attributes:
	# 	print x, attributes[x]

######################################################################################

def loadTables(length):

	csvfile=open('./'+tables[0]+'.csv')
	rows=csv.reader(csvfile, delimiter=',')

	global tablecontent
	for row in rows:
		# print 'row',row
		tup=[]
		for x in row:
			# print 'x in row-',x[0]
			if ( x[0]=='\"'):
				x=x[1:len(x)-1]
			tup.append(int(x))
	 	tablecontent.append(tup)
	csvfile.close()

	for x in attributes.get(tables[0]):
		jointab.append(tables[0]+"."+x)
		joincol.append(x)
		finalcolumns[tables[0]+"."+x]=tables[0]+"."+x

	if(length > 1):

		for i in range(1, len(tables)):
			
			for x in attributes.get(tables[i]):
				jointab.append(tables[i]+"."+x)
				joincol.append(x)
				finalcolumns[tables[i]+"."+x]=tables[i]+"."+x

			# print jointab
			tup=[]
			for z in tablecontent:
				csvfile=open('./'+tables[i]+'.csv')
				rows=csv.reader(csvfile, delimiter=',')
				for row in rows:
					x=list(z)
					for j in range(0,len(row)):
						y=row[j]
						if ( y[0]=='\"'):
							y=y[1:len(y)-1]
						x.append(int(y))
					tup.append(x)
				csvfile.close()
			tablecontent=list(tup)
	# print tablecontent

######################################################################################

def aggregate(function_name, col_name):

	ind=jointab.index(col_name)
	output=tablecontent[0][ind]
	# print output

	for i in range(1, len(tablecontent)):
		if(function_name.lower()=='sum' or function_name.lower()=='avg'):
			output+=tablecontent[i][ind]
		elif(function_name.lower()=='min'):
			output=min(output,tablecontent[i][ind])
		elif(function_name.lower()=='max'):
			output=max(output,tablecontent[i][ind])

	if(function_name.lower()=='avg'):
		output=float("{0:.3f}".format(float(output) / float(len(tablecontent)) ))

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
			if( func.lower() == 'dist' or func.lower() == 'distinct'):
				dist=1
			if(func in function_name):
				output=aggregate(func, name)
				print columns[0] #changes to be made
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
		# display headers
		for i in range(0, len(column_name)):
			if(i== (len(column_name)-1) ):
				st=st+""+column_name[i]
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

def checktype(data):

	if(data.isdigit()):
		data=int(data)
	elif(data in joincol):
		if( joincol.count(data) > 1):
			print "Column",data,"exists in more than one table. Give table name also along with the column."
			sys.exit(0)
		else:
			for x in tables:
				if (data in attributes.get(x)):
					data=x+"."+data
					break
	elif( data not in jointab):
		print "Column",data,"does not exits."
		sys.exit(0)

	return data

######################################################################################

def whereclause(clause, ind):

	clause=str(clause[ind]).strip() + " "

	ind=clause.find(' ')
	flag=0
	andop=0
	orop=0
	relop=['<','>','=','!']
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


##################################################################################

def main():

	arglen= len(sys.argv)
	# print sys.argv
	
	if(arglen != 2):
		print "Wrong arguments. Should write the query in double quotes."
		sys.exit(0)
		
	if(sys.argv[1].endswith(';')):
		token=sqlparse.parse(sys.argv[1][:-1])[0] #to drop semicolon
	else:
		#1st change to be done for handling "MISSING SEMI-COLON"
		token=sqlparse.parse(sys.argv[1])[0]	
	# print 'token- ',token
	
	for x in token:
		z=str(x).strip(' ')
		# print 'Z-',x
		if(len(z) != 0):
			querycontent.append(z)

	# print 'qc',querycontent

	if(len(querycontent) < 4):
		print "Query is not properly formed."
		sys.exit(0)

	global cols
	global dist
	global table
	global query
	for i in range(0, len(querycontent)):
		if(querycontent[i].lower() == "select"):
			cols=i+1
		elif(querycontent[i].lower() == "distinct"):
			dist=1
			cols=i+1
		elif(querycontent[i].lower() == "from"):
			table=i+1
		else:
			clause=querycontent[i].split()
			if(clause[0].lower()=="where"):
				query=i

	# print dist, cols, table,query

	if( cols==-1 or table==-1):
		print "Query is not properly formed."
		sys.exit(0)
	if( cols != 1 and len(querycontent)> 5 and query == -1):
		print "Query is not properly formed."
		sys.exit(0)

	fillColumnList(querycontent,cols)
	fillTableList(querycontent,table)
	getMetadata()
	checkFile()
	loadTables( len(tables) )
	
	if(query != -1):
		whereclause(querycontent,query)

	printoutput(dist)
	
if __name__=="__main__":
	main()
