import operator
import csv
import sys
import os
import re
import sqlparse
sys.path.insert(0,os.getcwd()) # sys.path is a list, and so, of course, supports all list methods with their exact semantics.

# But for sys.path specifically, element 0 is the path containing the script, and so using index 1 causes Python to search that path first and then the inserted pat

tablecontent=[]	# contains data of the tables mentioned in the query
dist=-1
table=-1
querycontent=[]	# contains parts of query
attributes={}	# contains column names for each table from the metadata file
finalcolumns={}


columns=[]	# contains column names from the query
tables=[]	# contains table names from the query
function_name=['avg','sum','min','max']
ro=[]

def check_meta():
	if( not (os.path.isfile('./metadata.txt'))):
		print "Need the metadata file"
		return -1
	return 1

def read_mdata_file():

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

def open_Metadata_file():
	res=check_meta()
	if res==-1:
		sys.exit(0)

	read_mdata_file()
	# for x in attributes:
	# 	print x, attributes[x]

def print_it(column_name,ol):
	resul=""		
	# print 'heya'	
	for i in range(0, len(column_name)):
		if(i== (len(column_name)-1) ):
			resul+=""+column_name[i]
		else:
			resul+=""+column_name[i]+", "
	print resul
	
	j=0
	for j in range(len(ol)):
		resul=""
		x=ol[j]
		for i in range(0, len(x)):
			if(i== (len(x)-1) ):
				resul+=""+str(x[i])
			else:
				resul+=""+str(x[i])+", "
		print resul

######################################################################################

joincol=[]
jointab=[]

def load_all_Tables(size):

	csvfile=open('./'+tables[0]+'.csv')
	all_rows=csv.reader(csvfile, delimiter=',')

	global tablecontent
	for row in all_rows:
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

	if(size > 1):

		for i in range(1, len(tables)):
			
			for x in attributes.get(tables[i]):
				jointab.append(tables[i]+"."+x)
				joincol.append(x)
				finalcolumns[tables[i]+"."+x]=tables[i]+"."+x

			# print jointab
			tup=[]
			for z in tablecontent:
				# prsint 'checkk'
				csvfile=open('./'+tables[i]+'.csv')
				all_rows=csv.reader(csvfile, delimiter=',')
				for row in all_rows:
					x=list(z)
					# for j in range(0,len(row)):
					for y in row:
						# y=row[j]
						# print 'checkk'
						if ( y[0]=='\"'):
							y=y[1:len(y)-1]
						x.append(int(y))
					tup.append(x)
				csvfile.close()
			tablecontent=list(tup)
	# print 'checkk', tablecontent


######################################################################################

indexlist=[]
def print_output(dist):

	st=""
	func=""
	name=columns[0]
	if(len(tablecontent) == 0):
		print "No matching records."
	elif('(' in name):
		split_name= name.split('(')
		func=split_name[0].strip()
		nn=split_name[1].split(')')[0]

		name=nn.strip()
		if(name.isdigit()==0):
			name=check_type(name)
			if( func.lower() == 'dist' or func.lower() == 'distinct'):
				dist=1
			if(func in function_name):
				output=calc(func, name)
				print columns[0] #changes to be made
				print output

			elif(func.lower() == 'count'):
				print len(tablecontent )
			else:
				print "\"",func,"() \" function does not exist."
		else:			
			print "Aggregate functions don't take constant value."
			sys.exit(0)
		

	else:
		column_name=[]
		column=[]
		
		if(name!='*'):
			column_name=[check_type(z) for z in columns]
			# print 'cc',columns
			k=0
			for k in range(len(columns)):
			# for x in columns:
				x=columns[k]
				if(x.isdigit()):
					print "Column names cannot be integer."
					sys.exit(0)
				else:
					n=check_type(x)
					while( finalcolumns.get(n) != n ):
						n=finalcolumns.get(n)
					column.append(n)
		else:
			for x in jointab:
				if( finalcolumns.get(x) == x):
					column_name.append(x)
					column.append(x)
		outputlist=[]		
		k=0
		for k in range(len(column)):
			x=column[k]
		# for x in column:
			ind=jointab.index(x)
			indexlist.append(ind)


		for x in tablecontent:
			tup=[]
			for ind in indexlist:
				tup.append(x[ind])
			if(dist==1):
				if (tup not in outputlist):
					outputlist.append(tup)
			else:
				outputlist.append(tup)

		# display headers
		print_it(column_name,outputlist)

######################################################################################

def get_tups(t_content,operatorlist,operandlist,an_op, o_op):
	temp=[]

	for x in t_content:
		flag=True
		for i in range(0, len(operatorlist)):
			a= operandlist[i*2]
			b= operandlist[(i*2)+1]

			if( (type(a)==str) and (type(b)==str)):
				aind= jointab.index(a)
				bind= jointab.index(b)
				a_value=x[aind]
				b_value=x[bind]
			elif( (type(a)==str) and (type(b)==int)):
				aind= jointab.index(a)
				a_value=x[aind]
				b_value=b
			elif( (type(b)==str) and (type(a)==int)):
				bind= jointab.index(b)
				b_value=x[bind]
				a_value=a
			else:
				print "Both the operands in a condition cannot be a constant."
				sys.exit(0)

			if( relatnl_opr(operatorlist[i],a_value, b_value) ):
				if(o_op==1):
					flag=True
					break
			else:
				flag=False
				if(an_op==1):						
					break
		if(flag):
			temp.append(x)
	return temp


def enter_TableList(token, index):
	token=str(token[index]).split(',')
	for x in token:
		tables.append(x.strip())
		# print tables
######################################################################################

def calc(function_name, col_name):

	ind=jointab.index(col_name)
	output=tablecontent[0][ind]
	# print output
	len_tbl_cnt=len(tablecontent)
	fn=function_name.lower()
	for i in range(1, len_tbl_cnt):
		
		if(fn=='sum'):
			output+=tablecontent[i][ind]
		elif(fn=='avg'):
			output+=tablecontent[i][ind]
		elif(fn=='min'):
			output=min(output,tablecontent[i][ind])
		elif(fn=='max'):
			output=max(output,tablecontent[i][ind])

	if(fn=='avg'):
		output=float("{0:.3f}".format(float(output) / float(len_tbl_cnt) ))

	return output


######################################################################################

def relatnl_opr(opr, a, b):
	# print opr[0]
	if(opr[0]=='>'):
		if(len(opr)==1):
			ans=operator.gt(a,b)
		else:
			ans=operator.ge(a,b)
	elif(opr[0]=='<'):
		if(len(opr)==1):
			ans=operator.lt(a,b)
		else:
			ans=operator.le(a,b)

	
	elif(opr == '='):
		ans= operator.eq(a,b)
	elif(opr == '<>' or opr == '!='):
		ans= operator.ne(a,b)
	else:
		print "check operators.Supported operators are : >, >=, <, <=, <>, !=, = ."
		sys.exit(0)
	return ans



######################################################################################

def eval_where(operandlist,operatorlist, andop, orop):

	# print operandlist,operatorlist
	
	if(len(operandlist) == 0):
		print "No condition in where clause."
		sys.exit(0)

	if( (len(operatorlist)*2) != len(operandlist)):
		print "Conditions are not properly formed in where clause."
		sys.exit(0)

	
	global tablecontent

	tup=[]
	if(andop==1 and orop==1):
		print "Query should not contain both AND and OR. It may contain multiple ANDs or multiple ORs."
		sys.exit(0)

	tablecontent=get_tups(tablecontent,operatorlist,operandlist,andop, orop)


	for i in range(0, len(operatorlist) ):
		if( operatorlist[i] == '='):
			opp1=operandlist[i*2+1]
			opp=operandlist[i*2]
			if((type(opp1)== str) and (type(opp)== str)):
				finalcolumns[opp1]=opp
				

######################################################################################

def check_type(data):
	# print 'neg h kya:',data[0]
	if(data.isdigit()):
		data=int(data)
	elif(data[0]=='-'):
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
		print "Column",data,"does not exist."
		sys.exit(0)

	return data

######################################################################################
def find_q_c(token):			
	for x in token:
		z=str(x).strip(' ')
		# print 'Z-',x
		if(len(z) != 0):
			querycontent.append(z)
	return querycontent

######################################################################################

operandlist=[]
operatorlist=[]
relop=['<','>','=','!']

def where_operation(ind,clause):

	clause=str(clause[ind]).strip() + " "

	ind=clause.find(' ')
	i=ind+1
	flag=0
	andop=0
	orop=0	
	st=""
	op=""
	
	while(i < len(clause) ):

		if(clause[i] == '('):
			if(flag == 1):
				print "Query not properly formed. Not found right parantheses."
				sys.exit(0)
			flag=1
		elif(clause[i] == ')'):
			if(flag == 1):
				flag=0
			else:
				print "Query not properly formed. Not found left parantheses."
				sys.exit(0)

		elif(clause[i] == ' '):
			if(len(st)>0):
				if(st=='AND'):
					andop=1
				elif(st=='OR'):
					orop=1
				else:
					st=check_type(st)
					operandlist.append(st)
				st=""
		elif(clause[i] in relop):
			op=op+clause[i]
			if(clause[i+1] in relop):
				op=op+clause[i+1]
				i+=1
			if(len(st)>0):
				st=check_type(st)
				operandlist.append(st)
				st=""
			operatorlist.append(op)
			op=""
			
		else:
			st=st+clause[i]
		i+=1

	if(flag==1):
		print "Not found right parantheses."
		sys.exit(0)

	eval_where(operandlist,operatorlist, andop, orop)

######################################################################################

def enter_ColumnList(token, index):
	token=str(token[index]).split(',')
	for x in token:
		columns.append(x.strip())
	# print 'col',columns

##################################################################################
def claue_where(o_list,oprnd,oprlist, an, or_op):

	# print operandlist,operatorlist
	col_fin=[]	
	if(len(oprnd) == 0):
		print "No condition in where clause."
		sys.exit(0)
	ro=-1

	while ro< len(oprlist):
		ro+=1
		if( oprlist[i] == '='):
			opp1=oprnd[i*2+1]
			opp=oprnd[i*2]
	k=-1
	for k in range(len(o_list)):
		if(or_op== -1):
			print 'check'



##################################################################################

cols=-1
query=-1
def main():
	len_arg=len(sys.argv)

	global cols
	global dist
	global table
	global query
	
	if(len(sys.argv) != 2):
		print "Wrong arguments. Should write the query in double quotes."
		sys.exit(0)

	argm=sys.argv
	if(argm[1].endswith(';')):
		token=sqlparse.parse(argm[1][:-1])[0] #to drop semicolon
	else:
		print "Missing semi-colon. query must end with ';'"
		sys.exit(0)
		
		token=sqlparse.parse(argm[1])[0]	
	
	querycontent=find_q_c(token)

	if(len(querycontent) < 4):
		print "Query is not properly formed."
		sys.exit(0)
# for i in range(0, len(querycontent)):
	i=0
	while i<len(querycontent):
		pass
		q_c=querycontent[i].lower()
		if(q_c == "select"):
			cols=i+1
		elif(q_c == "distinct"):
			dist=1
			cols=i+1
		elif(q_c == "from"):
			table=i+1
		else:
			clause=q_c.split()
			if(clause[0].lower()=="where"):
				query=i
		i+=1
	# print dist, cols, table,query
	if(table==-1):
		print "Wrong syntax of query."
		sys.exit(0)
	if( cols != 1):
		if(query == -1):
			if(len(querycontent)> 5 ):
				print "Wrong syntax of query."
				sys.exit(0)

	if( cols==-1 ):
		print "Wrong syntax of query."
		sys.exit(0)

	if(cols != -1):
		enter_ColumnList(querycontent,cols)
	if(table != -1):
		enter_TableList(querycontent,table)
	open_Metadata_file()
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
	# File_exists()
	j=0
	for j in range(len(tables)):
		x=tables[j]
		if( x in attributes ):
			if ( not (os.path.isfile('./'+x+'.csv')) ):
				print x," is not present"
				sys.exit(0)
		else:
			print x,"is not in metadata.txt"
			sys.exit(0)

	load_all_Tables( len(tables) )
	
	if(query != -1):
		where_operation(query,querycontent)

	print_output(dist)
	
if __name__=="__main__":
	main()
