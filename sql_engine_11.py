import csv
# import sys

table_list = []
table_found = False
word = '<begin_table>'
lines = open('./metadata.txt', 'r')
for line in lines:
    if table_found:
        table_list.append(line.strip())
        table_found = False
    if word in line:
        table_found = True
lines.close()
# print table_list

table_start = False
field_start = False
field_list = []
for i in range(len(table_list)):
    field_list.append([])
start_word = '<begin_table>'
end_word = '<end_table>'
table_number = 0
lines = open('./metadata.txt', 'r')
for line in lines:
    if field_start and (end_word in line) is False:
        field_list[table_number].append(line.strip())
    if end_word in line:
        field_start = False
        table_number = table_number + 1
    if line.strip() in table_list:
        field_start = True
lines.close()
# print field_list

table_dict = {}
for i in range(len(table_list)):
    table_dict[table_list[i]] = field_list[i]
# print table_dict

table_field = {}
for table in table_list:
    filename = table + '.csv'
    table_field[table] = []
    # print table_dict[table]
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile, table_dict[table])
        for row in reader:
            table_field[table].append(row)
            # print row
    # print table_field[table]
# for table in table_field:
#     print table
#     for row in table_field[table]:
#         print row

# print table_field

##########################################################################
from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword

def test( str ):
    print str,"->"
    try:
        tokens = simpleSQL.parseString( str )
        # print "tokens = ",        tokens
        # print "tokens.columns =", tokens.columns
        # print "tokens.tables =",  tokens.tables
        # print "tokens.where =", tokens.where
    except ParseException, err:
        print " "*err.loc + "^\n" + err.msg
        print err
    return tokens


# define SQL tokens
selectStmt = Forward()
selectToken = Keyword("select", caseless=True)
fromToken   = Keyword("from", caseless=True)

ident          = Word( alphas, alphanums + "_$" ).setName("identifier")
columnName     = Upcase( delimitedList( ident, ".", combine=True ) )
columnNameList = Group( delimitedList( columnName ) )
tableName      = Upcase( delimitedList( ident, ".", combine=True ) )
tableNameList  = Group( delimitedList( tableName ) )

whereExpression = Forward()
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)

E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                         ( "." + Word(nums) ) ) + 
            Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) + 
            Optional( E + Optional("+") + Word(nums) ) )

columnRval = realNum | intNum | quotedString | columnName # need to add support for alg expressions
whereCondition = Group(
    ( columnName + binop + columnRval ) |
    ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
    ( columnName + in_ + "(" + selectStmt + ")" ) |
    ( "(" + whereExpression + ")" )
    )
whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

# define the grammar
selectStmt      << ( selectToken + 
                   ( '*' | columnNameList ).setResultsName( "columns" ) + 
                   fromToken + 
                   tableNameList.setResultsName( "tables" ) + 
                   Optional( Group( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("where") )

simpleSQL = selectStmt

# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
simpleSQL.ignore( oracleSqlComment )

##########################################################################
def field_extract ( table, field):
    field_output = []
    for t in table_field:
    	if t.lower() == table.lower():
    		if field == '*':
    			for record in table_field[t]:
    				field_output.append(record)
    		else:
	    		for record in table_field[t]:
	    			if field in record:
	    				field_output.append(record[field])
    return field_output

# field_extract('table1','A')
# field_extract('table1','*')

##########################################################################

print "-------------------------------------------------"
print "Welcome to mini SQL engine!"
print "Enter an SQL query to run it on given database"
print "Enter 'quit','q' or 'exit' to exit the program"
print "-------------------------------------------------"

while True:
    query = raw_input("Input SQL query:\n")
    if query == "quit" or query == "q" or query == "exit":
        break
    else:
        print "->\n"
        # print test(query)
        tokens = test(query)
        #print tokens
        ans = []
        for column in tokens[1]:
        	ans.append(field_extract(tokens[3][0].lower(),column))
        	#print ans
        # print tokens[3][0]
        # print tokens[1][0]
        # print field_extract(tokens[3][0].lower(), tokens[1][0].lower())
        # print ans[0]
        str = ''
        for c in tokens[1]:
        	str = str + c + ' '
        print str
        for i in range(len(ans[0])):
            if len(ans) == 1:
            	print ans[0][i]
            else:
            	str = ''
                for j in range(len(ans)):
                	str = str + ans[j][i] + '  '
                print str

