options {
	STATIC = false;
	UNICODE_INPUT = true;
	DEBUG_PARSER = false;
	VISIBILITY_INTERNAL = true;
}

PARSER_BEGIN(SqlParser)

namespace Deveel.Data.Sql.Parser;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;

class SqlParser {
	private int parameterId = 0;

	public void ResetParameter() {
		parameterId = 0;
	}

	public int GetParameterId() {
		int curId = parameterId;
		++parameterId;
		return curId;
	}

	private static Expression MakeFunction(string name, Expression arg1, Expression arg2, Expression arg3) {
		FunctionExpression exp = new FunctionExpression(name.ToLower(CultureInfo.InvariantCulture));
		exp.Parameters.Add(arg1);
		exp.Parameters.Add(arg2);
		exp.Parameters.Add(arg3);
		return exp;
	}

	private static Expression MakeFunction(string name, Expression arg1, Expression arg2) {
		FunctionExpression exp = new FunctionExpression(name.ToLower(CultureInfo.InvariantCulture));
		exp.Parameters.Add(arg1);
		exp.Parameters.Add(arg2);
		return exp;
	}

	private static Expression MakeFunction(string name, Expression arg) {
		FunctionExpression exp = new FunctionExpression(name.ToLower(CultureInfo.InvariantCulture));
		exp.Parameters.Add(arg);
		return exp;
	}
	
	private static Expression MakeFunction(Token t, string name, Expression exp1, Expression exp2) {
		FunctionExpression exp = new FunctionExpression(name.ToLower(CultureInfo.InvariantCulture));
		exp.Parameters.Add(exp1);
		exp.Parameters.Add(exp2);
		exp.Line = t.beginLine;
		exp.Column = t.beginColumn;
		return exp;
	}
	
	private static Expression MakeFunction(Token t, string name, Expression arg) {
		FunctionExpression exp = new FunctionExpression(name.ToLower(CultureInfo.InvariantCulture));
		exp.Parameters.Add(arg);
		exp.Line = t.beginLine;
		exp.Column = t.beginColumn;
		return exp;
	}

	private static Expression UserFunction(Token identToken, string functionName, IList parameters) {
 		// Special case for 'if'
		string lcFunctionName = functionName.ToLower(CultureInfo.InvariantCulture);
		if (lcFunctionName.Equals("if"))
			lcFunctionName = "if_sql";

		FunctionExpression exp = new FunctionExpression("@" + lcFunctionName);
		foreach(object param in parameters) {
			exp.Parameters.Add(param);
		}

		exp.Line = identToken.beginLine;
		exp.Column = identToken.beginColumn;
		return exp;
	}
	
	private static Expression FetchGlob(string glob_str) {
		return new FetchGlobExpression(glob_str);
	}
	
	private static Expression FetchVariable(Token t) {
		Expression exp = FetchVariable((Variable) Util.ToParamObject(t, false));
		exp.Line = t.beginLine;
		exp.Column = t.beginColumn;
		return exp;
	}
	
	private static Expression FetchVariable(Variable v) {
		return new FetchVariableExpression(v);
	}
	
	private static Expression FetchStatic(SqlObject ob) {
		return new FetchStaticExpression(ob);
	}
	
	private static Expression LabelAsFetchStatic(Token t) {
		return FetchStatic(new SqlObject(t.image.ToLower(CultureInfo.InvariantCulture)));
	}
	
	private static Expression FetchParam(int paramId) {
		return new FetchParameterExpression(paramId);
	}
	
	private static Expression FetchTable(TableName tableName) {
		return new FetchTableExpression(tableName);
	}
	
	private static Expression FetchTable(Token tableName) {
		Expression exp = FetchTable(Util.ParseTableName(tableName));
		exp.Line = tableName.beginLine;
		exp.Column = tableName.beginColumn;
		return exp;
	}
	
	private static Expression AliasTableName(Expression table, TableName alias) {
		return new AliasTableNameExpression(table, alias);
	}
	
	private static Expression AliasVariable(Expression exp, Variable variable) {
		return new AliasVariableNameExpression(exp, variable);
	}
	
	private static Expression JoinExpression(Expression leftExp, Expression rightExp, JoinType type, Expression filterExp) {
		return new JoinExpression(leftExp, rightExp, type, filterExp);
	}
}

PARSER_END(SqlParser)


SKIP : {
	  " "
	| "\t"
	| "\n"
	| "\r"
	|  <"//" (~["\n","\r"])* ("\n" | "\r" | "\r\n")>
	|  <"--" (~["\n","\r"])* ("\n" | "\r" | "\r\n")>
//	|  <"/*" (~["*"])* "*" ("*" | ~["*","/"] (~["*"])* "*")* "/">

}


TOKEN: {
	  <STAR:       "*" >
	| <QUESTION:   "?" >
	| <ASSIGNMENT: "=" >
	| <EQUALS:     "==" >
	| <GR:         ">" >
	| <LE:         "<" >
	| <GREQ:       ">=" >
	| <LEEQ:       "<=" >
	| <NOTEQ:      "!=" | "<>" >
	| <DIVIDE:     "/" >
	| <ADD:        "+" >
	| <SUBTRACT:   "-" >
	| <CONCAT:     "||" >
}

TOKEN [IGNORE_CASE] : { 
	  <BOOLEAN_LITERAL: "true" | "false" >
	| <NULL_LITERAL:    "null" >

	// NOTE: Handling regex literals is a horrible horrible hack.  The <REGEX_LITERAL> 
	//   token starts with 'regex /' and the regex string follows.
	//   The reason for this hack is because / clashes with <DIVIDE>
	| <REGEX_LITERAL:   "regex /" (~["/","\n","\r"] | "\\/" )* "/" ( "i" | "s" | "m" )* >  
}

TOKEN [IGNORE_CASE] : { /* KEYWORDS */
	  <DROP:        "drop">
	| <SHOW:        "show">
	| <ALTER:       "alter">
	| <SELECT:      "select">
	| <UPDATE:      "update">
	| <CREATE:      "create">
	| <DELETE:      "delete">
	| <INSERT:      "insert">
	| <COMMIT:      "commit">
	| <COMPACT:     "compact">
	| <EXPLAIN:     "explain">
	| <ROLLBACK:    "rollback">
	| <OPTIMIZE:    "optimize">
	| <DESCRIBE:    "describe">
	| <SHUTDOWN:    "shutdown">
	
	| <IS:          "is">
	| <IF:          "if">
	| <AS:          "as">
	| <ON:          "on">
	| <TO:          "to">
	| <NO:          "no">
	| <BY:          "by">
	| <ALL:         "all">
	| <ANY:         "any">
	| <SET:         "set">
	| <USE:         "use">
	| <ASC:         "asc">
	| <OLD:         "old">
	| <NEW:         "new">
	| <SQLADD:      "add">
	| <FOR:         "for">
	| <ROW:         "row">
	| <EACH:        "each">
	| <CALL:        "call">
	| <BOTH:        "both">
	| <SOME:        "some">
	| <FROM:        "from">
	| <LEFT:        "left">
	| <DESC:        "desc">
	| <INTO:        "into">
	| <JOIN:        "join">
	| <TRIM:        "trim">
	| <VIEW:        "view">
	| <LOCK:        "lock">
	| <WITH:        "with">
	| <USER:        "user">
	| <CAST:        "cast">
	| <LONG:        "long">
	| <NAME:        "name">
	| <AFTER:       "after">
	| <START:       "start">
	| <WHERE:       "where">
	| <CYCLE:       "cycle">
	| <CACHE:       "cache">
	| <RIGHT:       "right">
	| <TABLE:       "table">
	| <LIMIT:       "limit">
	| <INNER:       "inner">
	| <INDEX:       "index">
	| <CROSS:       "cross">
	| <OUTER:       "outer">
	| <CHECK:       "check">
	| <USING:       "using">
	| <UNION:       "union">
	| <GRANT:       "grant">
	| <USAGE:       "usage">
	| <GROUP:       "group">
	| <ORDER:       "order">
	| <SQLRETURN:   "return">
	| <BEFORE:      "before">
	| <CSHARP:        "csharp">
	| <UNLOCK:      "unlock">
	| <ACTION:      "action">
	| <GROUPS:      "groups">
	| <REVOKE:      "revoke">
	| <OPTION:      "option">
	| <PUBLIC:      "public">
	| <EXCEPT:      "except">
	| <IGNORE:      "ignore">
	| <SCHEMA:      "schema">
	| <EXISTS:      "exists">
	| <VALUES:      "values">
	| <HAVING:      "having">
	| <UNIQUE:      "unique">
	| <SQLCOLUMN:   "column">
	| <RETURNS:     "returns">
	| <ACCOUNT:     "account">
	| <LEADING:     "leading">
	| <NATURAL:     "natural">
	| <BETWEEN:     "between">
	| <TRIGGER:     "trigger">
	| <SQLDEFAULT:  "default">
	| <VARYING:     "varying">
	| <EXECUTE:     "execute">
	| <VOLATILE:    "volatile">
	| <CALLBACK:    "callback">
	| <MINVALUE:    "minvalue">
	| <MAXVALUE:    "maxvalue">
	| <FUNCTION:    "function">
	| <SEQUENCE:    "sequence">
	| <RESTRICT:    "restrict">
	| <PASSWORD:    "password">
	| <TRAILING:    "trailing">
	| <DEFERRED:    "deferred">
	| <DISTINCT:    "distinct">
	| <LANGUAGE:    "language">
	| <AGGREGATE:   "aggregate">
	| <COMPOSITE:   "composite">
	| <INCREMENT:   "increment">
	| <PROCEDURE:   "procedure">
	| <CHARACTER:   "character">
	| <IMMEDIATE:   "immediate">
	| <INITIALLY:   "initially">
	| <TEMPORARY:   "temporary">
	| <INTERSECT:   "intersect">
	| <PRIVILEGES:  "privileges">
	| <COMPARABLE:  "comparable">
	| <CONSTRAINT:  "constraint">
	| <DEFERRABLE:  "deferrable">
	| <REFERENCES:  "references">
	| <DETERMINISTIC: "deterministic">

	
	| <PRIMARY:     "primary">
	| <FOREIGN:     "foreign">
	| <KEY:         "key">
	
	
	| <COLLATE:     "collate">
	
	// Data types,
	| <BIT:         "bit">
	| <INT:         "int">
	| <REAL:        "real">
	| <CLOB:        "clob">
	| <BLOB:        "blob">
	| <CHAR:        "char">
	| <TEXT:        "text">
	| <DATE:        "date">
	| <TIME:        "time">
	| <FLOAT:       "float">
	| <BIGINT:      "bigint">
	| <DOUBLE:      "double">
	| <NUMBER:      "number">
	| <STRING:      "string">
	| <BINARY:      "binary">
	| <OBJECT:      "object">
	| <NUMERIC:     "numeric">
	| <DECIMAL:     "decimal">
	| <BOOLEAN:     "boolean">
	| <TINYINT:     "tinyint">
	| <INTEGER:     "integer">
	| <VARCHAR:     "varchar">
	| <SMALLINT:    "smallint">
	| <VARBINARY:   "varbinary">
	| <TIMESTAMP:   "timestamp">
	| <LONGVARCHAR: "longvarchar">
	| <YEARTOMONTH: "yeartomonth">
	| <DAYTOSECOND: "daytosecond">
	| <LONGVARBINARY: "longvarbinary">
	
	| <TRANSACTIONISOLATIONLEVEL: "transaction isolation level">
	| <AUTOCOMMIT:                "auto commit">
	| <READCOMMITTED:             "read committed">
	| <READUNCOMMITTED:           "read uncommitted">
	| <REPEATABLEREAD:            "repeatable read">
	| <SERIALIZABLE:              "serializable">
	
	| <CASCADE:                   "cascade">
	
	// Current date/time/timestamp literals
	| <CURRENT_TIME:      "current_time">
	| <CURRENT_DATE:      "current_date">
	| <CURRENT_TIMESTAMP: "current_timestamp">

	
	| <LIKE:       "like" >
	| <REGEX:      "regex" >
	| <AND:        "and" >
	| <OR:         "or" >
	| <IN:         "in" >
	
	| <NOT:        "not">
}

TOKEN : {  /* IDENTIFIERS */
	  <IDENTIFIER: <LETTER> ( <LETTER> | <DIGIT> )* >
	| <DOT_DELIMINATED_REF: <IDENTIFIER> ( "." <IDENTIFIER> )* >
	| <QUOTED_DELIMINATED_REF: <QUOTED_VARIABLE> ( "." <QUOTED_VARIABLE> )* >
	| <OBJECT_ARRAY_REF: <DOT_DELIMINATED_REF> "[]" >
	| <CTALIAS: <IDENTIFIER> >
	| <GLOBVARIABLE: <DOT_DELIMINATED_REF> ".*" >
	| <QUOTEDGLOBVARIABLE: <QUOTED_DELIMINATED_REF> ".*" >
	
	| <#LETTER: ["a"-"z", "A"-"Z", "_"] >
	| <#DIGIT: ["0"-"9"]>
}

TOKEN : {
	  <NUMBER_LITERAL:
       (["-","+"])? ( ( (["0"-"9"])+ ( "." (["0"-"9"])+ )? )
                 |( "." (["0"-"9"])+ ) )
                            ( "E" (["-","+"])? (["0"-"9"])+ )? 
    
      >
	| <STRING_LITERAL:   "'" ( "''" | "\\" ["a"-"z", "\\", "%", "_", "'"] | ~["'","\\"] )* "'" >
	| <QUOTED_VARIABLE:   "\"" ( ~["\""] )* "\"" >
}

IList<Expression> Statements() :
{ List<Expression> statements = new List<Expression>();
  Expression exp;
}
{  exp = Statement() { statements.Add(exp); }
      ( exp = Statement() { statements.Add(exp); } )*
  { return statements.ToArray(); }
}

Expression Statement() :
{ Expression exp;
}
{ (   exp = SelectStatement()
    | LOOKAHEAD(3) exp = CreateTableStatement()
    | exp = ExplainStatement()
	
	| exp = SetStatement()
  )

  ( ";" | <EOF> )

  { return exp; }
}

// ---------- Create Table statement ----------

Expression CreateTableStatement() :
{ FunctionExpression exp = new FunctionExpression("create_table");
  Token t;
  Expression declarations;
  Token tableName;
}
{  t=<CREATE> [<TEMPORARY> { exp.SetArgument("temporary", true); } ]
   <TABLE> [LOOKAHEAD(1) <IF> <NOT> <EXISTS> { exp.SetArgument("if_not_exists", true); } ]
   tableName = TableName()
   "(" declarations = CreateTableDeclarations() ")"

  { exp.Parameters.Add(FetchTable(Util.ParseTableName(tableName)));
    exp.Parameters.Add(declarations);
    exp.Line = t.beginLine;
	exp.Column = t.beginColumn;
    return exp; }
}

Expression CreateTableDeclarations() :
{ FunctionExpression exp = new FunctionExpression("table_declarations");
  Expression dec;
}
{  dec = CreateTableDeclaration() { exp.Parameters.Add(dec); }
     ( "," dec = CreateTableDeclaration() { exp.Parameters.Add(dec); } )*

  { return exp; }
}

FunctionExpression CreateTableDeclaration() :
{ FunctionExpression exp; }
{ (   exp = ColumnDeclaration()
    | exp = ConstraintDeclaration()
  )
  { return exp; }
}

Expression ColumnDefaultExprDeclaration() :
{ Token preToken = token;
  Expression defaultExp;
}
{  <SQLDEFAULT> defaultExp = Expression()

  { string source = Util.MakeSourceString(preToken, token);
    defaultExp.SetArgument("source", source);
    return defaultExp;
  }
}

// This is a column declaration in a create table statement.
// eg. "part_count NUMERIC DEFAULT 0 NOT NULL"
FunctionExpression ColumnDeclaration() :
{ FunctionExpression exp = new FunctionExpression("column_declaration");
  Expression defaultExp = null, constraintExp = null;
  Token columnName;
  SqlType type;
  bool unique = false;
}
{  columnName = VariableReference()  { exp.Parameters.Add(FetchVariable(columnName)); }
   type = GetSqlType()              { exp.Parameters.Add(type); }

    [ ( constraintExp = NullabilityConstraint()
          [ defaultExp = ColumnDefaultExprDeclaration() ] )
      |
      ( defaultExp = ColumnDefaultExprDeclaration()
          [ constraintExp = NullabilityConstraint() ] )
    ]

    [ <UNIQUE> { unique = true; } ]

  { if (defaultExp != null) {
      exp.SetArgument("has_default_exp", true);
      exp.Parameters.Add(defaultExp);
    }
    if (constraintExp != null)
      exp.Parameters.Add(constraintExp);
    if (unique == true)
      exp.Parameters.Add(FetchStatic(new SqlObject("UNIQUE")));

    return exp; }
}

// Nullability constraint
Expression NullabilityConstraint() :
{ string constraint; }
{
   (   <NOT> <NULL_LITERAL>  { constraint = "NOT NULL"; }
     | <NULL_LITERAL>        { constraint = "NULL"; }
   )
   { return FetchStatic(new SqlObject(constraint)); }
}


// The function/column ordering collation
FunctionExpression OrderFunction() :
{ FunctionExpression exp = new FunctionExpression("order_function");
  Expression expr;
  Token desc = null;
}
{   expr = Expression() [ <ASC> | desc=<DESC> ]
  { exp.Parameters.Add(expr);
    exp.Parameters.Add(FetchStatic((desc == null ? "ASC" : "DESC")));
    return exp; }
}



// A basic fetch variable column composite
FunctionExpression BasicColumnComposite() :
{ FunctionExpression exp = new FunctionExpression("basic_var_list");
  Token columnName;
}
{       columnName = VariableReference()
               { exp.Parameters.Add(FetchVariable(columnName)); }
  ( "," columnName = VariableReference()
               { exp.Parameters.Add(FetchVariable(columnName)); } )*

  { return exp; }
}

string ReferentialTrigger() :
{ string trigger;
}
{
  (   <NO> <ACTION>                     { trigger="NO ACTION"; }
    | <RESTRICT>                        { trigger="NO ACTION"; }
    | <CASCADE>                         { trigger="CASCADE"; }
    | LOOKAHEAD(2) <SET> <NULL_LITERAL> { trigger="SET NULL"; }
    | <SET> <SQLDEFAULT>                { trigger="SET DEFAULT"; }
  )

  { return trigger; }
}

FunctionExpression PrimaryKeyConstraint() :
{ FunctionExpression exp = new FunctionExpression("constraint_primary_key");
  Expression columnList;
}
{  <PRIMARY> <KEY> "(" columnList = BasicColumnComposite() ")"
  { exp.Parameters.Add(columnList);
    return exp; }
}

FunctionExpression UniqueConstraint() :
{ FunctionExpression exp = new FunctionExpression("constraint_unique");
  Expression columnList;
}
{  <UNIQUE> "(" columnList = BasicColumnComposite() ")"
  { exp.Parameters.Add(columnList);
    return exp; }
}

FunctionExpression CheckConstraint() :
{ FunctionExpression exp = new FunctionExpression("constraint_check");
  Expression checkExp;
  Token preToken = token;
}
{  <CHECK> "(" checkExp = Expression() ")"
  { exp.Parameters.Add(checkExp);
    string source = Util.MakeSourceString(preToken, token);
    exp.SetArgument("source", source);
    return exp; }
}

FunctionExpression ForeignKeyConstraint() :
{ FunctionExpression exp = new FunctionExpression("constraint_foreign_key");
  Expression columnList, columnList2 = null;
  Token referenceTable;
  string updateRule = "NO ACTION";
  string deleteRule = "NO ACTION";
}
{  <FOREIGN> <KEY> "(" columnList = BasicColumnComposite() ")"
        <REFERENCES> referenceTable=TableName()
                             [ "(" columnList2 = BasicColumnComposite() ")" ]
        [   LOOKAHEAD(2) ( <ON> <DELETE> deleteRule=ReferentialTrigger()
              [ <ON> <UPDATE> updateRule=ReferentialTrigger() ]
            )
          | ( <ON> <UPDATE> updateRule=ReferentialTrigger()
              [ <ON> <DELETE> deleteRule=ReferentialTrigger() ]
            )
        ]

  { exp.Parameters.Add(columnList);
    exp.Parameters.Add(FetchTable(referenceTable));
    if (columnList2 != null) exp.Parameters.Add(columnList2);
    exp.Parameters.Add(FetchStatic(updateRule));
    exp.Parameters.Add(FetchStatic(deleteRule));
    return exp;
  }
}

void ConstraintAttributes(FunctionExpression constraint) :
{ string deferrable = "deferrable";
  string initially = "initially immediate";
}
{
  [ (
      <INITIALLY> (   <DEFERRED>  { initially = "initially deferred"; }
                    | <IMMEDIATE>
                  )

        [   <NOT> <DEFERRABLE>    { deferrable = "not deferrable"; }
          | <DEFERRABLE>
        ]
    )
  |
    (
        (   <NOT> <DEFERRABLE>    { deferrable = "not deferrable"; }
          | <DEFERRABLE> )
        [ <INITIALLY> (   <DEFERRED>  { initially = "initially deferred"; }
                        | <IMMEDIATE>
                      )
        ]
    )
  ]

  { constraint.Parameters.Add(FetchStatic(deferrable));
    constraint.Parameters.Add(FetchStatic(initially));
  }
}


FunctionExpression ConstraintDeclaration() :
{ FunctionExpression constraint;
  string constraintName = null;
}
{
  ( [ <CONSTRAINT> constraintName = NoneDeliminatedReference() ]

    (   constraint = PrimaryKeyConstraint()
      | constraint = UniqueConstraint()
      | constraint = CheckConstraint()
      | constraint = ForeignKeyConstraint()
    )

    // Constraint deferrability
    ConstraintAttributes(constraint)

  )
  { if (constraintName != null)
        constraint.SetArgument("constraint_name", constraintName);
    return constraint;
  }

}

// A select with an optional order by clause
SelectExpression SelectStatement():
{ SelectExpression selectExp; }
{
  selectExp = NestedSelect()
  [ <ORDER> <BY> OrderByElem(selectExp) ( "," OrderByElem(selectExp) )* ]
  { return selectExp; }
}

// ----------

// A nested select expression

SelectExpression NestedSelect():
{ Token st;
  Token distinct_t = null;
  SelectExpression selectExp = new SelectExpression();
  Expression joinExp = null;
  Expression whereExp = null;
  Expression havingExp = null;
}
{ st = <SELECT>
     [ ( distinct_t = <DISTINCT> | <ALL> ) ]
     SelectOutput(selectExp) ( "," SelectOutput(selectExp) )*

     [ <FROM> joinExp = JoinFromSelect()
       [ <WHERE> whereExp = Expression()
          { selectExp.Filter = whereExp; }
       ]
       { selectExp.Join = joinExp; }
     ]

     [ <GROUP> <BY> GroupByElem(selectExp) ( "," GroupByElem(selectExp) )*
       [ <HAVING> havingExp = Expression()
          { selectExp.Having = havingExp; }
       ]
     ]

  { selectExp.Line = st.beginLine;
    selectExp.Column = st.beginColumn;
    if (distinct_t != null) selectExp.IsDistinct = true;
    return selectExp;
  }
}

Expression JoinFromSelect():
{ Expression t1, t2;
  JoinType join_t;
  Expression onExp = null;
}
{
    t1 = TableSpec()
      ( ( join_t = InnerJoin() | join_t = OuterJoin() )
        t2 = TableSpec() [ <ON> onExp = Expression() ]
             { t1 = JoinExpression(t1, t2, join_t, onExp);
               onExp = null;
             }
      )*

  { return t1; }
}

// A table specification

Expression TableSpec():
{ Expression fetchExp;
  Token aliasName = null;
}
{ ( (   "(" fetchExp = NestedSelect() ")"
      | fetchExp = FetchTable()
    )
    [ <AS> ] [ aliasName = TableName() ]
  )

  { if (aliasName != null)
      return AliasTableName(fetchExp, Util.ParseTableName(aliasName));
    else
      return fetchExp;
  }
}


// Inner join specification

JoinType InnerJoin():
{ }
{
  (   ","
    | [ <INNER> ] <JOIN>
  )
  { return JoinType.Inner; }
}

// An outer join specification (eg. ',', 'INNER JOIN', 'LEFT OUTER JOIN', etc)

JoinType OuterJoin():
{ JoinType join_t; }
{
  (    <LEFT> [ <OUTER> ] <JOIN>      { join_t = JoinType.OuterLeft; }
     | <RIGHT> [ <OUTER> ] <JOIN>     { join_t = JoinType.OuterRight; }
     | <OUTER> <JOIN>                 { join_t = JoinType.Outer; }
  )
  { return join_t; }
}

// The output definition of a select (typically after SELECT)
void SelectOutput(SelectExpression selectExp):
{ Token alias = null;
  Expression exp;
  Token preToken = token;
  string label = null;
}
{ (   exp = Expression() { label = Util.MakeLabel(preToken, token); }
              [ <AS> ] [ alias = VariableReference() ]
    | exp = FetchGlobVarsExpression()
  )
  { if (alias != null) {
      exp = AliasVariable(exp, (Variable) Util.ToParamObject(alias, false));
      exp.Line = alias.beginLine;
	  exp.Column = alias.beginColumn;
    }
    exp.SetArgument("label", label);
    selectExp.Output.Add(new SelectOutput(exp));
  }
}

// The group by element spec of a select statement
void GroupByElem(SelectExpression selectExp):
{ Expression exp;
}
{   exp = Expression()
  { selectExp.GroupBy.Add(exp); }
}

// The order by element spec of a select statement
void OrderByElem(SelectExpression selectExp):
{ Expression exp;
  Token desc = null;
}
{   exp = Expression() [ <ASC> | desc=<DESC> ]
  { selectExp.OrderBy.Add(new OrderBy(exp, (desc == null))); }
}



Expression FetchGlobVarsExpression():
{ Token t; }
{ (   t=<STAR>
    | t=<GLOBVARIABLE>
    | t=<QUOTEDGLOBVARIABLE>
  )
  { Expression exp = FetchGlob(Util.AsNonQuotedRef(t));
    exp.Line = t.beginLine;
    exp.Column = t.beginColumn;
    return exp;
  }
}


// A table name

Expression FetchTable():
{ Token t; }
{
    t = TableName()

    { Expression exp = FetchTable(Util.ParseTableName(t));
	  exp.Line = t.beginLine;
      exp.Column = t.beginColumn;
      return exp;
    }
}

Token TableName() :
{ Token name;
}
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <OLD> | name = <NEW> |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return name; }
}

// ---------- Misc statements ----------

Expression TransactionStatement() :
{ string functionName; }
{ (   <COMMIT>   { functionName = "transaction_commit"; }
    | <ROLLBACK> { functionName = "transaction_rollback"; }
  )
  { return new FunctionExpression(functionName); }
}


Expression ExplainStatement() :
{ Expression selectExp; }
{  <EXPLAIN> selectExp = SelectStatement()
  { FunctionExpression exp = new FunctionExpression("explain_expression");
    exp.Parameters.Add(selectExp);
    return exp;
  }
}

Expression SetStatement() :
{ FunctionExpression setExp;
  Expression expr;
  string schemaName;
  Token t;
}
{  <SET>
   (    t=SQLIdentifier() <ASSIGNMENT> expr = Expression()
         { setExp = new FunctionExpression("session_assignment");
           setExp.Parameters.Add(FetchStatic(Util.AsNonQuotedRef(t)));
           setExp.Parameters.Add(expr);
         }

     |  <TRANSACTIONISOLATIONLEVEL> t=<SERIALIZABLE>
         { setExp = new FunctionExpression("isolation_assignment");
           setExp.Parameters.Add(FetchStatic(t.image));
         }

     |  <AUTOCOMMIT> ( t=<ON> | t=<IDENTIFIER> )
         { setExp = new FunctionExpression("autocommit_assignment");
           setExp.Parameters.Add(FetchStatic(t.image));
         }

     |  <SCHEMA> schemaName = NoneDeliminatedReference()
         { setExp = new FunctionExpression("schema_assignment");
           setExp.Parameters.Add(FetchStatic(schemaName));
         }

   )
   { return setExp; }
}

// ---------- Function specifications ----------

// A function set specification


// Returns a list of type function specification lists parsed from the input
// string.

IList TypeFunction() :
{ ArrayList typeList = new ArrayList();
  IList<Function> input;
  Token t, type;
}
{
  ( ( t=<AGGREGATE> | t=<FUNCTION> ) type=<DOT_DELIMINATED_REF> "{"
    input=FunctionList()
    "}"
    { typeList.Add(String.Intern(t.image.ToUpper(CultureInfo.InvariantCulture)));
      typeList.Add(type.image);
      typeList.Add(input);
    }
  )*
  { return typeList; }
}

// Returns a list that is a set of function specifications parsed from the
// input string.

List<Function> FunctionList() :
{ List<Function> list = new List<Function>();
  Function spec;
}
{ ( spec = Function() ";" { list.Add(spec); } )*
  { return list; }
}

// A Mckoi function specification

Function Function() :
{ Token fstate;
  Token fname;
  Function fun = new Function();
}
{ fstate = FunctionState()
  FunctionReturnType(fun)
  fname = FunctionName()
  "(" (   <STAR> { fun.Parameters.Add(FunctionParameter.Star); }
        | FunctionParameters(fun)
      )
  ")"

  { fun.Name = fname.image;
    fun.State = (FunctionState)Enum.Parse(typeof(FunctionState), fstate.image, true);
    fun.Line = fstate.beginLine;
	fun.Column = fstate.beginColumn;
    return fun;
  }
}

Token FunctionState() :
{ Token t; }
{ ( t = <DETERMINISTIC> | t = <VOLATILE> )
  { return t; }
}

void FunctionReturnType(Function fun) :
{ Token type;
  Token t;
}
{   LOOKAHEAD(1) type = FunctionType()
                      { Util.SetFunctionReturnType(fun, type.image); }
  | t=SQLIdentifier() { Util.SetFunctionVariableReturnType(fun, t.image); }
           
}

Token FunctionName() :
{ Token t; }
{ t = SQLIdentifier()
  { return t; }
}

Token FunctionType() :
{ Token t; }
{ (   t=<BINARY> | t=<BOOLEAN> | t=<COMPOSITE> | t=<DATE>
    | t=<OBJECT> | t=<NULL_LITERAL> | t=<NUMERIC> | t=<STRING>
    | t=<TABLE> | t=<ANY> | t=<COMPARABLE>
  )
  { return t; }
}

void ParameterTypeItem(IList<String> list) :
{ Token t; }
{ ( t = FunctionType() )
  { list.Add(String.Intern(t.image.ToLower(CultureInfo.InvariantCulture)));
  }
}

// Parameter type specification lists such as <NUMERIC|STRING>
void ParameterTypeList(List<String> list) :
{ Token t; }
{ "<" ParameterTypeItem(list)
      ( "|" ParameterTypeItem(list) )*
  ">"
}

// Adds a param item that describes a function type item or item sequence.
// The format of the list is parameter type string(s) (1 or more), "Tref" if
// the parameter must be the same as other parameters defined as "Tref"
// or "noref", and "1", "+", or "*" for the number of times the parameter must
// be matched for this function.

void FunctionParameterItem(Function fun) :
{ Token vart = null;
  Token matcht = null;
  List<String> list = new List<String>();
}
{ ( ( ParameterTypeItem(list) | ParameterTypeList(list) )
    [ vart=SQLIdentifier() ]
    [ matcht=<ADD> | matcht=<STAR> | matcht=<QUESTION> ]
  )
  { string matchCount = (matcht == null) ? "1" : matcht.image;
    if (vart == null) fun.Parameters.Add(Util.CreateFunctionParameter(list, null, matchCount));
    else fun.Parameters.Add(Util.CreateFunctionParameter(list, vart.image, matchCount));
  }
}

void FunctionParameters(Function fun) :
{ }
{  [ FunctionParameterItem(fun)
     ( "," FunctionParameterItem(fun) )*
   ]
}


FunctionExpression AssignmentExpression() :
{ FunctionExpression fExp = new FunctionExpression("assignment");
  Token colName;
  Expression exp;
}
{  
  colName = VariableReference() <ASSIGNMENT> exp = Expression()

  { fExp.Parameters.Add(FetchVariable(colName));
    fExp.Parameters.Add(exp);
    return fExp;
  }
}

// The ' set a = (a * 9), b = concat(b, "aa") ' part of the 'update', 'insert' statement
FunctionExpression AssignmentList() :
{ FunctionExpression exp = new FunctionExpression("assignments");
  Expression assignment;
}
{
  ( assignment = AssignmentExpression()        { exp.Parameters.Add(assignment); }
    ( "," assignment = AssignmentExpression()  { exp.Parameters.Add(assignment); } )*
  )

  { return exp; }
}

// A list of expression operations

ArrayList ExpressionList() :
{ ArrayList thelist = new ArrayList();
  Expression exp;
}
{
  [ exp = Expression() { thelist.Add(exp); }
    ( "," exp = Expression() { thelist.Add(exp); } )* ]

  { return thelist; }
}

// Returns an expression (a series of functions necessary to find the result).

Expression Expression():
{ Expression exp; }
{   exp = OrExpression() 

  { return exp; }
}


Expression OrExpression():
{ Token t;
  Expression exp1, exp2; }
{   exp1 = AndExpression() ( t=<OR> exp2 = AndExpression()
                       { exp1 = MakeFunction(t, t.image, exp1, exp2); } )*

  { return exp1; }
}

Expression AndExpression():
{ Token t;
  Expression exp1, exp2; }
{   exp1 = NonLogicalExpression() ( t=<AND> exp2 = NonLogicalExpression()
                       { exp1 = MakeFunction(t, t.image, exp1, exp2); } )*

  { return exp1; }
}


// Point of 'not' precedence
Expression NonLogicalExpression():
{ Expression exp; }
{ exp = IsExpression() { return exp; }
}


Expression IsExpression():
{ Token t;
  Token t2 = null;
  Expression exp1, exp2; }
{   exp1 = EqualityExpression()
            ( LOOKAHEAD(1)
              t=<IS> [ LOOKAHEAD(1) t2=<NOT> ] exp2 = EqualityExpression()
                       { string tfun = (t2 == null) ? "is" : "isnot";
                         exp1 = MakeFunction(t, tfun, exp1, exp2); } )*

  { return exp1; }
}

Expression EqualityExpression():
{ Token t;
  Expression exp1, exp2; }
{   exp1 = ComparisonExpression()
            ( LOOKAHEAD(1)
              ( t=<EQUALS> | t=<ASSIGNMENT> | t=<NOTEQ> ) exp2 = ComparisonExpression()
                       { string funstr = t.image.Equals("==") ? "=" : t.image;
                         exp1 = MakeFunction(t, funstr, exp1, exp2);
                       } )*

  { return exp1; }
}

Expression ComparisonExpression():
{ Token t;
  Expression exp1, exp2; }
{   exp1 = SetComparisonExpression()
          ( LOOKAHEAD(2)
            ( t=<GR> | t=<LE> | t=<GREQ> | t=<LEEQ> ) exp2 = SetComparisonExpression()
                       { exp1 = MakeFunction(t, t.image, exp1, exp2); } )*

  { return exp1; }
}


// Set comparison operation, eg. '= ALL', etc
Expression SetComparisonExpression():
{ Expression exp1, exp2;
  string set_comp_fun;
}
{   exp1 = BetweenExpression()
          [ LOOKAHEAD(2)
                set_comp_fun = SetComparisonType() exp2 = SetRHSExpression()
                      { exp1 = MakeFunction(set_comp_fun, exp1, exp2); } ]

  { return exp1; }
}

// BETWEEN and NOT BETWEEN - we rewrite BETWEEN here.
Expression BetweenExpression():
{ Token t;
  Expression exp1, exp2, exp3;
  bool notbet = false;
}
{   exp1 = LikeExpression()
          [ LOOKAHEAD(2)
              [ <NOT> { notbet = true; } ] t=<BETWEEN>
                  exp2 = AdditionExpression() <AND> exp3 = AdditionExpression()

                      { // Translate BETWEEN to op1 >= op2 AND op1 <= op3
                        Expression ge = MakeFunction(t, ">=", exp1, exp2);
                        Expression le = MakeFunction(t, "<=", exp1, exp3);
                        exp1 = MakeFunction(t, "and", ge, le);
                        // Insert NOT function if necessary.
                        if (notbet) exp1 = MakeFunction(t, "not", exp1);
                      } ]

  { return exp1; }
}

// LIKE and NOT LIKE - we rewrite as a function.
Expression LikeExpression():
{ Expression exp1, exp2;
  Token t;
  bool like = true;
}
{   exp1 = AdditionExpression()
        [ LOOKAHEAD(2) [ <NOT> { like = false; } ]
          t=<LIKE> exp2 = AdditionExpression()
               { if (like) exp1 = MakeFunction("@like_sql", exp1, exp2);
                 else exp1 = MakeFunction("@nlike_sql", exp1, exp2);
                 exp1.Line = t.beginLine;
				 exp1.Column = t.beginColumn;
               }
        ]
  { return exp1; }
}


Expression AdditionExpression():
{ Token t;
  Expression exp1, exp2; }
{   exp1 = MultiplicationExpression()
               ( LOOKAHEAD(1)
                 ( t=<ADD> | t=<SUBTRACT> ) exp2 = MultiplicationExpression() 
                       { exp1 = MakeFunction(t, t.image, exp1, exp2); } )*

  { return exp1; }
}

Expression MultiplicationExpression():
{ Expression exp1, exp2;
  Token t;
}
{   exp1 = UnaryExpression()
               ( LOOKAHEAD(1)
                 ( t=<STAR> | t=<DIVIDE> ) exp2 = UnaryExpression()
                       { exp1 = MakeFunction(t, t.image, exp1, exp2); } )*

  { return exp1; }
}


Expression UnaryExpression():
{ Expression exp; }
{  (   "(" ( exp = Expression() ) ")"

// a user function
     | LOOKAHEAD(2) exp = FFunctionExpression()

     | exp = FVarExpression()
     | exp = FStaticExpression()
//     | exp = DateTimeTimestampStatic()
     | exp = ParameterReference()

// Lookahead here for 'NOT [expression]' and 'NOT EXISTS ( nested query )'
     | LOOKAHEAD(2) exp = ExistsExpression()
     | exp = NotExpression()
   )
   { return exp; }
}

// Handle SQL NOT.  If "(" follows the NOT then we parse the nested expression.
// Otherwise, we parse the next expression without logical operations.

Expression NotExpression():
{ Expression exp; }
{  <NOT>
   (   LOOKAHEAD(1) "(" exp=Expression() ")"
     | exp=NonLogicalExpression()
   )

  { return MakeFunction("not", exp); }
}

// Exists nested query operation

Expression ExistsExpression():
{ Expression exp;
  bool notExists = false;
}
{
  [ <NOT> { notExists = true; } ] <EXISTS> "(" exp = NestedSelect() ")"

  { if (notExists) return MakeFunction("notexists", exp);
    else return MakeFunction("exists", exp);
  }
}

// A user function
Expression FFunctionExpression():
{ Expression exp; }
{ (   LOOKAHEAD(1) exp = UTrimFunction()
    | LOOKAHEAD(1) exp = UCastFunction()
    | exp = UFunction()
  )

  { return exp; }
}

Expression UFunction():
{ Token t;
  ArrayList paramList;
  bool distinct = false;
  bool glob_use = false;
}
{ t=SQLIdentifier()
          "(" [ ( <ALL> | <DISTINCT> { distinct = true; } ) ]
              (   paramList = AggregateGlob() { glob_use = true; }
                | paramList = ExpressionList()
              ) ")"

  { Expression exp = UserFunction(t, t.image, paramList);
    if (distinct) exp.SetArgument("is_distinct", true);
    if (glob_use) exp.SetArgument("glob_use", true);
    return exp;
  }
}


Expression UTrimFunction():
{ Token t1, t2 = null;
  Expression exp1 = null, exp2;
}
{ t1 = <TRIM> "("
     [ LOOKAHEAD(3)
        [ t2=<LEADING> | t2=<BOTH> | t2=<TRAILING> ] [ exp1=FStaticExpression() ] <FROM> ]
     exp2 = Expression() ")"

  { String str_type = t2 == null ? "both" : t2.image.ToLower(CultureInfo.InvariantCulture);
    if (exp1 == null) exp1 = FetchStatic(new SqlObject(" "));
    ArrayList list = new ArrayList();
    list.Add(FetchStatic(new SqlObject(str_type)));
    list.Add(exp1);
    list.Add(exp2);
    return UserFunction(t1, t1.image, list);
  }
}

Expression UCastFunction():
{ Token t;
  Expression exp;
  SqlType ttype;
}
{ t = <CAST> "(" exp = Expression() <AS> ttype = GetSqlType() ")"

  { ArrayList list = new ArrayList();
    list.Add(exp);
    list.Add(FetchStatic(new SqlObject(ttype.ToString())));
    return UserFunction(t, t.image, list);
  }
}

Expression FVarExpression():
{ Token t; }
{  t = VariableReference()
   { Expression exp = FetchVariable(t);
     exp.Line = t.beginLine;
	 exp.Column = t.beginColumn;
     return exp;
   }
}

Expression FStaticExpression():
{ SqlObject ob; }
{  ob = TypedValueStatic()
   { return FetchStatic(ob); }
}

// '*' in an aggregate expression
ArrayList AggregateGlob():
{ }
{  <STAR>
   { ArrayList list = new ArrayList();
     Expression glob_ob = new FunctionExpression("star");
     list.Add(glob_ob);
     return list;
   }
}

// Context parameter reference

Expression ParameterReference():
{ Token t; }
{  t=<QUESTION>
   { Expression exp = FetchParam(GetParameterId());
     exp.Line = t.beginLine;
	 exp.Column = t.beginColumn;
     return exp;
   }
}

// Set expression Right Hand Side (eg. a IN ( 4, 2 ) )

Expression SetRHSExpression():
{ Token t;
  Expression exp;
  ArrayList expList;
}
{  t="("
     (
         exp = NestedSelect()
       | expList = ExpressionList()
          { exp = UserFunction(t, "nested_list", expList); }
     )
   ")"

   { return exp; }
}

// String literal

SqlObject StringLiteral() :
{ Token t; }
{  t = <STRING_LITERAL>
   { return (SqlObject) Util.ToParamObject(t, false); }
}

// Returns a SqlObject static value

SqlObject TypedValueStatic() :
{ Token t; }
{ (  (  t=<STRING_LITERAL>
      | t=<BOOLEAN_LITERAL>
      | t=<NULL_LITERAL>
     )  { return (SqlObject) Util.ToParamObject(t, false); }
    |
     (  t=<NUMBER_LITERAL>
     )  { return (SqlObject) Util.ParseNumberToken(t, false); }
  )
}

// Date, Time, Timestamp statics (eg. 'DATE '2003-12-1'').

Expression DateTimeTimestampStatic():
{ Token t;
  String fname;
  SqlObject string_ob;
}
{ (   t=<DATE>       { fname="DATEOB"; }
    | t=<TIME>       { fname="TIMEOB"; }
    | t=<TIMESTAMP>  { fname="TIMESTAMPOB"; }
  )
  string_ob = StringLiteral()

  { Expression exp = MakeFunction(fname, FetchStatic(string_ob));
    exp.Line = t.beginLine;
	exp.Column = t.beginColumn;
    return exp;
  }
}

void GetStringSQLType() :
{ }
{
    LOOKAHEAD(2) ( <CHARACTER> <VARYING> )
  | LOOKAHEAD(3) ( <LONG> <CHARACTER> <VARYING> )
  | ( <TEXT> | <STRING> | <LONGVARCHAR> )
  | ( <CHAR> | <CHARACTER> )
  | <VARCHAR>
  | <CLOB>
}

void GetNumericSQLType() :
{ }
{
    ( <INT> | <INTEGER> )
  | <TINYINT>
  | <SMALLINT>
  | <BIGINT>
  | <LONG>
  | <FLOAT>
  | <REAL>
  | <DOUBLE>
  | <NUMERIC>
  | <DECIMAL>
  | <NUMBER>
}

void GetBooleanSQLType() :
{ }
{
  ( <BOOLEAN> | <BIT> )
}

void GetDateSQLType() :
{ }
{
    <TIMESTAMP>
  | <TIME>
  | <DATE>
}

void GetBinarySQLType() :
{ }
{
    LOOKAHEAD(2) ( <BINARY> <VARYING> )
  | LOOKAHEAD(3) ( <LONG> <BINARY> <VARYING> )
  | <LONGVARBINARY>
  | <VARBINARY>
  | <BINARY>
  | <BLOB>
}

// Parses an SQL type and forms a Type object.  For example, "CHAR(500)" is
// parsed to a string Type with a maximum size of 500 and lexicographical
// collation.
SqlType GetSqlType() :
{ Token t;
  int size = -1;
  int scale = -1;
  string loc = null;
}
{
  (
      LOOKAHEAD(GetStringSQLType())
      GetStringSQLType() [ "(" size = PositiveIntegerConstant() ")" ]
      [ <COLLATE> t=<STRING_LITERAL> { loc = ((SqlObject) Util.ToParamObject(t, false)).ToString(); } ]
      { return new SqlType(SqlTypeCode.String, size, loc); }

    | LOOKAHEAD(GetBinarySQLType())
      GetBinarySQLType() [ "(" size = PositiveIntegerConstant() ")" ]
      { return new SqlType(SqlTypeCode.Binary, size); }

    | GetNumericSQLType() [ "(" size = PositiveIntegerConstant()
                             [ "," scale = PositiveIntegerConstant() ] ")" ]
      { return new SqlType(SqlTypeCode.Numeric, size, scale); }
	        
    | GetBooleanSQLType()
      { return new SqlType(SqlTypeCode.Boolean); }

    | GetDateSQLType()
      { return new SqlType(SqlTypeCode.DateTime); }


  )
}

// Parses a simple positive integer constant.
int PositiveIntegerConstant() :
{ Token t; }
{
  t = <NUMBER_LITERAL>

  { int val = Int32.Parse(t.image);
    if (val < 0) GenerateParseException();
    return val;
  }
}

// A none deliminated reference
String NoneDeliminatedReference() :
{ Token name; }
{
  ( name = <QUOTED_DELIMINATED_REF> | name = SQLIdentifier() )

  { return Util.AsNonQuotedRef(name); }
}

// Parses a column name as a Variable object  

Token VariableReference() :
{ Token name; }
{
  ( name = SQLIdentifier() |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return name; } 
}

// Set comparison type (eg, > ALL, = ANY, etc)

String SetComparisonType():
{ Token t1, t2;
}
{    ( (   t1=<ASSIGNMENT> | t1=<NOTEQ>
       | t1=<GR> | t1=<LE> | t1=<GREQ> | t1=<LEEQ> )
       ( t2=<ALL> | t2=<ANY> )
       { return t2.image + t1.image; }
     )
   |
     ( <IN> { return "any="; } | <NOT> <IN> { return "all<>"; } )
}

// Parses an SQL identifier
Token SQLIdentifier() :
{ Token name; }
{
  (   name = <IDENTIFIER>
    | name = <IF> | name = <OPTION> | name = <ACCOUNT>
    | name = <PASSWORD> | name = <PRIVILEGES> | name = <GROUPS>
    | name = <LANGUAGE> | name = <NAME> | name = <CSHARP>
    | name = <ACTION> | name = <COMPARABLE> | name = <TRIM>
    | name = <USER> | name = <CAST>
  )
  
  { return name; }
}