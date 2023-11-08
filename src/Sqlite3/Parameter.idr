module Sqlite3.Parameter

import public Control.Monad.State

import Data.Buffer.Indexed
import Data.ByteString
import Data.List.Quantifiers
import Data.Maybe
import Data.SortedMap
import Data.String

import Sqlite3.Cmd
import Sqlite3.Expr
import Sqlite3.Marshall
import Sqlite3.Table
import Sqlite3.Types

%default total

||| Parameter to be bound in an SQL statement.
public export
record Parameter where
  constructor P
  name  : String
  type  : SqliteType
  value : IdrisType type

||| State type used to keep track of the parameters used in an
||| SQLite statement that is being assembled.
public export
record ParamST where
  constructor PS
  ix   : Nat
  args : List Parameter

||| Initial list of parameters
export
init : ParamST
init = PS 0 []

||| Utility alias for an SQL statement with parameters.
public export
0 ParamStmt : Type
ParamStmt = State ParamST String

--------------------------------------------------------------------------------
-- Encode with parameters
--------------------------------------------------------------------------------

encOp : String -> Expr s t -> Expr s t -> ParamStmt

encPrefix : String -> Expr s t -> ParamStmt

encExprs : SnocList String -> List (Expr s t) -> ParamStmt

encFun1 : String -> Expr s t -> ParamStmt

encFun : String -> List (Expr s t) -> ParamStmt

||| Encodes an expression, generating a list of parameters with
||| unique names that will be bound when running the SQL statement.
export
encodeExprP : Expr s t -> ParamStmt
encodeExprP (Lit t v)    =
  state $ \(PS x as) =>
    let s := ":\{show x}"
     in (PS (S x) (P s t v :: as), s)

encodeExprP (Add x y)    = encOp "+" x y
encodeExprP (Mult x y)   = encOp "*" x y
encodeExprP (Sub x y)    = encOp "-" x y
encodeExprP (Div x y)    = encOp "/" x y
encodeExprP (Mod x y)    = encOp "%" x y
encodeExprP (Abs y)       = encFun1 "abs" y
encodeExprP (x < y)      = encOp "<" x y
encodeExprP (x > y)      = encOp ">" x y
encodeExprP (x <= y)     = encOp "<=" x y
encodeExprP (x >= y)     = encOp ">=" x y
encodeExprP (x == y)     = encOp "==" x y
encodeExprP (x /= y)     = encOp "!=" x y
encodeExprP (IS x y)     = encOp "IS" x y
encodeExprP (IS_NOT x y) = encOp "IS NOT" x y
encodeExprP (x && y)     = encOp "AND" x y
encodeExprP (x || y)     = encOp "OR" x y
encodeExprP (x ++ y)     = encOp "||" x y
encodeExprP (x .&. y)    = encOp "&" x y
encodeExprP (x .|. y)    = encOp "|" x y
encodeExprP (ShiftR x y) = encOp ">>" x y
encodeExprP (ShiftL x y) = encOp "<<" x y
encodeExprP (Neg x)      = encPrefix "-" x
encodeExprP (NOT x)      = encPrefix "NOT" x
encodeExprP (Raw s)      = pure s
encodeExprP NULL         = pure "NULL"
encodeExprP TRUE         = pure "1"
encodeExprP FALSE        = pure "0"
encodeExprP (C c)        = pure c
encodeExprP (Coalesce xs)     = encFun "coalesce" xs
encodeExprP (Count x)         = encFun1 "count" x
encodeExprP (Avg x)           = encFun1 "avg" x
encodeExprP (Sum x)           = encFun1 "sum" x
encodeExprP (Min x)           = encFun1 "min" x
encodeExprP (Max x)           = encFun1 "max" x
encodeExprP (GroupConcat x s) = do
  ex <- encodeExprP x
  pure "group_concat(\{ex}, \{s})"

encodeExprP CURRENT_TIME      = pure "CURRENT_TIME"
encodeExprP CURRENT_DATE      = pure "CURRENT_DATE"
encodeExprP CURRENT_TIMESTAMP = pure "CURRENT_TIMESTAMP"
encodeExprP (LIKE x y)        = encOp "LIKE" x y
encodeExprP (GLOB x y)        = encOp "GLOB" x y

encodeExprP (IN x xs) = do
  s  <- encodeExprP x
  ss <- encExprs [<] xs
  pure "(\{s}) IN (\{ss})"

encOp s x y = do
  sx <- encodeExprP x
  sy <- encodeExprP y
  pure $ "(\{sx} \{s} \{sy})"

encPrefix s x = do
  sx <- encodeExprP x
  pure $ "\{s}(\{sx})"

encExprs sc []      = pure . commaSep id $ sc <>> []
encExprs sc (x::xs) = do
  v <- encodeExprP x
  encExprs (sc :< v) xs

encFun f xs      = do
  exs <- encExprs [<] xs
  pure "\{f}(\{exs})"

encFun1 f x      = do
  ex <- encodeExprP x
  pure "\{f}(\{ex})"

--------------------------------------------------------------------------------
-- Encoding Commands
--------------------------------------------------------------------------------

record Constraints where
  constructor CS
  colConstraints : SortedMap String String
  tblConstraints : List String

addCol : Constraints -> (col, constraint : String) -> Constraints
addCol (CS cs ts) n v =
  case lookup n cs of
    Just s  => CS (insert n (s ++ " " ++ v) cs) ts
    Nothing => CS (insert n v cs) ts

addTbl : Constraints -> String -> Constraints
addTbl (CS cs ss) s = CS cs (s::ss)

names : SnocList String -> LAll (TColumn t) ts -> String
names sc []           = commaSep id (sc <>> [])
names sc (TC c :: cs) = names (sc :< c) cs

encodeDflt : Expr s t -> String
encodeDflt x         = "DEFAULT (\{encodeExpr x})"

references : (t : Table) -> LAll (TColumn t) xs -> String
references t cs = "REFERENCES \{t.name} (\{names [<] cs})"

encConstraint : Constraints -> Constraint t -> Constraints
encConstraint y (NotNull $ TC n)       = addCol y n"NOT NULL"
encConstraint y (AutoIncrement $ TC n) = addCol y n "AUTOINCREMENT"
encConstraint y (Unique [TC n])        = addCol y n "UNIQUE"
encConstraint y (PrimaryKey [TC n])    = addCol y n "PRIMARY KEY"
encConstraint y (Default s expr)       = addCol y s (encodeDflt expr)
encConstraint y (Unique xs)            = addTbl y "UNIQUE (\{names [<] xs})"
encConstraint y (PrimaryKey xs)        = addTbl y "PRIMARY KEY (\{names [<] xs})"
encConstraint y (Check x)              = addTbl y "CHECK (\{encodeExpr x})"
encConstraint y (ForeignKey s [p] ys)  = addCol y p.name (references s ys)
encConstraint y (ForeignKey s xs ys)   =
  addTbl y "FOREIGN KEY (\{names [<] xs}) \{references s ys}"

ine : Bool -> String
ine True  = "IF NOT EXISTS"
ine False = ""

ie : Bool -> String
ie True  = "IF EXISTS"
ie False = ""

encodeCols : SortedMap String String -> List Column -> List String
encodeCols m = map encodeCol
  where
    encodeCol : Column -> String
    encodeCol (C n t) =
      let constraints := fromMaybe "" (lookup n m)
       in "\{n} \{show t} \{constraints}"

insertCols : SnocList String -> LAll (TColumn t) ts -> String
insertCols sc []         = commaSep id (sc <>> [])
insertCols sc (TC c::cs) = insertCols (sc :< c) cs

exprs : SnocList String -> LAll (Expr s) ts -> ParamStmt
exprs sc []      = pure $ commaSep id (sc <>> [])
exprs sc (c::cs) = do
  s <- encodeExprP c
  exprs (sc :< s) cs

updateVals : SnocList String -> List (Val t) -> ParamStmt
updateVals sc []        = pure $ commaSep id (sc <>> [])
updateVals sc (x :: xs) = do
  v <- encodeExprP x.val
  updateVals (sc :< "\{x.name} = \{v}") xs

||| Encodes an SQLite data management command.
|||
||| The command will be encoded as a string with parameters
||| inserted as placeholders for literal values where appropriate.
|||
||| `State ParamST` is used to keep track of the defined parameters.
export
encodeCmd : Cmd t -> ParamStmt
encodeCmd (CREATE_TABLE t cs ifNotExists) =
  let CS m ts := foldl encConstraint (CS empty []) cs
      cols    := encodeCols m t.cols
      add     := commaSep id (cols ++ ts)
   in pure "CREATE TABLE \{ine ifNotExists} \{t.name} (\{add});"
encodeCmd (DROP_TABLE t ifExists) =
   pure "DROP TABLE \{ie ifExists} \{t.name};"
encodeCmd (INSERT t cs vs) = do
  vstr <- exprs [<] vs
  pure "INSERT INTO \{t.name} (\{insertCols [<] cs}) VALUES (\{vstr});"
encodeCmd (REPLACE t cs vs) = do
  vstr <- exprs [<] vs
  pure "REPLACE INTO \{t.name} (\{insertCols [<] cs}) VALUES (\{vstr});"
encodeCmd (UPDATE t vs wh) = do
  vstr <- updateVals [<] vs
  xstr <- encodeExprP wh
  pure "UPDATE \{t.name} SET \{vstr} WHERE \{xstr};"
encodeCmd (DELETE t wh) = do
  xstr <- encodeExprP wh
  pure "DELETE FROM \{t.name} WHERE \{xstr};"

joinPred : JoinPred s t -> ParamStmt
joinPred (Left u)  = pure "USING (\{commaSep name u})"
joinPred (Right x) = do
  ez <- encodeExprP x
  pure "ON \{ez}"

tbl : Table -> String
tbl (T n a _) = if n == a then n else "\{n} AS \{a}"

join : Join s t -> ParamStmt
join (JOIN t p) = do
  ep <- joinPred p
  pure "JOIN \{tbl t} \{ep}"

join (OUTER_JOIN t p) = do
  ep <- joinPred p
  pure "LEFT OUTER JOIN \{tbl t} \{ep}"

join (CROSS_JOIN t) = pure "CROSS JOIN \{tbl t}"
join (FROM t)       = pure "FROM \{tbl t}"

encodeFrom : From s -> ParamStmt
encodeFrom [<]      = pure ""
encodeFrom [<x]     = join x
encodeFrom (x :< y) = do
  ef <- encodeFrom x
  ej <- join y
  pure "\{ef} \{ej}"

asc : AscDesc -> String
asc NoAsc = ""
asc ASC   = "ASC"
asc DESC  = "DESC"

collate : Collation t -> String
collate None   = ""
collate NOCASE = "COLLATE NOCASE"

encodeOrderingTerm : OrderingTerm t -> ParamStmt
encodeOrderingTerm (O expr coll a) = do
  ex <- encodeExprP expr
  pure "\{ex} \{collate coll} \{asc a}"

ots : SnocList String -> List (OrderingTerm t) -> ParamStmt
ots ss []      = pure $ commaSep id (ss <>> [])
ots ss (x::xs) = do
  s <- encodeOrderingTerm x
  ots (ss :< s) xs

encodeOrd : String -> List (OrderingTerm t) -> ParamStmt
encodeOrd s [] = pure ""
encodeOrd s xs = do
  str <- ots [<] xs
  pure "\{s} \{str}"


||| Encodes an SQLite `SELECT` statement.
|||
||| The query will be encoded as a string with parameters
||| inserted as placeholders for literal values where appropriate.
export
encodeQuery : Query ts -> ParamStmt
encodeQuery (SELECT vs from where_ group_by order_by) = do
  vstr <- exprs [<] vs
  fstr <- encodeFrom from
  wh   <- encodeExprP where_
  grp  <- encodeOrd "GROUP BY" group_by
  ord  <- encodeOrd "ORDER BY" order_by
  pure "SELECT \{vstr} \{fstr} WHERE \{wh} \{grp} \{ord}"
