module Control.Node.Sqlite3

import public Control.RIO.App
import public Control.Monad.Continuation
import public Control.Monad.Either
import public Sqlite3
import public Data.List.Quantifiers
import public Node.FFI.Sqlite3

%default total

export
withDB : String -> (DBNode => IO a) -> IO a
withDB path op = do
  db <- sqlite_open path
  v <- op @{db}
  result <- sqlite_close db
  pure v

export
withStmt : DBNode => String -> (StmtNode => IO a) -> IO a
withStmt str f = do
  stmt <- sqlite_prepare str
  res <- f @{stmt}
  pure res

export
withBoundStmt : DBNode => ParamStmt -> (StmtNode => IO a) -> IO a
withBoundStmt st f =
  let (ps, str) := runState init st
  in Sqlite3.withStmt {a} str (bindParams ps.args *> f)



||| Executes the given SQL command.
export %inline
cmd : DBNode => Cmd t -> IO ()
cmd c = withBoundStmt (encodeCmd c) (pure ())

||| Prepare an SQL statement and use it to run the given effectful computation.
|||
||| This comes with the guarantees that the statement is properly
||| finalized at the end.
|||
||| This works just like `withStmt` but it also bind the given arguments.
export
bindParams : DBNode => ParamStmt => List Parameter -> IO ()
bindParams ps = ignore $ FFI.Sqlite3.bindParams ps

{-

  ||| Prepare an SQL statement and use it to run the given effectful computation.
  |||
  ||| This comes with the guarantees that the statement is properly
  ||| finalized at the end.
  |||
  ||| This works just like `withStmt` but it also bind the given arguments.
  export
  withBoundStmt : DB => ParamStmt -> (Stmt => App es a) -> App es a
  withBoundStmt st f =
    let (ps, str) := runState init st
     in withStmt str (bindParams ps.args >> f)

  ||| Runs an SQL statement, returning the response from the database.
  export
  step : (s : Stmt) => App es SqlResult
  step @{s} = liftIO $ sqliteStep s

  ||| Prepares and executes the given SQL query and extracts up to
  ||| `n` rows of results.
  export
  selectRows : DB => FromRow a => ParamStmt -> (n : Nat) -> App es (List a)
  selectRows st n = withBoundStmt st (injectIO $ loadRows n)

  ||| Prepares and executes the given SQL query and extracts the
  ||| first result.
  export
  selectRow : DB => FromRow a => ParamStmt -> App es a
  selectRow st = do
    [v] <- selectRows st 1 | _ => throw NoMoreData
    pure v

  ||| Prepares and executes the given SQL query and extracts the
  ||| first result (if any).
  export
  findRow : DB => FromRow a => ParamStmt -> App es (Maybe a)
  findRow st = do
    [v] <- selectRows st 1 | _ => pure Nothing
    pure $ Just v

--------------------------------------------------------------------------------
-- Runnings Commands
--------------------------------------------------------------------------------

  rollback : DB => HSum es -> App es a
  rollback x = ignore (withStmt "ROLLBACK TRANSACTION" step) >> fail x

  ||| Runs several commands in a single transaction.
  |||
  ||| If any of the commands fails, the whole transaction is rolled back.
  export %inline
  cmds : DB => Cmds -> App es ()
  cmds cs = do
    ignore $ withStmt "BEGIN TRANSACTION" step
    catch rollback (runCommands cs)
    ignore $ withStmt "COMMIT TRANSACTION" step

    where
      runCommands : Cmds -> App es ()
      runCommands []      = pure ()
      runCommands (c::cs) = cmd c >> runCommands cs

  ||| Runs the given query and accumulates at most `n` rows.
  export %inline
  query : DB => Query t -> (n : Nat) -> App es (List t)
  query q = selectRows (encodeQuery q)

  ||| Runs the given query and accumulates at most `n` rows.
  |||
  ||| The result is stored in a `Table` with a proper header of
  ||| column names.
  export
  queryTable :
       {auto db : DB}
    -> {auto tr : ToRow t}
    -> (q : Query t)
    -> {auto 0 prf : ToRowTypes t === FromRowTypes t}
    -> Nat
    -> App es (Table t)
  queryTable {prf} q n = do
    rs <- query q n
    pure (T (rewrite prf in hmap columnName q.columns) rs)
