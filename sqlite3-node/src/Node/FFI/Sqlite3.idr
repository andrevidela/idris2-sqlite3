module Node.FFI.Sqlite3

import public Sqlite3.Types
import Control.Monad.Continuation

export
data DBNode : Type where [external]

export
data StmtNode : Type where [external]

%foreign """
    node:lambda: (name, err, suc) => {
      const sqlite3 = require('sqlite3').verbose();
      const db = new sqlite3.Database(name, (error) => {
        if (error === null) {
            console.log("successfully open db with name: " + name);
            suc(db)();
        } else {
            console.log(JSON.stringify(error));
            err()();
        }
      });
    }
    """
node__sqlite_open : (path : String) ->
    (errCb : Int -> PrimIO ()) ->
    (succCb : DBNode -> PrimIO ()) -> PrimIO ()

export
sqlite_open :
    (path : String) -> (callback : Either SqlError DBNode -> IO ()) -> IO ()
sqlite_open path cb = primIO $ node__sqlite_open path
    (\x => toPrim (cb (Left $ ResultError (fromInt x) "unable to open connect to \{path}")))
    (\x => toPrim (cb (Right x)))

export
sqliteOpen : (path : String) ->
             ContT () IO (Either SqlError DBNode)
sqliteOpen path = MkContT (sqlite_open path)

%foreign """
    node:lambda:(db, ok, err) => {
      db.close(e => {
        if (e === null) {
           ok()();
         } else {
           err()();
         }
      });
    }
    """
node__sqlite_close : DBNode ->
    (okCb : () -> PrimIO ()) ->
    (errCb : Int -> PrimIO ()) -> PrimIO ()

export
sqlite_close : DBNode ->
    (cb : SqlResult -> IO ()) ->
    IO ()
sqlite_close db cb =
  primIO $ node__sqlite_close db
      (\x => toPrim $ cb SQLITE_OK)
      (\x => toPrim $ cb (fromInt x))

export
sqliteClose : DBNode -> ContT () IO SqlResult
sqliteClose db = MkContT (sqlite_close db)

%foreign """
    node:lambda: (db, stmt, suc, err) => {
      db.prepare(stmt, (error) => {
        if (error === null) {
          suc();
        } else {
          err(error);
        }
      });
    }
    """
node__sqlite_prepare : DBNode -> String ->
  (success : () -> PrimIO ()) ->
  (error : SqlError -> PrimIO ()) ->
  PrimIO ()

export
sqlite_prepare : DBNode -> String ->
  (Either SqlError () -> IO ()) -> IO ()
sqlite_prepare db stmt cb = primIO $
  node__sqlite_prepare db stmt
    (\x => toPrim $ cb (Right ()))
    (\x => toPrim $ cb (Left x))

sqlitePrepare : (db : DBNode) => String -> ContT () IO (Either SqlError ())
sqlitePrepare str = MkContT (sqlite_prepare db str)

