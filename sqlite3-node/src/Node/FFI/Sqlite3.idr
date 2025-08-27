module Node.FFI.Sqlite3

import public Sqlite3
import Control.Monad.Continuation
import Data.Buffer
import Data.ByteString
import Data.ByteVect
import Data.IORef
import Node.FFI

import System

export
data DB : Type where [external]

export
data Stmt : Type where [external]

%foreign """
    node:lambda: (name) => {
      const sqlite = require('better-sqlite3');
      return new Database(name);
    }
    """
node__sqlite_open : (path : String) -> PrimIO DB

export
sqlite_open :
    (path : String) -> IO DB
sqlite_open path = primIO $ node__sqlite_open path

%foreign """
    node:lambda:(db) => db.close()
    """
node__sqlite_close : DB -> PrimIO ()

export
sqlite_close : DB -> IO ()
sqlite_close db =
  primIO $ node__sqlite_close db

%foreign """
    node:lambda: (db, stmt) => db.prepare(stmt);
    """
node__sqlite_prepare : DB -> String -> PrimIO Stmt

export
sqlite_prepare : (db :DB) => String -> IO Stmt
sqlite_prepare stmt = primIO $ node__sqlite_prepare db stmt


%foreign "node:lambda: (db, stmt, params) => stmt.bind(params)"
node__bind_all : DB -> Stmt -> JSArray -> PrimIO Stmt

byteStringGetBuffer : ByteString -> Buffer
byteStringGetBuffer (BS size (BV buf offset lte)) = unsafePerformIO $ do
  Just newBuf <- newBuffer (cast size)
  | Nothing => die "could not allocate buffer"
  copyData (unsafeGetBuffer buf) (cast offset) (cast size) newBuf 0
  pure newBuf

paramsToJS : List Parameter -> List JSValue
paramsToJS = map ?paramsToJS_rhs
  where
    param2JS : Parameter -> JSValue
    param2JS (P name BLOB bs) = buffer_to_JS (byteStringGetBuffer bs)
    param2JS (P name TEXT value) = str_to_JS value
    param2JS (P name INTEGER value) = int64_to_JS value
    param2JS (P name REAL value) = double_to_JS value

total export
bindParams : (db : DB) => (stmt : Stmt) => List Parameter -> IO Stmt
bindParams ps = primIO $ node__bind_all db stmt (list2JS $ paramsToJS ps)

export
bindParam : DB => (s : Stmt) => Parameter -> IO Stmt
bindParam p = bindParams [p]

