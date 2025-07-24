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
data DBNode : Type where [external]

export
data StmtNode : Type where [external]

%foreign """
    node:lambda: (name) => {
      const sqlite = require('better-sqlite3');
      return new Database(name);
    }
    """
node__sqlite_open : (path : String) -> PrimIO DBNode

export
sqlite_open :
    (path : String) -> IO DBNode
sqlite_open path = primIO $ node__sqlite_open path

%foreign """
    node:lambda:(db) => db.close()
    """
node__sqlite_close : DBNode -> PrimIO ()

export
sqlite_close : DBNode -> IO ()
sqlite_close db =
  primIO $ node__sqlite_close db

%foreign """
    node:lambda: (db, stmt) => db.prepare(stmt);
    """
node__sqlite_prepare : DBNode -> String -> PrimIO StmtNode

export
sqlite_prepare : (db :DBNode) => String -> IO StmtNode
sqlite_prepare stmt = primIO $ node__sqlite_prepare db stmt


%foreign "node:lambda: (db, stmt, params) => stmt.bind(params)"
node__bind_all : DBNode -> StmtNode -> JSArray -> PrimIO StmtNode

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

export
bindParams : (db : DBNode) => (stmt : StmtNode) => List Parameter -> IO StmtNode
bindParams ps = primIO $ node__bind_all db stmt (list2JS $ paramsToJS ps)

export
bindParam : DBNode => (s : StmtNode) => Parameter -> IO StmtNode
bindParam p = bindParams [p]

