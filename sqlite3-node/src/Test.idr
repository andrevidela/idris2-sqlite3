module Test

import Node.FFI.Sqlite3
import Control.Monad.Continuation

main : IO ()
main = runCont $ do
  Right db <- sqliteOpen ":memory:"
    | Left err => putStrLn (show err)
  result <- sqliteClose db
  putStrLn "closed db with result \{show result}"
  {-
main = sqlite_open "lol" $ \case
  (Right _) => do
      putStrLn "success"
      sqlite_open "lol" (
          \case (Right _) => putStrLn "success"
                (Left _) => putStrLn "error")
  (Left _) => putStrLn "error"
