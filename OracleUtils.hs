module OracleUtils 
  (
   decodeConnectString,
   encodeConnectString
  ) where

import Data.List

type ConnectString = String
type User          = String
type Password      = String
type DatabaseName  = String

decodeConnectString :: ConnectString -> (User, Password, DatabaseName)
decodeConnectString s = 
  let user = takeWhile (/= '/') s
      dbname = reverse $ takeWhile (/= '@') $ reverse s
      password = let x = dropWhileEnd (/= '@') $ drop (length user + 1) s
                 in if null x then "" else init x
  in (user, password, dbname)

encodeConnectString :: User -> Password -> DatabaseName -> ConnectString
encodeConnectString user password dbname =
  user ++ "/" ++ password ++ "@" ++ dbname