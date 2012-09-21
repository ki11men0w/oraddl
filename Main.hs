{-  -*- coding:utf-8 -*-  -}
{-# LANGUAGE DeriveDataTypeable #-}

module Main where

import System.IO
import System.Environment
import System.Console.CmdArgs as CMD
import Control.Monad
import Data.Maybe (isNothing, isJust, fromJust, fromMaybe)
import Data.List
import Data.Char
import qualified Data.Map as M

--import OracleUtils
import RetrieveDDL

programVersion = "0.0.0.1"

data Flags = Flags 
             {
               conn :: Maybe String,
               schema :: Maybe String,
               list :: Maybe [String],
               listf :: Maybe FilePath,
               directory :: String,
               save_end_spaces :: Bool
             } deriving (Show, Data, Typeable)

getOpts :: IO Flags
getOpts = do
  programName <- getProgName
  opts <- cmdArgs $
          Flags {
          conn =
            def
            &= explicit &= name "connect"
            &= typ "user/password@db"
            &= help "Connect string",
          schema =
            def
            &= help "Schema for which objects you get DDL's",
          list =
            def
            &= typ "OBJ1,OBJ2,..."
            &= help "Object names delimited by comma (,). Object names are case sensitive",
          listf =
            def
            &= typ "FILE"
            &= help "File with list of intresting objects. Object names are case sensitive",
          directory =
            "."
            &= explicit &= name "dir"
            &= typ "DIRNAME"
            &= help "Directory where to put files whith SQL (default is current directory)",
          save_end_spaces =
            def
            &= explicit &= name "save-end-spaces"
            &= help "By default insignificant spaces at end of each line are deleted, this option prevent this behavior"
          }
        &= program programName
        &= summary ("Retrieves DDL for all or specified objects of oracle database schema. Version " ++ programVersion)
  
  checkOptions opts
  
  where
    checkOptions opts = do
      when (isNothing $ conn opts) $
        error "Option --connect is mandatory"
      
      return opts


main = do
  
  opts <- getOpts  >>= translateOptions
  
  retreiveDDL opts
  
  return ()

translateOptions :: Flags -> IO Options
translateOptions flags = do
  obj_list <- getObjList
  return
   Options
   {
    o_conn = fromJust $ conn flags,
    o_schema = schema flags,
    o_obj_list = obj_list,
    o_output_dir = directory flags,
    o_save_end_spaces = save_end_spaces flags
    }
  where
    getObjList = do
      ox <- case listf flags of
        Just f -> do
          lx <- lines `liftM` readFile f
          return [ (dropWhileEnd isSpace) . (dropWhile isSpace) $ x
                 | x <- lx,
                   not . all isSpace $ x,
                   not . null $ x
                 ]
        _ -> return []
      
      let x = M.keys $ M.fromList $ map (\x -> (x,())) $ ox ++ (fromMaybe [] (list flags))
      return $ if null x then Nothing else Just x


