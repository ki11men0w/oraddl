{-  -*- coding:utf-8 -*-  -}
{-# LANGUAGE DeriveDataTypeable #-}

module Main where

import System.IO
import System.Environment
import System.Console.CmdArgs as CMD
import Control.Monad
import Data.Maybe (isNothing, isJust, fromJust, fromMaybe)
import Data.List
import Utils (isSpace)
import qualified Data.Map as M

--import OracleUtils
import RetrieveDDL

programVersion = "0.0.0.1"

data Flags = Flags 
             {
               conn :: Maybe String,
               schema :: Maybe String,
               tables :: Maybe [String],
               views :: Maybe [String],
               sources :: Maybe [String],
               triggers :: Maybe [String],
               synonyms :: Maybe [String],
               sequences :: Maybe [String],
               list :: Maybe [String],
               listf :: Maybe FilePath,
               directory :: String,
               saveEndSpaces :: Bool
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
          tables =
            def
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of tables to retrieve. Names are case sensitive",
          views =
            def
            &= explicit &= name "views"
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of views to retrieve. Names are case sensitive",
          sources =
            def
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of sources (packages, functions, procedures, java, types) to retrieve. Names are case sensitive",
          triggers =
            def
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of triggers to retrieve. Names are case sensitive",
          synonyms =
            def
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of synonyms to retrieve. Names are case sensitive",
          sequences =
            def
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of sequences to retrieve. Names are case sensitive",
          list =
            def
            &= typ "NAME,NAME,..."
            &= help "Names of objects to retrieve. Names are case sensitive. Use this option instead of --tables, --views, etc. if the type of objects is unknown",
          listf =
            def
            &= typ "FILE"
            &= help "File with list of intresting objects. Names are case sensitive",
          directory =
            "."
            &= explicit &= name "dir"
            &= typ "DIRNAME"
            &= help "Directory where to put files whith SQL (default is current directory)",
          saveEndSpaces =
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
    oConn = fromJust $ conn flags,
    oSchema = schema flags,
    oObjList = obj_list,
    oByTypeLists = ByTypeLists {
                     oTables    = uniqify `fmap` (tables flags),
                     oViews     = uniqify `fmap` (views flags),
                     oSources   = uniqify `fmap` (sources flags),
                     oSequences = uniqify `fmap` (sequences flags),
                     oSynonyms  = uniqify `fmap` (synonyms flags),
                     oTriggers  = uniqify `fmap` (triggers flags)
                   },
    oOutputDir = directory flags,
    oSaveEndSpaces = saveEndSpaces flags
    }
  where
    uniqify = M.keys . M.fromList . map (\x -> (x,())) . filter (not . null)

    getObjList = do
      ox <- case listf flags of
        Just f -> do
          lx <- lines `liftM` readFile f
          return [ dropWhileEnd isSpace . dropWhile isSpace $ x
                 | x <- lx,
                   not . all isSpace $ x,
                   not . null $ x
                 ]
        _ -> return []
      
      let x = uniqify $ ox ++ fromMaybe [] (list flags)
      return $ if null x then Nothing else Just x


