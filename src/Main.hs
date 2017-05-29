{-  -*- coding:utf-8 -*-  -}
{-# LANGUAGE DeriveDataTypeable #-}

module Main where

import System.Environment
import System.Console.CmdArgs as CMD
import Control.Monad
import Data.Maybe (isNothing, fromJust, fromMaybe)
import Data.List
import Utils (isSpace)
import qualified Data.Map as M

import Paths_oraddl (version)
import Data.Version (showVersion)

--import OracleUtils
import RetrieveDDL

programVersion :: String
programVersion =
  showVersion version ++ " (haskell)"

data Flags = Flags 
             {
               conn :: Maybe String,
               tables :: Maybe String,
               views :: Maybe String,
               sources :: Maybe String,
               triggers :: Maybe String,
               synonyms :: Maybe String,
               sequences :: Maybe String,
               mviews :: Maybe String,
               mviewlogs :: Maybe String,
               list :: Maybe String,
               listf :: Maybe FilePath,
               directory :: String,
               saveDollared :: Bool,
               saveEndSpaces :: Bool,
               saveAutoGenerated :: Bool
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
          tables =
            def
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of tables to retrieve. Names are case sensitive. If you specify no names then all the tables will be retrieved",
          views =
            def
            &= explicit &= name "views"
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of views to retrieve. Names are case sensitive. If you specify no names then all the views will be retrieved",
          sources =
            def
            &= explicit &= name "sources"
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of sources (packages, functions, procedures, java, types) to retrieve. Names are case sensitive. If you specify no names then all the sources will be retrieved",
          triggers =
            def
            &= explicit &= name "triggers"
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of triggers to retrieve. Names are case sensitive. If you specify no names then all the triggers will be retrieved",
          synonyms =
            def
            &= explicit &= name "synonyms"
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of synonyms to retrieve. Names are case sensitive. If you specify no names then all the synonyms will be retrieved",
          sequences =
            def
            &= explicit &= name "sequences"
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of sequences to retrieve. Names are case sensitive. If you specify no names then all the sequences will be retrieved",
          mviews =
            def
            &= explicit &= name "mviews"
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of materialized views to retrieve. Names are case sensitive. If you specify no names then all the materialized views will be retrieved",
          mviewlogs =
            def
            &= explicit &= name "mview-logs"
            &= typ "NAME,NAME,..."
            &= opt ""
            &= help "Names of tables for witch to retrieve materialized view logs. Names are case sensitive. If you specify no names then all materialized view logs will be retrieved",
          list =
            def
            &= explicit &= name "list"
            &= typ "NAME,NAME,..."
            &= help "Names of objects of any type to retrieve. Names are case sensitive. Use this option instead of --tables, --views, etc. if the type of objects is unknown",
          listf =
            def
            &= typ "FILE"
            &= help "File with list of intresting objects (one object per line). Names are case sensitive",
          directory =
            "."
            &= explicit &= name "dir"
            &= typ "DIRNAME"
            &= help "Directory where to put files whith SQL (default is current directory)",
          saveDollared =
            def
            &= explicit &= name "save-dollared"
            &= help "By default objects with names containing the dollar sign ($) in their names are ignored if not specified explicitly via --tables, --list and etc. This option enables saving of such objects.",
          saveEndSpaces =
            def
            &= explicit &= name "save-end-spaces"
            &= help "By default insignificant spaces at end of each line are deleted, this option prevent this behavior",
          saveAutoGenerated =
            def
            &= explicit &= name "save-auto-generated"
            &= help "By default auto-generated objects (like indexes supporting constraints) not saved, this option enables auto-generated objects"
          }
        &= program programName
        &= summary ("Retrieves DDL SQL for all or listed objects of the specified Oracle database schema. Version " ++ programVersion)
  
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


splitByComma :: String -> [String]
splitByComma = filter (/=",") . groupBy (\x y -> x/=',' && y/=',')

translateOptions :: Flags -> IO Options
translateOptions flags = do
  obj_list <- getObjList
  return
   Options
   {
    oConn = fromJust $ conn flags,
    oSchema = Nothing,
    oObjList = obj_list,
    oByTypeLists = ByTypeLists {
                     oTables    = (uniqify . splitByComma) `fmap` tables flags,
                     oViews     = (uniqify . splitByComma) `fmap` views flags,
                     oMViews    = (uniqify . splitByComma) `fmap` mviews flags,
                     oMViewLogs = (uniqify . splitByComma) `fmap` mviewlogs flags,
                     oSources   = (uniqify . splitByComma) `fmap` sources flags,
                     oSequences = (uniqify . splitByComma) `fmap` sequences flags,
                     oSynonyms  = (uniqify . splitByComma) `fmap` synonyms flags,
                     oTriggers  = (uniqify . splitByComma) `fmap` triggers flags
                   },
    oOutputDir = directory flags,
    oSaveDollared = saveDollared flags,
    oSaveEndSpaces = saveEndSpaces flags,
    oSaveAutoGenerated = saveAutoGenerated flags
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
      
      let x = uniqify $ ox ++ maybe [] splitByComma (list flags)
      return $ if null x then Nothing else Just x


