{-# LANGUAGE ScopedTypeVariables #-}

module RetrieveDDL where

import System.IO
import System.FilePath
import Data.Maybe (fromJust, isNothing, isJust)
import Data.List
import Data.Char

import Text.Printf
import Control.Monad
import Control.Monad.Trans
import Database.Enumerator
import Database.Oracle.Enumerator

import OracleUtils

data Options = Options
               {
                 o_conn :: String,
                 o_schema :: Maybe String,
                 o_obj_list :: Maybe [String],
                 o_output_dir :: String,
                 o_save_end_spaces :: Bool
               }

getDefaultSchema = do
  let it :: (Monad m) => String -> IterAct m String
      it a accum = result' a
  r <- doQuery (sql "select user from dual") it []
  return r
  
getSchema schema = do
  case schema of
    Just s -> return s
    _      -> getDefaultSchema
  

getSafeName :: String -> String
getSafeName ora_name =
  if all (`elem` "1234567890QWERTYUIOPASDFGHJKLZXCVBNM_#$") stripped
  then stripped
  else "\"" ++ stripped ++ "\""
  
  where
    stripped =
      if length ora_name > 1 && head ora_name == '"' && last ora_name == '"'
      then init . tail $ ora_name
      else ora_name
          

getUnionAll :: [String] -> String
getUnionAll lst =
  case lst of
    [] -> "select 'x' from dual where 1 = 2"
    [x] -> select x
    x1:xs@(x2:_) -> select x1 ++ " union all " ++ getUnionAll xs
  where
    select = printf "select '%s' from dual"

clear0 x =
  map clear x
  where
    clear '\0' = ' '
    clear x    = x

chomp x =
  dropWhileEnd (=='\n') $ unlines $ map (dropWhileEnd (isSpace)) $ lines x




retreiveDDL :: Options -> IO ()
retreiveDDL opts = do
  let (user, password, db) = decodeConnectString $ o_conn opts
  withSession (connect user password db) $ do
    
    schema' <- getSchema $ o_schema opts
    let opts' = opts {o_schema = Just schema'}
    
    retrieveViewsDDL opts'
    retrieveSourcesDDL opts'
    retrieveTriggersDDL opts'
    retrieveSynonymsDDL opts'
    retrieveSequencesDDL opts'
    retrieveTablesDDL opts'


write2File :: FilePath -> FilePath -> FilePath -> String -> IO ()
write2File directory name_base name_suffix content = do
  withFile (directory </> (flip addExtension "sql" . flip addExtension name_suffix $ name_base)) WriteMode $ \h -> do
    hPutStr h content

retrieveViewsDDL opts = do
  let
    schema = fromJust $ o_schema opts
  
  let
    queryIteratee :: (Monad m) => String -> String -> Maybe String -> IterAct m [(String, String, Maybe String)]
    queryIteratee a b c accum = result' ((a, b, c):accum)
    sql' = printf  "select a.view_name, a.text, b.comments \n\
                   \  from sys.all_views a,                \n\
                   \       sys.all_tab_comments b          \n\
                   \ where a.owner='%s'                    \n\
                   \   and a.owner = b.owner(+)            \n\
                   \   and a.view_name=b.table_name(+)     \n" schema
           ++
           if isNothing $ o_obj_list opts
           then ""
           else printf " and a.view_name in (%s) " $ getUnionAll $ fromJust $ o_obj_list opts
  
  r <- reverse `liftM` doQuery (sql sql') queryIteratee []
  
  forM_ r $ \(view_name, text, comments) -> do
    let
      create :: String =
        printf "CREATE OR REPLACE VIEW %s\nAS\n%s\n/\n" (getSafeName view_name) $ chomp . clear0 $ text
      comment :: String =
        case comments of
          Just c  -> printf "\nCOMMENT ON TABLE %s IS '%s'\n/\n" (getSafeName view_name) $ chomp . clear0 $ c
          Nothing -> ""
    let
      qryItr :: (Monad m) => String -> Maybe String -> IterAct m [(String, Maybe String)]
      qryItr a b accum = result' ((a,b):accum)
      sql' :: String =
        printf "select column_name, comments \n\
               \  from sys.all_col_comments  \n\
               \ where owner='%s'            \n\
               \   and table_name='%s'       \n\
               \ order by column_name        \n" schema view_name
    r <- (filter (\(_, x) -> isJust x) . reverse) `liftM` doQuery (sql sql') qryItr []
    let column_comments :: String = concat $ flip map r $ \(column_name, comments) -> do
          printf "\nCOMMENT ON COLUMN %s.%s IS '%s'\n/\n" (getSafeName view_name) (getSafeName column_name) (clear0 $ fromJust comments) :: String
    
    liftIO $ write2File (o_output_dir opts) view_name "vew" $ create ++ comment ++ column_comments


retrieveSourcesDDL opts = do
  return ()

retrieveTriggersDDL opts = do
  return ()

retrieveSynonymsDDL opts = do
  return ()

retrieveSequencesDDL opts = do
  return ()

retrieveTablesDDL opts = do
  return ()

