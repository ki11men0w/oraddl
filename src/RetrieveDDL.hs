{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}

module RetrieveDDL where

import System.IO
import System.FilePath ((</>), addExtension, isValid)
import Data.Maybe (fromJust, isNothing, isJust, mapMaybe, fromMaybe)
import Data.List (dropWhileEnd, intercalate, isPrefixOf, sortBy)
import Data.Char (toUpper, toLower, ord)

import Text.Printf (printf)
import Control.Monad (when, unless, liftM, forM)
import Control.Monad.Trans (liftIO)
import Database.Oracle.Enumerator
import Data.Typeable (Typeable)

import Data.Map (Map)
import qualified Data.Map as Map

import Text.Parsec

--import Text.Regex.TDFA

import qualified Data.Map as M

import OracleUtils
import Utils ( clearSqlSource,
               stringCSI,
               printWarning,
               isSpace,
               strip,
               parseMatch
             )

data ByTypeLists = ByTypeLists {
    oTables :: Maybe [String],
    oViews :: Maybe [String],
    oSources :: Maybe [String],
    oSequences :: Maybe [String],
    oSynonyms :: Maybe [String],
    oTriggers :: Maybe [String]
    }

isAtLeastOneTypeSpecified :: ByTypeLists -> Bool
isAtLeastOneTypeSpecified x =
  (isJust . oTables) x || 
  (isJust . oViews) x || 
  (isJust . oSources) x || 
  (isJust . oSequences) x || 
  (isJust . oSynonyms) x || 
  (isJust . oTriggers) x

data Options = Options
               {
                 oConn :: String,
                 oSchema :: Maybe String,
                 oObjList :: Maybe [String],
                 oByTypeLists :: ByTypeLists,
                 oOutputDir :: String,
                 oSaveEndSpaces :: Bool
               }

getDefaultSchema = do
  let
    iter (a::String) accum = result' $ if True then a else accum
  doQuery (sql "select user from dual") iter []
  
getSchema schema =
  case schema of
    Just s -> return s
    _      -> getDefaultSchema
  

getSafeName' :: String -> String -> String
getSafeName' simpleSymbols ora_name =
  if all (`elem` simpleSymbols) stripped
  then stripped
  else "\"" ++ stripped ++ "\""
  
  where
    stripped =
      if length ora_name > 1 && head ora_name == '"' && last ora_name == '"'
      then init . tail $ ora_name
      else ora_name
          
getSafeName :: String -> String
getSafeName = getSafeName' "1234567890QWERTYUIOPASDFGHJKLZXCVBNM_#$"

getSafeName2 :: String -> String
getSafeName2 = getSafeName' "1234567890QWERTYUIOPASDFGHJKLZXCVBNM_#$."


newtype OracleName = ON String

getNormalizedOracleName :: String -> String
getNormalizedOracleName [] = []
getNormalizedOracleName n =
  if head sn /= '"' then map toUpper sn else sn
    where
      sn = getSafeName n
    

instance Eq OracleName where
  ON n1 == ON n2 = getNormalizedOracleName n1 == getNormalizedOracleName n2

instance Ord OracleName where
  compare (ON n1@[]) (ON n2) = compare n1 n2
  compare (ON n1) (ON n2@[]) = compare n1 n2
  compare (ON n1) (ON n2) =
    compare (getNormalizedOracleName n1) (getNormalizedOracleName n2)


getUnionAll :: [String] -> String
getUnionAll lst =
  case lst of
    [] -> "select 'x' from dual where 1 = 2"
    [x] -> select x
    x1:xs@(x2:_) -> select x1 ++ " union all " ++ getUnionAll xs
  where
    select = printf "select '%s' from dual"


sqlStringLiteral :: String -> String
sqlStringLiteral input =
  '\'' : concatMap doublefyQuoteSign input ++ "'"
  where
    doublefyQuoteSign '\'' = "\'\'"
    doublefyQuoteSign c    = [c]


retreiveDDL :: Options -> IO ()
retreiveDDL opts = do
  let (user, password, db) = decodeConnectString $ oConn opts
  withSession (connect user password db) $ do
    
    schema' <- getSchema $ oSchema opts
    let opts' = opts {oSchema = Just schema'}
    
    -- liftIO $ putStr "Yoo! " >> hFlush stdout >> getLine

    retrieveViewsDDL opts'
    retrieveSourcesDDL opts'
    retrieveTriggersDDL opts'
    retrieveSynonymsDDL opts'
    retrieveSequencesDDL opts'
    retrieveTablesDDL opts'

normalizeFileName :: FilePath -> FilePath
normalizeFileName name =
  concat $ flip map name $ \n ->
    case n of
      _
        | n `elem` canNotBeInFileName || (not . isValid) [n] -> printf "%%%X" $ ord n
        | otherwise -> [n]
  where
    canNotBeInFileName = "/\\"

write2File :: FilePath -> FilePath -> FilePath -> String -> IO ()
write2File directory name_base name_suffix content =
  withFile (directory </> (flip addExtension "sql" . flip addExtension name_suffix $ normalizeFileName name_base)) WriteMode $ \h ->
    hPutStr h content

data WhatToRetrieve = None | All | JustList [String]
getObjectList :: Options -> (ByTypeLists -> Maybe [String]) -> WhatToRetrieve
getObjectList opts f =
  case f $ oByTypeLists opts of
    Just x ->
      case x of
        [] -> All
        _  -> JustList x
    Nothing ->
      case oObjList opts of
        Just x ->
          case x of
            [] -> All
            _  -> JustList x
        Nothing ->
          if isAtLeastOneTypeSpecified $ oByTypeLists opts
          then None -- Задан список для какогото другого типа, а для нашего нет. Поэтому считаем что наш тип не нужен.
          else All  -- Никакие списки не заданы - считаем что надо получить всё.
      

retrieveViewsDDL opts = do
  let
    schema = fromJust $ oSchema opts
    what2Retrieve = getObjectList opts oViews

  withPreparedStatement 
    (prepareQuery . sql $
                    "select column_name, comments \n\
                    \  from sys.all_col_comments  \n\
                    \ where owner = ?             \n\
                    \   and table_name = ?        \n\
                    \ order by column_name        \n") $ \stm_comments -> do
  let
    iter (a::String) (b::String) (c::Maybe String) accum = saveOneFile (oOutputDir opts) schema (a,b,c) >> result' accum
    stm = sqlbind
          (
           "select a.view_name, a.text, b.comments \n\
           \  from sys.all_views a,                \n\
           \       sys.all_tab_comments b          \n\
           \ where a.owner = ?                     \n\
           \   and a.owner = b.owner(+)            \n\
           \   and a.view_name=b.table_name(+)     \n"
           ++
           case what2Retrieve of
             JustList lst ->
               printf "   and a.view_name in (%s) \n" $ getUnionAll lst
             _ -> ""
          )
          [bindP schema]
  
    saveOneFile outputDir schema viewInfo@(view_name, _, _) = do
      decl <- getViewDecl schema viewInfo
      liftIO $ write2File (oOutputDir opts) view_name "vew" decl
      return ()
      where
        getViewDecl schema (view_name, text, comments) = do
          let
            create :: String =
              printf "create or replace view %s\nas\n%s\n/\n" (getSafeName view_name) $ clearSqlSource text
            comment :: String =
              case comments of
                Just c  -> printf "\ncomment on table %s\n  is %s\n/\n" (getSafeName view_name) $ sqlStringLiteral . clearSqlSource $ c
                Nothing -> ""
        
            iter (a::String) (b::Maybe String) accum = result' ((a,b):accum)

          r <- withBoundStatement stm_comments [bindP schema, bindP view_name] $ \bstm ->
                 (filter (\(_, x) -> isJust x) . reverse) `liftM` doQuery bstm iter []
          let column_comments :: String = concat $ flip map r $ \(column_name, comments) ->
                printf "\ncomment on column %s.%s\n  is %s\n/\n" (getSafeName view_name) (getSafeName column_name) (sqlStringLiteral . clearSqlSource $ fromJust comments) :: String
        
          return $ create ++ comment ++ column_comments

  case what2Retrieve of
    None -> return ()
    _    -> doQuery stm iter ()


retrieveSourcesDDL opts = do
  let
    schema = fromJust $ oSchema opts
    what2Retrieve = getObjectList opts oSources
  
  let
    iter (name::String) (type'::String) (text::String) accum@(dataFound::Bool, prevName::String, prevType'::String, collectedText::[String]) =
            if name == prevName && type' == prevType'
            then result' (True, name, type', collectedText ++ [text])
            else
              do unless (null prevName) $ saveOneFile prevName prevType' collectedText
                 result' (True, name, type', [text])

    stm = sqlbind
          (
           "select name, type, text                                          \n\
           \  from sys.all_source                                            \n\
           \ where owner = ?                                                 \n\ 
           \   and type in ('PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION', \n\
           \                'TYPE', 'TYPE BODY', 'JAVA SOURCE')              \n"
           ++
           case what2Retrieve of
             JustList lst ->
               printf "   and name in (%s) \n" $ getUnionAll lst
             _ -> ""
           ++
           " order by owner,type,name,line "
          )
          [bindP schema]


  case what2Retrieve of
    None -> return ()
    _ -> do
      (dataFound,n,t,tx) <- doQuery stm iter (False,"","",[""])
      when dataFound $
         -- Сохраним последний найденный объект
         saveOneFile n t tx

  return ()

   where
    saveOneFile name type' textLines = do
      let
        text = foldr (++) "" textLines
      
        safe_name = getSafeName name
  
        saveSQL suffix = do
          let
            isNameCaseSensitive = safe_name /= name
            text' =  parse (do 
                                 spaces
                                 t'  <- stringCSI type'
                                 spaces
            
                                 n' <- try $ do string "\""
                                                n <- stringCSI name
                                                try $ string "\""
                                                return n
                                             <|> stringCSI name
                                 x <- many anyChar
                                 let nameMismatchInCode = not $ if isNameCaseSensitive then n' == name else map toUpper n' == map toUpper name
                                 return $ 
                                   t' ++ " " ++
                                   case () of
                                          _
                                            | isNameCaseSensitive -> "\"" ++ name ++ "\""
                                            | nameMismatchInCode  -> name
                                            | otherwise           -> n'
                                   ++ x
                           ) safe_name text
  
          text'' <- case text' of
                      Right x -> return x
                      Left e -> do liftIO . printWarning $ show e
                                   return text
          let text''' = printf "create or replace\n%s\n/\n" $ clearSqlSource text''
          liftIO $ write2File (oOutputDir opts) name suffix text'''
        saveJava = do
          let text''' = printf "create or replace and compile java source named %s as\n%s" safe_name text
          liftIO $ withFile (oOutputDir opts </> addExtension (normalizeFileName name) "java") WriteMode $ \h -> hPutStr h text'''
      case type' of
        "PACKAGE" -> saveSQL "pkg"
        "PACKAGE BODY" -> saveSQL "pkb"
        "FUNCTION" -> saveSQL "fun"
        "PROCEDURE" -> saveSQL "prc"
        "TYPE" -> saveSQL "typ"
        "TYPE BODY" -> saveSQL "tyb"
        "JAVA SOURCE" -> saveJava



retrieveTriggersDDL opts = do
  let
    schema = fromJust $ oSchema opts
    what2Retrieve = getObjectList opts oTriggers
  
  let
    iter (a::String) (b::String) (c::String) (d::String) (e::String) accum = saveOneFile (a,b,c,d,e) >> result' accum
    stm = sqlbind (
                   "select owner,trigger_name,description,trigger_body,status        \n\
                   \  from sys.all_triggers                                          \n\
                   \ where owner = ?                                                 \n"
                   ++
                   case what2Retrieve of
                     JustList lst ->
                       printf "   and trigger_name in (%s) \n" $ getUnionAll lst
                     _ -> ""
                  )
                  [bindP schema]

  case what2Retrieve of
    None -> return ()
    _    -> doQuery stm iter ()
  
  where
    saveOneFile (owner, name, descr, body, status) = do
      let
        safe_name = getSafeName name
        isNameCaseSensitive = safe_name /= name
        descr' = parse (do
                         let quotedName = do 
                               string "\""
                               n <- stringCSI name
                               string "\""
                               return n
  
                             schemaName = do
                               sch <- try $ do string "\"" 
                                               sch <- stringCSI owner
                                               string "\""
                                               return sch
                                      <|>
                                      stringCSI owner
  
                               spaces >> string "." >> spaces
                               return sch
  
                         spaces
                         
                         n' <- try (stringCSI name)
                               <|>
                               try quotedName
                               <|>
                               (schemaName >> (try (stringCSI name) <|> quotedName))
  
                         x <- many anyChar
                         let nameMismatchInCode = not $ if isNameCaseSensitive then n' == name else map toUpper n' == map toUpper name
                         return $ 
                           case () of
                                  _
                                    | isNameCaseSensitive -> "\"" ++ name ++ "\""
                                    | nameMismatchInCode  -> name
                                    | otherwise           -> n'
                           ++
                           -- Если после имени тригера нет пробела, то добавляем его
                           case x of
                             x1:_ -> if isSpace x1 then x else ' ' : x
                             _    -> x
                       ) safe_name descr
      descr'' <- case descr' of
                  Right x -> return x
                  Left e -> do liftIO . printWarning $ show e
                               return descr
      let
        text' = printf "create or replace trigger %s\n%s\n/\n" (clearSqlSource descr'') (clearSqlSource body)
        text = if status == "disabled"
               then text' ++ printf "\nalter trigger %s disable\n/\n" safe_name
               else text'
      
      liftIO $ write2File (oOutputDir opts) name "trg" text


retrieveSynonymsDDL opts = do
  let
    schema = fromJust $ oSchema opts
    what2Retrieve = getObjectList opts oSynonyms
  
  let
    iter (a::Bool) accum = result' $ True || accum
    -- Определяем есть ли колонка DB_LINK
    sql' = "select 'True'                    \n\
           \  from sys.all_tab_columns       \n\
           \ where table_name='ALL_SYNONYMS' \n\
           \   and COLUMN_NAME = 'DB_LINK'   \n\
           \   and rownum = 1                "

  dbLinkColumnExists <- doQuery (sql sql') iter False

  let
    iter (a::String) (b::String) (c::Maybe String) (d::String) (e::Maybe String) accum = saveOneFile (a,b,c,d,e) >> result' accum
    sql'  = "select owner            \n\
            \      ,synonym_name     \n\
            \      ,table_owner      \n\
            \      ,table_name       \n\
            \      ,%s as db_link    \n\
            \  from sys.all_synonyms \n\
            \ where owner = ?        \n"
            ++
            case what2Retrieve of
              JustList lst ->
                printf "   and synonym_name in (%s) \n" $ getUnionAll lst
              _ -> ""

    stm = sqlbind (printf sql' $ if dbLinkColumnExists then "db_link" else "null") [bindP schema]

  case what2Retrieve of
    None -> return ()
    _    -> doQuery stm iter ()

    where
      saveOneFile (owner, synonym_name, table_owner, table_name, db_link) = do
        let
          safe_name = getSafeName synonym_name
          isNameCaseSensitive = safe_name /= synonym_name
    
          text :: String = case table_owner of
            Nothing ->
               printf "create synonym %s\n\
                      \           for %s@%s\n/\n" safe_name (getSafeName table_name) (getSafeName2 $ fromMaybe "" db_link)
            Just table_owner'
             | getSafeName table_owner'  == getSafeName owner ->
               printf "create synonym %s\n\
                      \           for %s\n/\n" safe_name (getSafeName table_name) 
             | otherwise ->
               printf "create synonym %s\n\
                      \           for %s.%s\n/\n" safe_name table_owner' (getSafeName table_name) 
    
        liftIO $ write2File (oOutputDir opts) synonym_name "syn" text


retrieveSequencesDDL opts = do
  let
    schema = fromJust $ oSchema opts
    what2Retrieve = getObjectList opts oSequences
  
  let
    iter (a1::String) (a2::String) (a3::String) (a4::String) (a5::String) (a6::Integer) (a7::String) (a8::String) accum =
      saveOneFile (a1, a2, a3, a4, a5, a6, a7, a8) >> result' accum
    
    stm  = sqlbind
           (
            "select sequence_name                         \n\
            \      ,to_char(increment_by) as increment_by \n\
            \      ,to_char(min_value) as min_value       \n\
            \      ,to_char(max_value) as max_value       \n\
            \      ,to_char(cache_size) as cache_size     \n\
            \      ,cache_size as cache_size_x            \n\
            \      ,cycle_flag                            \n\
            \      ,order_flag                            \n\
            \  from sys.all_sequences                     \n\
            \ where sequence_owner = ?                    \n"
            ++
            case what2Retrieve of
              JustList lst ->
                printf "   and sequence_name in (%s) \n" $ getUnionAll lst
              _ -> ""
           )
           [bindP schema]


  case what2Retrieve of
    None -> return ()
    _    -> doQuery stm iter ()

    where
      saveOneFile (sequence_name, increment_by, min_value, max_value, cache_size, cache_size_x, cycle_flag, order_flag) = do
        let
          decl = printf "\
                        \create sequence %s\n\
                        \  increment by %s\n\
                        \  minvalue %s\n\
                        \  maxvalue %s\n" (getSafeName sequence_name) increment_by min_value max_value
                 ++
                 case cycle_flag of
                    "Y" -> "  cycle\n"
                    "N" -> "  nocycle\n"
                    _   -> ""
                 ++
                 case order_flag of
                    "Y" -> "  order\n"
                    "N" -> "  noorder\n"
                    _   -> ""
                 ++
                 (if cache_size_x <= 0
                  then "  nocache\n"
                  else printf "  cache %s\n" cache_size)
                 ++
                 "/\n"
                 
    
        liftIO $ write2File (oOutputDir opts) sequence_name "seq" decl
    

data OraTableConstraint = OraTableConstraint {
    constraint_name :: String,
    constraint_type :: String,
    search_condition :: Maybe String,
    r_owner :: Maybe String,
    r_constraint_name :: Maybe String,
    delete_rule :: Maybe String,
    status :: String,
    deferrable :: String,
    deferred :: String
  } deriving Typeable

retrieveTablesDDL opts = do
  let
    schema = fromJust $ oSchema opts
    what2Retrieve = getObjectList opts oTables
  

  withPreparedStatement
    (prepareQuery . sql $
                    "select column_name,        \n\
                    \       data_type,          \n\
                    \       data_length,        \n\
                    \       data_precision,     \n\
                    \       data_scale,         \n\
                    \       nullable,           \n\
                    \       data_default        \n\
                    \  from sys.all_tab_columns \n\
                    \ where owner = ?           \n\
                    \   and table_name = ?      \n\
                    \order by column_id         \n") $ \qryColumnDecl -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select column_name, comments \n\
                    \  from sys.all_col_comments  \n\
                    \ where owner = ?             \n\
                    \   and table_name = ?        \n\
                    \order by column_name         ") $ \qryColumnComments -> do
  withPreparedStatement                     
    (prepareQuery . sql $
                    "select column_name            \n\
                    \  from sys.all_cons_columns   \n\
                    \ where owner = ?              \n\
                    \   and table_name = ?         \n\
                    \   and constraint_name = ?    \n\
                    \order by position             ") $ \qryConstraintColumns1 -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select column_name,           \n\
                    \       table_name             \n\
                    \  from sys.all_cons_columns   \n\
                    \ where owner = ?              \n\
                    \   and constraint_name = ?    \n\
                    \order by position             ") $ \qryConstraintColumns2 -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select constraint_name,                                            \n\
                    \       constraint_type,                                            \n\
                    \       search_condition,                                           \n\
                    \       r_owner,                                                    \n\
                    \       r_constraint_name,                                          \n\
                    \       delete_rule,                                                \n\
                    \       status,                                                     \n\
                    \       deferrable,                                                 \n\
                    \       deferred                                                    \n\
                    \  from sys.all_constraints                                         \n\
                    \ where owner = ?                                                   \n\
                    \   and table_name = ?                                              \n\
                    \order by decode(constraint_type, 'P',1, 'R',2, 'U',3, 'C',4, 100), \n\
                    \         constraint_name") $ \qryConstraintDecl -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select index_name    \n\
                    \     , uniqueness    \n\
                    \     , table_owner   \n\
                    \     , partitioned   \n\
                    \from sys.all_indexes \n\
                    \where owner = ?      \n\
                    \and table_name = ?   \n\
                    \order by index_name  ") $ \qryIndexes -> do
  withPreparedStatement    
    (prepareQuery . sql $
                    "select column_name         \n\
                    \     , column_position     \n\
                    \     , descend             \n\
                    \  from sys.all_ind_columns \n\
                    \ where index_owner = ?     \n\
                    \   and index_name = ?      \n\
                    \order by column_position   ") $ \qryIndexColumns1 -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select column_name         \n\
                    \     , column_position     \n\
                    \     , '' as descend       \n\
                    \  from sys.all_ind_columns \n\
                    \ where index_owner = ?     \n\
                    \   and index_name = ?      \n\
                    \order by column_position   ") $ \qryIndexColumns2 -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select column_position         \n\
                    \     , column_expression       \n\
                    \  from sys.all_ind_expressions \n\
                    \ where index_owner = ?         \n\
                    \   and index_name = ?          ") $ \qryIndexColumnExpressions -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select partitioning_type        \n\
                    \     , subpartitioning_type     \n\
                    \  from sys.all_part_tables      \n\
                    \ where owner = ?                \n\
                    \   and table_name = ?           ") $ \qryPartTables -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select column_name              \n\
                    \  from sys.all_part_key_columns \n\
                    \ where owner = ?                \n\
                    \   and name = ?                 \n\
                    \order by column_position        ") $ \qryPartKeyColumns -> do
  withPreparedStatement
    (prepareQuery . sql $
                    "select column_name                 \n\
                    \  from sys.all_subpart_key_columns \n\
                    \ where owner = ?                   \n\
                    \   and name = ?                    \n\
                    \order by column_position           ") $ \qrySubPartKeyColumns -> do
  let
    stm  = sqlbind
           (
            "select a.table_name                 \n\
            \     , b.comments                   \n\
            \     , a.temporary                  \n\
            \     , a.duration                   \n\
            \     , a.row_movement               \n\
            \     , a.cache                      \n\
            \     , a.iot_type                   \n\
            \  from sys.all_tables a,            \n\
            \       sys.all_tab_comments b       \n\
            \ where a.owner = ?                  \n\
            \   and a.owner = b.owner(+)         \n\
            \   and a.table_name=b.table_name(+) \n"
            ++
            case what2Retrieve of
              JustList lst ->
                printf "   and a.table_name in (%s) \n" $ getUnionAll lst
              _ -> ""
           )
           [bindP schema]
   
    iter (a1::String) (a2::Maybe String) (a3::String) (a4::Maybe String) (a5::String) (a6::String) (a7::Maybe String) accum = saveOneFile a1 a2 a3 a4 a5 a6 a7 >> result' accum

    -- Сохраняет одну таблицу
    saveOneFile table_name comments temporary duration row_movement cache iot_type = do

      -- Соединяем все вместе и получаем декларацию всей таблицы
      (columns_decl :: Maybe String, columns_order :: [String]) <- getDeclColumns schema table_name
      partitions_decl <- getPartitionsDecl schema table_name
      columns_comment_decl :: Maybe String <- getDeclColumnsComment schema table_name columns_order
      (constraints_decl, constraints) :: (Maybe String, M.Map String ())<- getDeclConstraints schema table_name
      indexes_decl :: Maybe String <- getDeclIndexes schema table_name constraints 
      iot_decl :: Maybe String <- getIOTDecl iot_type
      let
        table_spec = if temporary == "Y" then "global temporary " else ""
        temporary_decl = 
          case temporary of
            "Y" -> case duration of
                     Just "SYS$TRANSACTION" -> Just "on commit delete rows"
                     Just "SYS$SESSION"     -> Just "on commit preserve rows"
                     _                      -> Nothing
            _   -> Nothing
  
        decl :: String =
          printf "\
                  \create %stable %s" table_spec (getSafeName table_name)
          ++
          maybe "" (\x-> "\n (\n" ++ x ++ "\n )") columns_decl
          ++
          maybe "" ("\n"++) iot_decl
          ++
          maybe "" ("\n"++) partitions_decl
          ++
          case cache of
            "Y" -> "\ncache"
            _ -> ""
          ++
          case row_movement of
            "ENABLED" -> "\nenable row movement"
            _ -> ""
          ++
          maybe "" ("\n"++) temporary_decl
          ++
          "\n/\n"
          ++
          maybe "" (printf "\ncomment on table %s\n  is %s\n/\n" (getSafeName table_name) . sqlStringLiteral) comments
          ++
          fromMaybe "" columns_comment_decl
          ++
          maybe "" ("\n"++) constraints_decl
          ++
          maybe "" ("\n"++) indexes_decl
                
      liftIO $ write2File (oOutputDir opts) table_name "tab" decl
  

    -- Получение деклараций колонок таблицы
    getDeclColumns schema table_name = do
          let
            -- Получение декларации для колонки таблицы
            getColumnDecl (column_name, data_type, data_length, data_precision, data_scale, nullable, data_default) =
                printf "  %s %s" (getSafeName column_name) data_type
                ++
                -- Обрабатываем тип даннх
                case data_type of
                  "CHAR"     -> printf "(%d)" data_length
                  "VARCHAR2" -> printf "(%d)" data_length
                  "NUMBER"   -> case data_precision of
                                  Nothing -> ""
                                  Just data_precision' -> "("
                                                          ++
                                                          show data_precision'
                                                          ++
                                                          case data_scale of
                                                            Nothing -> ""
                                                            Just 0 -> ""
                                                            Just data_scale' -> printf ",%d" data_scale'
                                                          ++
                                                          ")"
                  _ -> ""
                ++
                -- Значение поумолчанию
                case data_default of
                  Nothing -> ""
                  Just data_default' -> " default " ++ dropWhileEnd isSpace data_default'
                ++
                -- Nullable
                if nullable == "N" then " not null" else ""
  
          let
            iter (a1::String) (a2::String) (a3::Integer) (a4::Maybe Integer) (a5::Maybe Integer) (a6::String) (a7::Maybe String) accum = result' ((a1,a2,a3,a4,a5,a6,a7):accum)
      

          columns_r <- withBoundStatement qryColumnDecl [bindP schema, bindP table_name] $ \stm ->
                         reverse `liftM` doQuery stm iter []

          let
            column_order = map (\(column_name,_,_,_,_,_,_) -> column_name) columns_r
            decl = if null columns_r then Nothing
                   else Just $ intercalate ",\n" $ map getColumnDecl columns_r

          return $ (decl, column_order)


    -- Обрабатываем комментарии для колонок
    getDeclColumnsComment schema table_name columns_order = do
          let
              getColumnComment (column_name, comments) =
                case comments of
                  Just comments' -> Just $ printf "comment on column %s.%s\n  is %s\n/\n"
                                           (getSafeName table_name)
                                           (getSafeName column_name)
                                           (sqlStringLiteral comments')
                  Nothing -> Nothing
    
              iter (a1::String) (a2::Maybe String) accum = result' ((a1,a2):accum)
   
          columns_comments_accum <-
            withBoundStatement qryColumnComments [bindP schema, bindP table_name] $ \stm ->
              reverse `liftM` doQuery stm iter []

          let columns_order_m = Map.fromList $ zip (map ON columns_order) [1..]
          let numbered_comments = zip columns_comments_accum [1..]

              
          let x = concatMap ("\n" ++) $ mapMaybe (getColumnComment . fst) $
                  flip sortBy numbered_comments $ \((x1,_),n1) ((x2,_),n2) ->
                    let
                      value_x1 = Map.lookup (ON x1) columns_order_m
                      value_x2 = Map.lookup (ON x2) columns_order_m
                    in
                    case (value_x1, value_x2) of
                      (Just x1, Just x2) -> compare x1 x2
                      (Nothing, Nothing) -> compare n1 n2
                      (Nothing, _) -> GT
                      (_, Nothing) -> LT


          return $ if null x then Nothing else Just x


    -- Обрабатываем констрэйнты
    getDeclConstraints schema table_name = do
          let
            getConstraintDecl (OraTableConstraint constraint_name constraint_type search_condition r_owner r_constraint_name delete_rule status deferrable deferred) =
              case constraint_type of
                "P" -> -- Primary key
                       getConstraintDecl' "primary key"
                "R" -> -- Foreign key
                       getConstraintDecl' "foreign key"
                "U" -> -- Uniq key
                       getConstraintDecl' "unique"
                "C" -> case search_condition of
                         Nothing -> return Nothing
                         Just sc ->
                           -- if (map toLower sc) =~ "^[ \n\r\t\0]*[^ \n\r\t\0]+[ \n\r\t\0]+is[ \n\r\t\0]+not[ \n\r\t\0]+null[ \n\r\t\0]*$" :: Bool
                           if parseMatch
                                (map toLower sc)
                                (manyTill anyChar $
                                          try (do let spaces1 = skipMany1 space
                                                  space >> string "is" >> spaces1 >> string "not" >> spaces1 >> string "null" >> spaces >> eof))
                           then return Nothing -- это просто условие not null
                           else getConstraintDecl' $ "check (" ++ sc ++ ")"
                "V" -> return Nothing
                _   -> return Nothing
                
              
              where

                getConstraintDecl' ctype = do
                  columns_decl :: Maybe String <- getDeclConstraintColumns
                  tail_decl :: String <- getDeclConstraintTail

                  return . Just $ getCommonConstraintHeadDecl ctype ++ maybe "" (" " ++) columns_decl ++ tail_decl

                  where
                    getCommonConstraintHeadDecl =
                        printf "alter table %s\n  add%s %s"
                               (getSafeName table_name)
                               (if "SYS_" `isPrefixOf` map toUpper constraint_name
                                then ""
                                else " constraint " ++ getSafeName constraint_name)

                -- Формируем декларацию списка полей включённых в констрэйнт
                getDeclConstraintColumns = do
                  let
                    iter (a1::String) accum = result' (a1:accum)
                  
                  r <- 
                    withBoundStatement qryConstraintColumns1 [bindP schema, bindP table_name, bindP constraint_name] $ \stm ->
                      reverse `liftM` doQuery stm iter []
                 
                  let
                    columns = filter (not . null) $ flip map r $ \column_name ->
                      case constraint_type of
                        "P" -> column_name
                        "R" -> column_name
                        "U" -> column_name
                        _ -> ""
                  
                  return $ case columns of
                             [] -> Nothing
                             _  -> Just $ printf "(%s)" $ intercalate "," columns
              
                -- Формируем заклччительную часть констрэйнта
                getDeclConstraintTail = do

                  part1 <- case constraint_type of
                    "R" -> do -- Foreign key
                      let
                        iter (a1::String) (a2::String) accum = result' ((a1,a2):accum)
    
                      r <-
                        withBoundStatement qryConstraintColumns2 [bindP $ fromJust r_owner, bindP $ fromJust r_constraint_name] $ \stm ->
                          reverse `liftM` doQuery stm iter []

                      let
                        r_table_name' = getSafeName . snd . head $ r
                        r_table_name =
                          case r_owner of
                            Nothing -> r_table_name'
                            Just r_owner'
                                 | r_owner' == schema -> r_table_name'
                                 -- Владелец таблицы на которую ссылаемся не совпадает с владельцем
                                 -- ключа, поэтому указываем схему владельца таблицы явно
                                 | otherwise -> getSafeName r_owner' ++ "." ++ r_table_name'

                      ref_part <-
                        case r of
                          [] -> do liftIO . printWarning $ "Error while processing constraint " ++ constraint_name
                                   return ""
                          _  -> do let x = intercalate "," $ map fst r
                                   return $ printf "\n      references %s(%s)" r_table_name x

                      let delete_part = case delete_rule of
                                          Just "CASCADE" -> "\n  on delete cascade"
                                          _ -> ""

                      return $ ref_part ++ delete_part

                    _ -> return ""

                  let deferred_part = if deferrable == "DEFERRABLE"
                                      then "\n  deferrable" ++
                                           (if deferred == "DEFERRED" then " initially deferred" else "")
                                      else ""

                  status_part <- case status of
                    "ENABLED" -> return ""
                    "DISABLED" -> return "\n  disable"
                    _ -> do let msg = printf "Unknown constraint status '%s' for constraint '%s'" status constraint_name
                            liftIO . printWarning $ msg
                            return $ "\n-- " ++ msg

                  return $ part1 ++ deferred_part ++ status_part

          let
            iter a1 a2 a3 a4 a5 a6 a7 a8 a9 accum = result' $ OraTableConstraint a1 a2 a3 a4 a5 a6 a7 a8 a9 : accum
    
          constraint_accum <-
            withBoundStatement qryConstraintDecl [bindP schema, bindP table_name] $ \stm ->
              reverse `liftM` doQuery stm iter []

          let constraints = M.fromList . flip map constraint_accum $ \x -> (constraint_name x, ())

          x <- liftM (intercalate "\n" . map ((++"\n/\n") . fromJust) . filter isJust) $ mapM getConstraintDecl constraint_accum
          return (if null x then Nothing else Just x, constraints)
  
    -- Снимаем индексы
    getDeclIndexes schema table_name constraints = do
      let
        iter (a1::String) (a2::String) (a3::String) (a4::String) accum = result' ((a1,a2,a3,a4):accum)

      r <-
          withBoundStatement qryIndexes [bindP schema, bindP table_name] $ \stm ->
            (reverse .
             -- если это индекс для констрэйнта, то пропускаем его
             filter (\(index_name,_,_,_) -> not $ M.member index_name constraints))
            `liftM`
            doQuery stm iter []

      indexesDecl <- forM r $ \(index_name, uniqueness, table_owner, partitioned) -> do

        index_columns <- getDeclIndexColumns schema index_name
        let
          headerDecl = "create"
                       ++
                       (if uniqueness == "UNIQUE" then " unique" else "")
                       ++
                       " index " ++ getSafeName index_name
                       ++
                       (if table_owner == schema
                        then printf "\n on %s\n" (getSafeName table_name)
                        else printf "\n on %s.%s\n" (getSafeName table_owner) (getSafeName table_name))
                       ++ "  (\n" ++ (intercalate ",\n" . map ("   "++)) index_columns ++ "\n  )"
                       ++ (if partitioned == "YES" then "\n local" else "")

        return headerDecl

      if null indexesDecl
       then return Nothing
       else return . Just $ intercalate "\n/\n\n" indexesDecl ++ "\n/\n"

      where
        getDeclIndexColumns schema index_name = do
          let
            iter (a1::String) (a2::Integer) (a3::String) accum = result' ((a1,a2,a3):accum)
          
          column_index_expressions_map <- getIndexColumnExpressions
               
          r <- reverse `liftM`
               catchDB (withBoundStatement qryIndexColumns1 [bindP schema, bindP index_name] $ \stm -> doQuery stm iter [])
                       (\_ ->
                          withBoundStatement qryIndexColumns2 [bindP schema, bindP index_name] $ \stm -> doQuery stm iter [])

          return $ map (getDeclColumnIndex column_index_expressions_map) r


            where
              -- Табицы all_ind_expressions не было в Oracle 7, поэтому строим хэш column_expression
              -- по ключу номера колонки отдельно.
              getIndexColumnExpressions = do
                let
                  iter (cp::Integer) (ce::String) accum = result' $ M.insert cp ce accum

                withBoundStatement qryIndexColumnExpressions [bindP schema, bindP index_name] $ \stm ->
                  doQuery stm iter M.empty

              -- Возвращает декларацию колонки индекса
              getDeclColumnIndex column_index_expressions_map (column_name, column_position, descend) =
                strip $ M.findWithDefault column_name column_position column_index_expressions_map
                ++
                (if map toUpper descend == "DESC" then " desc" else "")

    getPartitionsDecl schema table_name = do
      let
        iter (partitioning_type :: String) (subpartitioning_type :: String) accum =
          result' $ if True then Just (partitioning_type, subpartitioning_type) else accum

      r <- withBoundStatement qryPartTables [bindP schema, bindP table_name] $ \stm ->
             doQuery stm iter Nothing

      case r of
        Nothing -> return Nothing
        Just (pt, st)  -> do
          part_key_columns_decl <- getPartKeyColumnsDecl
          subpart_key_columns_decl <- getSubPartKeyColumnsDecl

          return . Just $
            "/*\npartition by " ++ map toLower pt
            ++
            maybe "" (" "++) part_key_columns_decl
            ++
            case st of 
              "NONE" -> ""
              _      -> "\nsubpartition by " ++ map toLower st
                        ++
                        maybe "" (" "++) subpart_key_columns_decl
            ++ "\n*/" 

        where
          getPartKeyColumnsDecl = do
            let
              iter (column_name :: String) accum = result' $ column_name : accum

            r <- withBoundStatement qryPartKeyColumns [bindP schema, bindP table_name] $ \stm ->
                 (reverse . map getSafeName) `fmap` doQuery stm iter []
            
            return $ case r of
              [] -> Nothing
              _  -> Just $ "(" ++ intercalate "," r ++ ")"

          getSubPartKeyColumnsDecl = do
            let
              iter (column_name :: String) accum = result' $ column_name : accum

            r <- withBoundStatement qrySubPartKeyColumns [bindP schema, bindP table_name] $ \stm ->
                 (reverse . map getSafeName) `fmap` doQuery stm iter []
            
            return $ case r of
              [] -> Nothing
              _  -> Just $ "(" ++ intercalate "," r ++ ")"


    getIOTDecl iot_type = do
      return $ case iot_type of
        Just "IOT" -> Just "organization index"
        Just "IOT_OVERFLOW" -> Just "organization index"
        _     -> Nothing
        

  case what2Retrieve of
    None -> return ()
    _    -> doQuery stm iter ()

