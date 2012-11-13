{-# LANGUAGE ScopedTypeVariables #-}

module RetrieveDDL where

import System.IO
import System.FilePath
import Data.Maybe (fromJust, isNothing, isJust)
import Data.List
import Data.Char (toUpper)

import Text.Printf
import Control.Monad
import Control.Monad.Trans
import Database.Enumerator
import Database.Oracle.Enumerator

import Text.Parsec

import OracleUtils
import Utils ( clearSqlSource,
               stringCSI,
               printWarning,
               isSpace
             )

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


getUnionAll :: [String] -> String
getUnionAll lst =
  case lst of
    [] -> "select 'x' from dual where 1 = 2"
    [x] -> select x
    x1:xs@(x2:_) -> select x1 ++ " union all " ++ getUnionAll xs
  where
    select = printf "select '%s' from dual"




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
        printf "CREATE OR REPLACE VIEW %s\nAS\n%s\n/\n" (getSafeName view_name) $ clearSqlSource text
      comment :: String =
        case comments of
          Just c  -> printf "\nCOMMENT ON TABLE %s IS '%s'\n/\n" (getSafeName view_name) $ clearSqlSource c
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
          printf "\nCOMMENT ON COLUMN %s.%s IS '%s'\n/\n" (getSafeName view_name) (getSafeName column_name) (clearSqlSource $ fromJust comments) :: String
    
    liftIO $ write2File (o_output_dir opts) view_name "vew" $ create ++ comment ++ column_comments


retrieveSourcesDDL opts = do
  let
    schema = fromJust $ o_schema opts
  
  let
    queryIteratee :: (Monad m) => String -> String -> String -> IterAct m [(String, String, String)]
    queryIteratee a b c accum = result' ((a, b, c):accum)
    sql' = printf "select name, type, text                                          \n\
                  \  from sys.all_source                                            \n\
                  \ where owner='%s'                                                \n\ 
                  \   and type in ('PACKAGE','PACKAGE BODY','PROCEDURE','FUNCTION', \n\
                  \                'TYPE', 'TYPE BODY', 'JAVA SOURCE')              \n" schema
           ++
           (if isNothing $ o_obj_list opts
            then ""
            else printf " and name in (%s) \n" $ getUnionAll $ fromJust $ o_obj_list opts)
           ++
           " order by owner,type,name,line "

  r <- ((groupBy (\(n1,t1,_) (n2,t2,_) -> n1 == n2 && t1 == t2)) . reverse)
       `liftM`
       doQuery (sql sql') queryIteratee []

  forM_ r $ \x -> do
    let
      (name, type', _) = head x
      text :: String
      text = foldr (++) "" $ map (\(_,_,text) -> text) x
    
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
        let text''' = printf "CREATE OR REPLACE\n%s\n/\n" $ clearSqlSource text''
        liftIO $ write2File (o_output_dir opts) name suffix text'''
      saveJava = do
        let text''' = printf "CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED %s AS\n%s" safe_name text
        liftIO $ withFile (o_output_dir opts </> (addExtension name "java")) WriteMode $ \h -> hPutStr h text'''
    case type' of
      "PACKAGE" -> saveSQL "pkg"
      "PACKAGE BODY" -> saveSQL "pkb"
      "FUNCTION" -> saveSQL "fun"
      "PROCEDURE" -> saveSQL "prc"
      "TYPE" -> saveSQL "typ"
      "TYPE BODY" -> saveSQL "tyb"
      "JAVA SOURCE" -> saveJava

  return ()


retrieveTriggersDDL opts = do
  let
    schema = fromJust $ o_schema opts
  
  let
    queryIteratee :: (Monad m) => String -> String -> String -> String -> String -> IterAct m [(String, String, String, String, String)]
    queryIteratee a b c d e accum = result' ((a, b, c, d, e):accum)
    sql' = printf "select owner,trigger_name,description,trigger_body,status        \n\
                  \  from sys.all_triggers                                          \n\
                  \ where owner='%s'                                                \n" schema
           ++
           (if isNothing $ o_obj_list opts
            then ""
            else printf " and name in (%s) \n" $ getUnionAll $ fromJust $ o_obj_list opts)

  r <- reverse `liftM` doQuery (sql sql') queryIteratee []
  
  forM_ r $ \(owner, name, descr, body, status) -> do
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
                           x1:_ -> if isSpace x1 then x else " " ++ x
                           _    -> x
                     ) safe_name descr
    descr'' <- case descr' of
                Right x -> return x
                Left e -> do liftIO . printWarning $ show e
                             return descr
    let
      text' = printf "CREATE OR REPLACE TRIGGER %s\n%s\n/\n" (clearSqlSource descr'') (clearSqlSource body)
      text = if status == "DISABLED"
             then text' ++ printf "\nALTER TRIGGER %s DISABLE\n/\n" safe_name
             else text'
    
    liftIO $ write2File (o_output_dir opts) name "trg" text

  return ()

retrieveSynonymsDDL opts = do
  let
    schema = fromJust $ o_schema opts
  
  let
    iter :: (Monad m) => Bool -> IterAct m Bool
    iter a accum = result' a
    -- Определяем есть ли колонка DB_LINK
    sql' = "select 'True'                    \n\
           \  from sys.all_tab_columns       \n\
           \ where table_name='ALL_SYNONYMS' \n\
           \   and COLUMN_NAME = 'DB_LINK'   \n\
           \   and rownum = 1                "

  dbLinkColumnExists <- doQuery (sql sql') iter False

  let
    iter :: (Monad m) => String -> String -> Maybe String -> String -> Maybe String -> IterAct m [(String, String, Maybe String, String, Maybe String)]
    iter a b c d e accum = result' ((a, b, c, d, e):accum)
    sql'' = "select owner            \n\
            \      ,synonym_name     \n\
            \      ,table_owner      \n\
            \      ,table_name       \n\
            \      ,%s as db_link    \n\
            \  from sys.all_synonyms \n\
            \ where owner='%s'       \n"
            ++
           (if isNothing $ o_obj_list opts
            then ""
            else printf " and synonym_name in (%s) \n" $ getUnionAll $ fromJust $ o_obj_list opts)

    sql' = printf sql''
            (if dbLinkColumnExists then "db_link" else "null")
            schema

  r <- reverse `liftM` doQuery (sql sql') iter []

  forM_ r $ \(owner, synonym_name, table_owner, table_name, db_link) -> do
    let
      safe_name = getSafeName synonym_name
      isNameCaseSensitive = safe_name /= synonym_name

      text :: String = case table_owner of
        Nothing ->
           printf "CREATE SYNONYM %s\n\
                  \           FOR %s@%s\n/\n" safe_name (getSafeName table_name) (getSafeName2 $ maybe "" id db_link)
        Just table_owner'
         | getSafeName table_owner'  == getSafeName schema ->
           printf "CREATE SYNONYM %s\n\
                  \           FOR %s\n/\n" safe_name (getSafeName table_name) 
         | otherwise ->
           printf "CREATE SYNONYM %s\n\
                  \           FOR %s.%s\n/\n" safe_name table_owner' (getSafeName table_name) 

    liftIO $ write2File (o_output_dir opts) synonym_name "syn" text


retrieveSequencesDDL opts = do
  let
    schema = fromJust $ o_schema opts
  
  let
    iter :: (Monad m) => String -> String -> String -> String -> String -> Integer -> String -> String
            -> IterAct m[(String, String, String, String, String, Integer, String, String)]
    iter a1 a2 a3 a4 a5 a6 a7 a8 accum = result' ((a1, a2, a3, a4, a5, a6, a7, a8):accum)
    
    sql' = printf
           "select sequence_name                         \n\
           \      ,to_char(increment_by) as increment_by \n\
           \      ,to_char(min_value) as min_value       \n\
           \      ,to_char(max_value) as max_value       \n\
           \      ,to_char(cache_size) as cache_size     \n\
           \      ,cache_size as cache_size_x            \n\
           \      ,cycle_flag                            \n\
           \      ,order_flag                            \n\
           \  from sys.all_sequences                     \n\
           \ where sequence_owner='%s'                   \n"
           schema
           ++
           (if isNothing $ o_obj_list opts
            then ""
            else printf " and sequence_name in (%s) \n" $ getUnionAll $ fromJust $ o_obj_list opts)


  r <- reverse `liftM` doQuery (sql sql') iter []
  
  forM_ r $ \(sequence_name, increment_by, min_value, max_value, cache_size, cache_size_x, cycle_flag, order_flag) -> do
    let
      decl = printf "\
                    \CREATE SEQUENCE %s\n\
                    \  INCREMENT BY %s\n\
                    \  MINVALUE %s\n\
                    \  MAXVALUE %s\n" (getSafeName sequence_name) increment_by min_value max_value
             ++
             case cycle_flag of
                "Y" -> "  CYCLE\n"
                "N" -> "  NOCYCLE\n"
                _   -> ""
             ++
             case order_flag of
                "Y" -> "  ORDER\n"
                "N" -> "  NOORDER\n"
                _   -> ""
             ++
             (if cache_size_x <= 0
              then "  NOCACHE\n"
              else printf "  CACHE %s\n" cache_size)
             ++
             "/\n"
             

    liftIO $ write2File (o_output_dir opts) sequence_name "seq" decl
    

retrieveTablesDDL opts = do
  return ()

