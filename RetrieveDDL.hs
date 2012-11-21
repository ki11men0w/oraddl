{-# LANGUAGE ScopedTypeVariables #-}

module RetrieveDDL where

import System.IO
import System.FilePath
import Data.Maybe (fromJust, isNothing, isJust, catMaybes)
import Data.List
import Data.Char (toUpper, toLower)

import Text.Printf
import Control.Monad
import Control.Monad.Trans
import Database.Enumerator
import Database.Oracle.Enumerator

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
      it a accum = result a
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
    
    -- liftIO $ putStr "Yoo! " >> hFlush stdout >> getLine

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
    queryIteratee a b c accum = result ((a, b, c):accum)
    sql'  = printf
           "select a.view_name, a.text, b.comments \n\
           \  from sys.all_views a,                \n\
           \       sys.all_tab_comments b          \n\
           \ where a.owner = '%s'                  \n\
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
      qryItr a b accum = result ((a,b):accum)
      stm = sqlbind
            "select column_name, comments \n\
            \  from sys.all_col_comments  \n\
            \ where owner = ?             \n\
            \   and table_name = ?        \n\
            \ order by column_name        \n" [bindP schema, bindP view_name]
    r <- (filter (\(_, x) -> isJust x) . reverse) `liftM` doQuery stm qryItr []
    let column_comments :: String = concat $ flip map r $ \(column_name, comments) -> do
          printf "\nCOMMENT ON COLUMN %s.%s IS '%s'\n/\n" (getSafeName view_name) (getSafeName column_name) (clearSqlSource $ fromJust comments) :: String
    
    liftIO $ write2File (o_output_dir opts) view_name "vew" $ create ++ comment ++ column_comments


retrieveSourcesDDL opts = do
  let
    schema = fromJust $ o_schema opts
  
  let
    queryIteratee :: (Monad m) => String -> String -> String -> IterAct m [(String, String, String)]
    queryIteratee a b c accum = result ((a, b, c):accum)
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
    queryIteratee a b c d e accum = result ((a, b, c, d, e):accum)
    sql' = printf "select owner,trigger_name,description,trigger_body,status        \n\
                  \  from sys.all_triggers                                          \n\
                  \ where owner='%s'                                                \n" schema
           ++
           (if isNothing $ o_obj_list opts
            then ""
            else printf " and trigger_name in (%s) \n" $ getUnionAll $ fromJust $ o_obj_list opts)

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
    iter a accum = result a
    -- Определяем есть ли колонка DB_LINK
    sql' = "select 'True'                    \n\
           \  from sys.all_tab_columns       \n\
           \ where table_name='ALL_SYNONYMS' \n\
           \   and COLUMN_NAME = 'DB_LINK'   \n\
           \   and rownum = 1                "

  dbLinkColumnExists <- doQuery (sql sql') iter False

  let
    iter :: (Monad m) => String -> String -> Maybe String -> String -> Maybe String -> IterAct m [(String, String, Maybe String, String, Maybe String)]
    iter a b c d e accum = result ((a, b, c, d, e):accum)
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
    iter a1 a2 a3 a4 a5 a6 a7 a8 accum = result ((a1, a2, a3, a4, a5, a6, a7, a8):accum)
    
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
  let
    schema = fromJust $ o_schema opts
  
    sql' = printf
           "select a.table_name, b.comments, a.temporary, a.duration \n\
           \  from sys.all_tables a,                                 \n\
           \       sys.all_tab_comments b                            \n\
           \ where a.owner='%s'                                      \n\
           \   and b.owner='%s'                                      \n\
           \   and a.table_name=b.table_name \n"
           schema schema
           ++
           (if isNothing $ o_obj_list opts
            then ""
            else printf " and a.table_name in (%s) \n" $ getUnionAll $ fromJust $ o_obj_list opts)
   
    iter :: (Monad m) => String -> Maybe String -> String -> Maybe String -> IterAct m [(String, Maybe String, String, Maybe String)]
    iter a1 a2 a3 a4 accum = result $ (a1,a2,a3,a4):accum

  tables <- reverse `liftM` doQuery (sql sql') iter []

  -- Обход всех таблиц
  forM_ tables $ \(table_name, comments, temporary, duration) -> do

    -- Соединяем все вместе и получаем декларацию всей таблицы
    columns_decl :: String <- getDeclColumns schema table_name
    columns_comment_decl :: Maybe String <- getDeclColumnsComment schema table_name
    (constraints_decl, constraints) :: (Maybe String, M.Map String ())<- getDeclConstraints schema table_name
    indexes_decl :: Maybe String <- getDeclIndexes schema table_name constraints 
    let
      table_spec = if temporary == "Y" then "GLOBAL TEMPORARY " else ""
      temporary_decl = 
        case temporary of
          "Y" -> case duration of
                   Just "SYS$TRANSACTION" -> "\nON COMMIT DELETE ROWS"
                   Just "SYS$SESSION"     -> "\nON COMMIT PRESERVE ROWS"
                   _                 -> ""
          _ -> ""

      decl :: String =
        printf "\
                \CREATE %sTABLE %s\n\
                \ (\n\
                \%s\n\
                \ )%s\n\
                \/\n" table_spec (getSafeName table_name) columns_decl temporary_decl
        ++
        maybe "" (printf "\nCOMMENT ON TABLE %s IS '%s'\n/\n" (getSafeName table_name)) comments
        ++
        maybe "" id columns_comment_decl
        ++
        maybe "" ("\n"++) constraints_decl
        ++
        maybe "" ("\n"++) indexes_decl
              
    liftIO $ write2File (o_output_dir opts) table_name "tab" decl
  
  return ()

  where
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
                  Just data_default' -> " DEFAULT " ++ dropWhileEnd isSpace data_default'
                ++
                -- Nullable
                if nullable == "N" then " NOT NULL" else ""
  
  
          let sql_columns' :: String =
                printf "\
                        \select column_name,        \n\
                        \       data_type,          \n\
                        \       data_length,        \n\
                        \       data_precision,     \n\
                        \       data_scale,         \n\
                        \       nullable,           \n\
                        \       data_default        \n\
                        \  from sys.all_tab_columns \n\
                        \ where owner='%s'          \n\
                        \   and table_name='%s'     \n\
                        \order by column_id         \n" schema table_name
        
              iter :: (Monad m) => String -> String -> Integer -> Maybe Integer -> Maybe Integer -> String -> Maybe String
                      -> IterAct m [(String, String, Integer, Maybe Integer, Maybe Integer, String, Maybe String)]
              iter a1 a2 a3 a4 a5 a6 a7 accum = result ((a1,a2,a3,a4,a5,a6,a7):accum)
      
          columns_r <- reverse `liftM` doQuery (sql sql_columns') iter []
  
          return . concat . intersperse ",\n" $ map getColumnDecl columns_r


    -- Обрабатываем комментарии для колонок
    getDeclColumnsComment schema table_name = do
          let
              getColumnComment (column_name, comments) =
                case comments of
                  Just comments' -> Just $ printf "COMMENT ON COLUMN %s.%s IS '%s'\n/\n"
                                           (getSafeName table_name)
                                           (getSafeName column_name)
                                           comments'
                  Nothing -> Nothing
    
              sql' :: String = printf
                     "select column_name, comments \n\
                     \  from sys.all_col_comments  \n\
                     \ where owner='%s'            \n\
                     \   and table_name='%s'       \n\
                     \order by column_name         " schema table_name
              iter :: (Monad m) => String -> Maybe String -> IterAct m [(String, Maybe String)]
              iter a1 a2 accum = result ((a1,a2):accum)
   
          columns_comments_accum <- reverse `liftM` doQuery (sql sql') iter []
      
          let x = concat . map ("\n" ++) . catMaybes . map getColumnComment $ columns_comments_accum
          return $ if null x then Nothing else Just x


    -- Обрабатываем констрэйнты
    getDeclConstraints schema table_name = do
          let
            getConstraintDecl (constraint_name, constraint_type, search_condition, r_owner, r_constraint_name, delete_rule, status) = do
              case constraint_type of
                "P" -> -- Primary key
                       getConstraintDecl' "PRIMARY KEY"
                "R" -> -- Foreign key
                       getConstraintDecl' "FOREIGN KEY"
                "U" -> -- Uniq key
                       getConstraintDecl' "UNIQUE"
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
                           else getConstraintDecl' $ "CHECK (" ++ sc ++ ")"
                "V" -> return Nothing
                _   -> return Nothing
                
              
              where

                getConstraintDecl' ctype = do
                  columns_decl :: Maybe String <- getDeclConstraintColumns
                  tail_decl :: String <- getDeclConstraintTail

                  return . Just $ getCommonConstraintHeadDecl ctype ++ maybe "" (" " ++) columns_decl ++ tail_decl

                  where
                    getCommonConstraintHeadDecl type_decl =
                        printf "ALTER TABLE %s\n  ADD%s %s"
                               (getSafeName table_name)
                               (if "SYS_" `isPrefixOf` (map toUpper constraint_name)
                                then ""
                                else " CONSTRAINT " ++ getSafeName constraint_name)
                               type_decl

                -- Формируем декларацию списка полей включённых в констрэйнт
                getDeclConstraintColumns = do
                  let
                    sql' = printf 
                             "select column_name            \n\
                             \  from sys.all_cons_columns   \n\
                             \ where owner='%s'             \n\
                             \   and table_name='%s'        \n\
                             \   and constraint_name = '%s' \n\
                             \order by position             " schema table_name constraint_name
                    iter :: (Monad m) => String -> IterAct m [String]
                    iter a1 accum = result ((a1):accum)
                  
                  r <- reverse `liftM` doQuery (sql sql') iter []
                 
                  let
                    columns = filter (not . null) $ flip map r $ \column_name ->
                      case constraint_type of
                        "P" -> column_name
                        "R" -> column_name
                        "U" -> column_name
                        _ -> ""
                  
                  return $ case columns of
                             [] -> Nothing
                             _  -> Just $ printf "(%s)" $ concat . intersperse "," $ columns
              
                -- Формируем заклччительную часть констрэйнта
                getDeclConstraintTail = do

                  part1 <- case constraint_type of
                    "R" -> do -- Foreign key
                      let
                        stm = sqlbind
                               "select column_name,           \n\
                               \       table_name             \n\
                               \  from sys.all_cons_columns   \n\
                               \ where owner = ?              \n\
                               \   and constraint_name = ?    \n\
                               \order by position             " [bindP $ fromJust r_owner, bindP $ fromJust r_constraint_name]
                        iter :: (Monad m) => String -> String -> IterAct m [(String, String)]
                        iter a1 a2 accum = result ((a1,a2):accum)
    
                      r <- reverse `liftM` doQuery stm iter []

                      ref_part <-
                        case r of
                          [] -> do liftIO . printWarning $ "Error while processing constraint " ++ constraint_name
                                   return ""
                          _  -> do let x = concat . intersperse "," $ map (\(column_name, _) -> column_name) r
                                   return $ printf "\n      REFERENCES %s(%s)" (snd . head $ r) x

                      let delete_part = case delete_rule of
                                          Just "CASCADE" -> "\n  ON DELETE CASCADE"
                                          _ -> ""

                      return $ ref_part ++ delete_part

                    _ -> return ""

                  status_part <- case status of
                    "ENABLED" -> return ""
                    "DISABLED" -> return "\n  DISABLE"
                    _ -> do let msg = printf "Unknown constraint status '%s' for constraint '%s'" status constraint_name
                            liftIO . printWarning $ msg
                            return $ "\n-- " ++ msg

                  return $ part1 ++ status_part

          let
            sql' :: String = printf
                   "select constraint_name,                                            \n\
                   \       constraint_type,                                            \n\
                   \       search_condition,                                           \n\
                   \       r_owner,                                                    \n\
                   \       r_constraint_name,                                          \n\
                   \       delete_rule,                                                \n\
                   \       status                                                      \n\
                   \  from sys.all_constraints                                         \n\
                   \ where owner='%s'                                                  \n\
                   \   and table_name='%s'                                             \n\
                   \order by decode(constraint_type, 'P',1, 'R',2, 'U',3, 'C',4, 100), \n\
                   \         constraint_name" schema table_name
            iter :: (Monad m) => String -> String -> Maybe String -> Maybe String -> Maybe String -> Maybe String -> String
                    -> IterAct m [(String, String, Maybe String, Maybe String, Maybe String, Maybe String, String)]
            iter a1 a2 a3 a4 a5 a6 a7 accum = result ((a1,a2,a3,a4,a5,a6,a7):accum)
    
          constraint_accum <- reverse `liftM` doQuery (sql sql') iter []

          let constraints = M.fromList . flip map constraint_accum $ \(constraint_name,_,_,_,_,_,_) -> (constraint_name, ())

          x <- liftM (concat . intersperse "\n" . map (++"\n/\n") . map fromJust . filter isJust) $ mapM getConstraintDecl constraint_accum
          return $ (if null x then Nothing else Just x, constraints)
  
    -- Снимаем индексы
    getDeclIndexes schema table_name constraints = do
      let
        stm = sqlbind
              "select index_name    \n\
              \     , uniqueness    \n\
              \     , table_owner   \n\
              \--     , table_name  \n\
              \from sys.all_indexes \n\
              \where owner=?        \n\
              \and table_name=?     \n\
              \order by index_name  "
              [bindP schema, bindP table_name]
        iter :: (Monad m) => String -> String -> String -> IterAct m [(String, String, String)]
        iter a1 a2 a3 accum = result ((a1,a2,a3):accum)

      r <- (reverse .
            -- если это индекс для констрэйнта, то пропускаем его
            filter (\(index_name,_,_) -> not $ M.member index_name constraints))
           `liftM`
           doQuery stm iter []

      indexesDecl <- forM r $ \(index_name, uniqueness, table_owner) -> do

        index_columns <- getDeclIndexColumns schema index_name
        let
          headerDecl = "CREATE"
                       ++
                       (if uniqueness == "UNIQUE" then " UNIQUE" else "")
                       ++
                       " INDEX " ++ getSafeName index_name
                       ++
                       (if table_owner == schema
                        then printf "\n ON %s\n" (getSafeName table_name)
                        else printf "\n ON %s.%s\n" (getSafeName table_owner) (getSafeName table_name))
                       ++ "  (\n" ++ (concat . intersperse ",\n" . map ("   "++)) index_columns ++ "\n  )"

        return headerDecl

      if null indexesDecl
      then return Nothing
      else return . Just $ (concat . intersperse "\n/\n\n" $ indexesDecl) ++ "\n/\n"

      where
        getDeclIndexColumns schema index_name = do
          let
            stm1 =
              sqlbind "select column_name         \n\
                      \     , column_position     \n\
                      \     , descend             \n\
                      \  from sys.all_ind_columns \n\
                      \ where index_owner=?       \n\
                      \   and index_name=?        \n\
                      \order by column_position   " [bindP schema, bindP index_name]
            stm2 =
              sqlbind "select column_name         \n\
                      \     , column_position     \n\
                      \     , '' as descend       \n\
                      \  from sys.all_ind_columns \n\
                      \ where index_owner=?       \n\
                      \   and index_name=?        \n\
                      \order by column_position   " [bindP schema, bindP index_name]
            iter :: (Monad m) => String -> Integer -> String -> IterAct m [(String, Integer, String)]
            iter a1 a2 a3 accum = result ((a1,a2,a3):accum)
          
          
          column_index_expressions_map <- getIndexColumnExpressions
          r <- reverse `liftM`
               catchDB (doQuery stm1 iter []) (\_ -> doQuery stm2 iter [])

          return $ map (getDeclColumnIndex column_index_expressions_map) r


            where
              -- Табицы all_ind_expressions не было в Oracle 7, поэтому строим хэш column_expression
              -- по ключу номера колонки отдельно.
              getIndexColumnExpressions = do
                let
                  stmb = sqlbind "select column_position         \n\
                                 \     , column_expression       \n\
                                 \  from sys.all_ind_expressions \n\
                                 \ where index_owner=?           \n\
                                 \   and index_name=?            " [bindP schema, bindP index_name]
                  iter :: (Monad m) => Integer -> String -> IterAct m (M.Map Integer String)
                  iter cp ce accum = result $ M.insert cp ce accum

                m  <- doQuery stmb iter M.empty
                return m

              -- Возвращает декларацию колонки индекса
              getDeclColumnIndex column_index_expressions_map (column_name, column_position, descend) =
                strip $ M.findWithDefault column_name column_position column_index_expressions_map
                ++
                (if map toUpper descend == "DECS" then " DESC" else "")

        
      
