{-# LANGUAGE FlexibleContexts #-}

module Utils
  (
   clearSqlSource,
   stringCSI,
   printWarning,
   CharParser,
   isSpace,
   strip,
   parseMatch,
   safeValueByIndex,
   sqlComment,
   sqlEndLineComment
  )
where

import System.IO
import Text.Parsec
import Text.Parsec.String
import Data.List
import Data.Char (ord, toUpper, toLower)

type CharParser st = GenParser Char st

-- | Для однобайтовых кодировок некоторые символы национальных алфавитов могут ошибочно
-- интерпретироваться как пробельные символы. Поэтому реализуем здесь упрощённый вариант isSpace.
isSpace :: Char -> Bool
isSpace c =
  ord c <= ord ' '

strip :: String -> String
strip = dropWhile isSpace . dropWhileEnd isSpace

parseMatch txt pars =
  case parse pars "(unknown)" txt of
       Left _  -> False
       Right _ -> True

clearSqlSource :: String -> String
clearSqlSource src =
  dropWhileEnd isSpace $
  case parse parseSql "(unknown)" src of
    Left e -> error $ show e
    Right x -> concat x

parseSql :: CharParser st [String]
parseSql = do
  s1 <- many $ try sqlPart
  s2 <- many $ try clearedChar
  eof
  return $ s1 ++ [s2]

sqlPart :: CharParser st String
sqlPart =
  sqlLiteral <|> sqlComment <|> sqlEndLineComment <|> sqlCode

sqlLiteral :: CharParser st String
sqlLiteral = do
  s1 <- string "'" <?> "literal start(')"
  s2 <- many $ noneOf "'"
  s3 <- string "'" <?> "literal end (')"
  return $ s1 ++ s2 ++ s3

sqlComment :: CharParser st String
sqlComment = do
  try (string start) <?> "comment start (/*)"
  s1 <- manyTill normalizeEOLs (try $ lookAhead $ string end)
  try (string end) <?> "comment end (*/)"
  return $ start ++ dropEndLineSpaces s1 ++ end
  where
    start = "/*"
    end   = "*/"
  
sqlEndLineComment :: CharParser st String
sqlEndLineComment = do
  try (string "--") <?> "line comment start (--)"
  s1 <- manyTill clearedChar (lookAhead $ eol <|> (eof >> return ""))
  s2 <- eol <|> (eof >> return "")
  return $ "--" ++ dropWhileEnd isSpace s1 ++ s2

sqlCode :: CharParser st String
sqlCode = do
  s <- try (manyTill normalizeEOLs (lookAhead $ try (string "/*") <|> try (string "--") <|> string "'")) <|> many1 clearedChar
  return $ dropEndLineSpaces s

eol :: CharParser st String
eol =
  ( try (string "\n\r")
    <|>
    try (string "\r\n")
    <|>
    string "\n"
    <|>
    string "\r"
    <?> "end of line" ) >> return "\n"

normalizeEOLs :: CharParser st Char
normalizeEOLs =
  (try eol >> return '\n')
  <|>
  clearedChar

clearedChar :: CharParser st Char
clearedChar =
  (char '\0' >> return ' ')
  <|> 
  anyChar

dropEndLineSpaces :: String -> String
dropEndLineSpaces [] = []
dropEndLineSpaces s =
  let
    endEol = last s == '\n'
    ls = lines s
    lastLine = last ls
    initLines' = unlines $ map (dropWhileEnd isSpace) $ init ls
    lastLine' = if endEol then dropWhileEnd isSpace lastLine ++ "\n" else lastLine
  in
    initLines' ++ lastLine'

-- | Case and count of spaces insensitive variant of 'string'
stringCSI :: String -> CharParser st String
stringCSI pattern =
  try $ foldl glue (return "") $ map test (unwords $ words pattern)
  where test x = if isSpace x
                 then space >> spaces >> return " "
                 else string [toUpper x] <|> string [toLower x]
        glue x y = do
          s1 <- x
          s2 <- y
          return $ s1 ++ s2
  

printWarning :: String -> IO ()
printWarning = hPutStrLn stderr

safeValueByIndex :: [a] -> Int -> Maybe a
safeValueByIndex [] _ = Nothing
safeValueByIndex xs i
  | i < 0 = Nothing
  | succ i > length xs = Nothing
  | otherwise = Just $ xs !! i
