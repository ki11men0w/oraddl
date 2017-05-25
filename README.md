# oraddl
Utility for retrieving DDL SQL for all or listed database objects of the specified Oracle database schema.
Each database object is stored in a separate SQL file with DDL.

The main purpose of this utility is to track changes of database objects in a version control system or to find
out differencies between several instances of logically the same database scheme (what is actual for development
proccess for several vendors or several versions of the same software).

Retrieved SQLs are cleared from insignificant details (eg insignificatn trailing spaces in package sources). Also information
not related to business logic are ignored (eg phisical storage parameters of tables) and not fall into generated SQLs.

```
Retrieves DDL SQL for all or listed database objects of the specified Oracle database schema.

oraddl [OPTIONS]

Common flags:
     --connect=user/password@db                 Connect string
     --tables[=NAME,NAME,...]                   Names of tables to retrieve. Names are case
                                                sensitive. If you specify no names then all the
                                                tables will be retrieved
  -v --views[=NAME,NAME,...]                    Names of views to retrieve. Names are case
                                                sensitive. If you specify no names then all the
                                                views will be retrieved
     --sources=ITEM                             Names of sources (packages, functions,
                                                procedures, java, types) to retrieve. Names are
                                                case sensitive. If you specify no names then all
                                                the sources will be retrieved
     --triggers=ITEM                            Names of triggers to retrieve. Names are case
                                                sensitive. If you specify no names then all the
                                                triggers will be retrieved
     --synonyms=ITEM                            Names of synonyms to retrieve. Names are case
                                                sensitive. If you specify no names then all the
                                                synonyms will be retrieved
     --sequences=ITEM                           Names of sequences to retrieve. Names are case
                                                sensitive. If you specify no names then all the
                                                sequences will be retrieved
     --mviews[=NAME,NAME,...]                   Names of materialized views to retrieve. Names
                                                are case sensitive. If you specify no names then
                                                all the materialized views will be retrieved
     --mview-logs[=NAME,NAME,...] --mviewlogs   Names of tables for witch to retrieve
                                                materialized view logs. Names are case sensitive.
                                                If you specify no names then all materialized view
                                                logs will be retrieved
     --list=ITEM                                Names of objects of any type to retrieve. Names
                                                are case sensitive. Use this option instead of
                                                --tables, --views, etc. if the type of objects is
                                                unknown
     --listf=FILE                               File with list of intresting objects (one object
                                                per line). Names are case sensitive
     --dir=DIRNAME                              Directory where to put files whith SQL (default
                                                is current directory)
     --save-dollared                            By default objects with names containing the
                                                dollar sign ($) in their names are ignored if not
                                                specified explicitly via --tables, --list and etc.
                                                This option enables saving of such objects.
     --save-end-spaces --saveendspaces          By default insignificant spaces at end of each
                                                line are deleted, this option prevent this behavior
     --save-auto-generated --saveautogenerated  By default auto-generated objects (like indexes
                                                supporting constraints) not saved, this option
                                                enables auto-generated objects
  -? --help                                     Display help message
  -V --version                                  Print version information
     --numeric-version                          Print just the version number
```
