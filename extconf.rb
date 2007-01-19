require 'mkmf'

if have_library("tds") && have_library("sybdb") && have_header("sqldb.h") && have_header("sqlfront.h")
  create_makefile ('freetds')
end