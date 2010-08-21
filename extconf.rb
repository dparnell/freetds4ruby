require 'mkmf'

if have_library("ct") && have_header("ctpublic.h")
  create_makefile ('freetds')
end
