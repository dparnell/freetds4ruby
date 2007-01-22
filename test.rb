#!/usr/bin/env ruby
require 'freetds'

config = {
  'servername' => 'beast',
  'username' => 'sa',
  'password' => 'sa'
}

puts 'Creating driver'
driver = FreeTDS::Driver.new

puts 'Connecting to beast'
connection = driver.connect(config)

puts 'Using eCareDev database'
connection.statement('use eCareDev').execute

puts 'Setting text size'
connection.statement("SET TEXTSIZE #{1024*1024*1024}").execute

puts 'selecting from Patient table'
statement = connection.statement('select * from Patient ')
statement.execute

puts statement.columns.inspect
puts statement.rows.inspect
puts statement.status
