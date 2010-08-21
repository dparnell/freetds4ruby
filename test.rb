#!/usr/bin/env ruby
require 'freetds'
require 'test/unit'

class TestFreeTDS < Test::Unit::TestCase

  def setup
    @config = {
      :hostname => 'sbs2003.local',
      :username => 'sa',
      :password => 'h3althsolv3'
    }
  end
  
  def test_driver
    driver = FreeTDS::Driver.new
    assert_not_nil(driver, "driver creation failed")
    
    connection = driver.connect(@config)

    begin
      connection.statement('drop database freetds_test').execute      
    rescue  
    end
    
    connection.statement('create database freetds_test').execute
      
    connection.statement('use freetds_test').execute
    connection.statement('create table posts ( id int, name varchar(100), body text, post_date datetime)').execute
    connection.statement("insert into posts values (1, 'Foo', 'This is a test message', getdate())").execute

    connection.statement("SET TEXTSIZE #{1024*1024*1024}").execute

    statement = connection.statement('select * from posts where id=1')
    statement.execute

    assert_not_nil(statement.columns, "columns should not be nil")
    # assert_equal(4, statement.columns.size, "there should be 4 columns") # TODO: columns not implemented yet
    assert_not_nil(statement.rows, "rows should not be nil")
    assert_equal(1, statement.rows.length, "only one row should have been returned")
    assert_nil(statement.status, "status should be nil")
    
    assert_equal(1, statement.rows[0]["id"], "post id should match")
    assert_equal("Foo", statement.rows[0]["name"], "post name should match")

    row = statement.rows.first
    statement.columns.each do |col|
      assert_not_nil(col['name'])
      assert(row.has_key?(col['name']), "Column #{col[name]} missing from selected row")
    end
    
    bad_statement = connection.statement('this should fail')
    assert_raise(IOError, "The statement execution should have failed with an IOError") {
      bad_statement.execute
    }
  end
  
  def test_bad_connection
    driver = FreeTDS::Driver.new
    assert_not_nil(driver, "driver creation failed")
    
    # these don't hit the server
    assert_raise(ArgumentError) { driver.connect({}) }
    assert_raise(ArgumentError) { driver.connect({:hostname => 'this host should not exist'}) }
    assert_raise(ArgumentError) { driver.connect({:hostname => 'this host should not exist', :port => 1234}) }
    
    assert_raise(IOError) { driver.connect({:hostname => 'this host should not exist', :port => 1234, :username => 'xxxx'}) }
    assert_raise(IOError) { driver.connect({:servername => 'beast', :username => 'xxxx'}) }
  end
  
end
