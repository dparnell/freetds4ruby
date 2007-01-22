#!/usr/bin/env ruby
require 'freetds'
require 'test/unit'

class TestFreeTDS < Test::Unit::TestCase

  def setup
    @config = {
      'servername' => 'beast',
      'username' => 'sa',
      'password' => 'sa'
    }
  end
  
  def test_driver
    driver = FreeTDS::Driver.new
    assert_not_nil(driver, "driver creation failed")
    
    connection = driver.connect(@config)

    connection.statement('use eCareDev').execute

    connection.statement("SET TEXTSIZE #{1024*1024*1024}").execute

    statement = connection.statement('select * from Patient where Patient=144')
    statement.execute

    assert_not_nil(statement.columns, "columns should not be nil")
    assert_not_nil(statement.rows, "rows should not be nil")
    assert_equal(1, statement.rows.length, "only one row should have been returned")
    assert_nil(statement.status, "status should be nil")

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
  
end