require 'rubygems'
require 'mysql'

AVERAGING_TIMEFRAME = 2

Before do
  @username = 'root'
  @password = ''
  @database = nil
end

#Helper functions
def run_query(query)
  Mysql.connect(@host, @username, @password, @database).query(query)
end

def get_process_count()
  run_query('SHOW PROCESSLIST;').num_rows
end

def get_table_count(table_like)
  query = "select count(1) from information_schema.tables where table_schema = '#{@database}' and table_name like '#{table_like}';"
  run_query(query).fetch_row[0].to_i
end

def get_global_status_sum(variables)
  in_string = variables.map{ |str| "'#{str}'" }.join(", ")
  query = "select sum(variable_value) as value from information_schema.global_status where variable_name in(#{in_string});" 
  run_query(query).fetch_row[0].to_i
end

def count_global_status_per_second(variables)
  starting = get_global_status_sum(variables) 
  sleep AVERAGING_TIMEFRAME
  ending = get_global_status_sum(variables) 
  (ending.to_i-starting.to_i)/AVERAGING_TIMEFRAME
end

# Step definitions for testing the state of the MySQL server
Given /I have a MySQL server on (.+)$/ do |host|
  @host = host
end

And /I use the username (.+)$/ do |username|
  @username = username
end

And /I use the password (.+)$/ do |password|
  @password = password
end

And /I use the database (.+)$/ do |database|
  @database = database
end

#schema tests:
Then /^it should have the table ([^\'\"]+)$/ do |table|
  get_table_count(table).should == 1
end

Then /^it should not have the table ([^\'\"]+)$/ do |table|
  get_table_count(table).should == 0
end

#statistical tests:
Then /it should have less than (\d+) processes$/ do |processes|
  get_process_count().should < processes.to_i
end

Then /it should have at least (\d+) processes$/ do |processes|
  get_process_count().should >= processes.to_i
end

Then /it should have less than (\d+) connections$/ do |connections|
  get_global_status_sum(['Connections']).should < connections.to_i
end

Then /it should have at least (\d+) connections$/ do |connections|
  get_global_status_sum(['Connections']).should >= connections.to_i
end

Then /it should have less than (\d+) threads running$/ do |threads|
  get_global_status_sum(['Threads_running']).should < threads.to_i
end

Then /it should have at least (\d+) threads running$/ do |threads|
  get_global_status_sum(['Threads_running']).should >= threads.to_i
end

Then /it should have less than (\d+) threads connected$/ do |threads|
  get_global_status_sum(['Threads_connected']).should < threads.to_i
end

Then /it should have at least (\d+) threads connected$/ do |threads|
  get_global_status_sum(['Threads_connected']).should >= threads.to_i
end

Then /it should have at least (\d+) queries cached$/ do |cached_queries|
  get_global_status_sum(['Qcache_queries_in_cache']).should >= cached_queries.to_i
end

Then /it should have at least (\d+) queries cached$/ do |cached_queries|
  get_global_status_sum(['Qcache_queries_in_cache']).should >= cached_queries.to_i
end

Then /it should have less than (\d+) update queries per second$/ do |queries|
  count_global_status_per_second(['Com_update']).should < queries.to_i
end

Then /it should have at least (\d+) update queries per second$/ do |queries|
  count_global_status_per_second(['Com_update']).should >= queries.to_i
end

Then /it should have less than (\d+) insert queries per second$/ do |queries|
  count_global_status_per_second(['Com_insert']).should < queries.to_i
end

Then /it should have at least (\d+) insert queries per second$/ do |queries|
  count_global_status_per_second(['Com_insert']).should >= queries.to_i
end

Then /it should have less than (\d+) delete queries per second$/ do |queries|
  count_global_status_per_second(['Com_delete']).should < queries.to_i
end

Then /it should have at least (\d+) delete queries per second$/ do |queries|
  count_global_status_per_second(['Com_delete']).should >= queries.to_i
end

Then /it should have less than (\d+) selects queries per second$/ do |queries|
  count_global_status_per_second(['Com_select']).should < queries.to_i
end

Then /it should have at least (\d+) selects queries per second$/ do |queries|
  count_global_status_per_second(['Com_select']).should >= queries.to_i
end

Then /it should have less than (\d+) queries per second$/ do |queries|
  count_global_status_per_second(['Com_update', 'Com_select', 'Com_delete', 'Com_insert']).should < queries.to_i
end

Then /it should have at least (\d+) queries per second$/ do |queries|
  count_global_status_per_second(['Com_update', 'Com_select', 'Com_delete', 'Com_insert']).should >= queries.to_i
end

Then /it should have less than (\d+) cache hits per second$/ do |queries|
  count_global_status_per_second(['Qcache_hits']).should < queries.to_i
end

Then /it should have at least (\d+) cache hits per second$/ do |queries|
  count_global_status_per_second(['Qcache_hits']).should >= queries.to_i
end

Then /it should have less than (\d+) slow queries per second$/ do |queries|
  count_global_status_per_second(['Slow_queries']).should < queries.to_i
end

Then /it should have at least (\d+) slow queries per second$/ do |queries|
  count_global_status_per_second(['Slow_queries']).should >= queries.to_i
end

Then /it should have less than (\d+) questions per second$/ do |queries|
  count_global_status_per_second(['Questions']).should < queries.to_i
end

Then /it should have at least (\d+) questions per second$/ do |queries|
  count_global_status_per_second(['Questions']).should >= queries.to_i
end