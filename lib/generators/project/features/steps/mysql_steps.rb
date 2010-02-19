require 'rubygems'
require 'mysql'

# simple functions to get at the number of currently running processes
# on a mysql server
def get_process_count()
  my = Mysql.connect(@host, @username, @password)
  results = my.query('SHOW PROCESSLIST;')
  @processes = results.size
end

# lots of helper funtions to retrieve information from the GLOBAL STATUS
# table where MySQL stores information about it's current state
def get_connection_count()
  my = Mysql.connect(@host, @username, @password)
  results = my.query("SHOW GLOBAL STATUS where Variable_name = 'Connections';")
  @connections = results.size
end

def get_threads_connected_count()
  my = Mysql.connect(@host, @username, @password)
  results = my.query("SHOW GLOBAL STATUS where Variable_name = 'Threads_connected';")
  @threads_connected = results.size
end

def get_threads_running_count()
  my = Mysql.connect(@host, @username, @password)
  results = my.query("SHOW GLOBAL STATUS where Variable_name = 'Threads_running';")
  @threads_running = results.size
end

def get_cached_queries_count()
  my = Mysql.connect(@host, @username, @password)
  results = my.query("SHOW GLOBAL STATUS where Variable_name = 'Qcache_queries_in_cache';")
  @cached_queries = results.size
end

def get_queries_per_second()
  my = Mysql.connect(@host, @username, @password)
  query = "SHOW GLOBAL STATUS where Variable_name = 'Com_update' or Variable_name = 'Com_select' or Variable_name = 'Com_delete' or Variable_name = 'Com_insert';"
  results = my.query(query)
  starting = results.fetch_row[1].to_i + results.fetch_row[1].to_i + results.fetch_row[1].to_i + results.fetch_row[1].to_i
  sleep 10
  results = my.query(query)
  ending = results.fetch_row[1].to_i + results.fetch_row[1].to_i + results.fetch_row[1].to_i + results.fetch_row[1].to_i
  # now calculate the difference and divide by 10
  (ending.to_i-starting.to_i)/10
end

def get_results_per_second(query)
  my = Mysql.connect(@host, @username, @password)
  results = my.query(query)
  starting = results.fetch_row[1]
  sleep 10
  results = my.query(query)
  ending = results.fetch_row[1]
  # now calculate the difference and divide by 10
  (ending.to_i-starting.to_i)/10
end

def get_updates_per_second()
  @updates_per_second = get_results_per_second("SHOW GLOBAL STATUS where Variable_name = 'Com_update';")
end

def get_selects_per_second()
  @selects_per_second = get_results_per_second("SHOW GLOBAL STATUS where Variable_name = 'Com_select';")
end

def get_deletes_per_second()
  @deletes_per_second = get_results_per_second("SHOW GLOBAL STATUS where Variable_name = 'Com_delete';")
end

def get_inserts_per_second()
  @inserts_per_second = get_results_per_second("SHOW GLOBAL STATUS where Variable_name = 'Com_insert';")
end

def get_cache_hits_per_second()
  @cache_hits_per_second = get_results_per_second("SHOW GLOBAL STATUS where Variable_name = 'Qcache_hots';")
end

def get_questions_per_second()
  @questions_per_second = get_results_per_second("SHOW GLOBAL STATUS where Variable_name = 'Questions';")
end

def get_slow_queries_per_second()
  @slow_queries_per_second = get_results_per_second("SHOW GLOBAL STATUS where Variable_name = 'Slow_queries';")
end

# specify the username and password if we're testing a default local install
Before do
  @username = 'root'
  @password = ''
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

Then /it should have less than (\d+) processes$/ do |processes|
  get_process_count()
  @processes.should < processes.to_i
end

Then /it should have less than (\d+) connections$/ do |connections|
  get_connection_count()
  @connections.should < connections.to_i
end

Then /it should have less than (\d+) threads running$/ do |threads|
  get_threads_running_count()
  @threads_running.should < threads.to_i
end

Then /it should have less than (\d+) threads connected$/ do |threads|
  get_threads_connected_count()
  @threads_connected.should < threads.to_i
end

Then /it should have at least (\d+) queries cached$/ do |cached_queries|
  get_cached_queries_count()
  @cached_queries.should >= cached_queries.to_i
end

Then /it should have less than (\d+) update queries per second$/ do |queries|
  get_updates_per_second()
  @updates_per_second.should < queries.to_i
end

Then /it should have less than (\d+) insert queries per second$/ do |queries|
  get_inserts_per_second()
  @inserts_per_second.should < queries.to_i
end

Then /it should have less than (\d+) delete queries per second$/ do |queries|
  get_deletes_per_second()
  @deletes_per_second.should < queries.to_i
end

Then /it should have less than (\d+) selects queries per second$/ do |queries|
  get_selects_per_second()
  @selects_per_second.should < queries.to_i
end

Then /it should have less than (\d+) queries per second$/ do |queries|
  get_queries_per_second()
  @queries_per_second.should < queries.to_i
end

Then /it should have less than (\d+) cache hits per second$/ do |queries|
  get_cache_hits_per_second()
  @cache_hits_per_second.should < queries.to_i
end

Then /it should have less than (\d+) slow queries per second$/ do |queries|
  get_slow_queries_per_second()
  @slow_queries_per_second.should < queries.to_i
end

Then /it should have less than (\d+) questions per second$/ do |queries|
  get_questions_per_second()
  @questions_per_second.should < queries.to_i
end

Then /it should have at least (\d+) processes$/ do |processes|
  get_process_count()
  @processes.should >= processes.to_i
end

Then /it should have at least (\d+) connections$/ do |connections|
  get_connection_count()
  @connections.should >= connections.to_i
end

Then /it should have at least (\d+) threads running$/ do |threads|
  get_threads_running_count()
  @threads_running.should >= threads.to_i
end

Then /it should have at least (\d+) threads connected$/ do |threads|
  get_threads_connected_count()
  @threads_connected.should >= threads.to_i
end

Then /it should have at least (\d+) queries cached$/ do |cached_queries|
  get_cached_queries_count()
  @cached_queries.should >= cached_queries.to_i
end

Then /it should have at least (\d+) update queries per second$/ do |queries|
  get_updates_per_second()
  @updates_per_second.should >= queries.to_i
end

Then /it should have at least (\d+) insert queries per second$/ do |queries|
  get_inserts_per_second()
  @inserts_per_second.should >= queries.to_i
end

Then /it should have at least (\d+) delete queries per second$/ do |queries|
  get_deletes_per_second()
  @deletes_per_second.should >= queries.to_i
end

Then /it should have at least (\d+) selects queries per second$/ do |queries|
  get_selects_per_second()
  @selects_per_second.should >= queries.to_i
end

Then /it should have at least (\d+) queries per second$/ do |queries|
  get_queries_per_second()
  @queries_per_second.should >= queries.to_i
end

Then /it should have at least (\d+) cache hits per second$/ do |queries|
  get_cache_hits_per_second()
  @cache_hits_per_second.should >= queries.to_i
end

Then /it should have at least (\d+) slow queries per second$/ do |queries|
  get_slow_queries_per_second()
  @slow_queries_per_second.should >= queries.to_i
end

Then /it should have at least (\d+) questions per second$/ do |queries|
  get_questions_per_second()
  @questions_per_second.should >= queries.to_i
end
