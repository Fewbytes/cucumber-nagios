require 'rubygems'
require 'mysql'

# simple function to get at the number of currently running processes
# on a mysql server
def get_process_count()
  my = Mysql.connect(@host, @username, @password)
  results = my.query('SHOW PROCESSLIST;')
  @processes = results.size
end

# set up some defaults so we can compare numbers without
# raising exceptions and don't have to specify the username
# and password if we're testing a default local install
Before do
  @processes = 0
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
