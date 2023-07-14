# frozen_string_literal: true

require 'csv'
require 'pg'

class CsvToPgTable
  def initialize(conn: nil, csv: nil, table_name: nil, remove_duplicates: false)
    @conn = conn
    @csv = csv
    @table_name = table_name
    @remove_duplicates = remove_duplicates
  end

  def run
    start_time = Time.now
    @conn = connect_to_database if @conn.nil?
    @csv = choose_csv if @csv.nil?
    @table_name = choose_table_name if @table_name.nil?
    drop_old_table
    @columns = data_types_in_columns
    create_table
    insert_data_into_table
    end_time = Time.now
    dedupe if @remove_duplicates
    @duration = end_time - start_time
    final_message
  end

  def dedupe
    puts "removing duplicates from both the table and spreadsheet for #{@table_name}"

    begin
      drop_temp_table_sql = "DROP TABLE temp_#{@table_name}"
      @conn.exec drop_temp_table_sql
    rescue StandardError
      puts 'no existing temp table to drop'
    end

    get_column_header_sql = "SELECT *
                            FROM information_schema.columns
                            WHERE table_name = '#{@table_name}'"
    column_headers = @conn.exec get_column_header_sql
    first_row = []
    column_headers.each do |ch|
      first_row.push ch['column_name']
    end

    order_by_string = ' ORDER BY '
    column_headers.each do |ch|
      order_by_string += "#{ch['column_name']},"
    end

    create_temp_table_sql = "CREATE TABLE temp_#{@table_name} AS SELECT * FROM #{@table_name} #{order_by_string.chop}"
    @conn.exec create_temp_table_sql
    drop_table_sql = "DROP TABLE #{@table_name}"
    @conn.exec drop_table_sql
    create_new_table_sql = "CREATE TABLE #{@table_name} AS SELECT DISTINCT * FROM temp_#{@table_name}"
    @conn.exec create_new_table_sql

    CSV.open(@csv, 'w') do |csv|
      csv << first_row
    end

    select_all_records_sql = "SELECT * FROM #{@table_name} #{order_by_string.chop}"
    all_rows = @conn.exec select_all_records_sql

    CSV.open(@csv, 'a+') do |csv|
      all_rows.each do |ar|
        array_of_values = []
        first_row.each do |r|
          array_of_values.push ar[r]
        end
        csv << array_of_values
      end
    end

    drop_temp_table_sql = "DROP TABLE temp_#{@table_name}"
    @conn.exec drop_temp_table_sql

    puts "duplicates have been removed for #{@table_name}."
  end

  def integer?(string)
    string.to_i.to_s == string
  end

  def convert_camel_case_to_snake_case(column_name)
    column_name.gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2')
               .gsub(/([a-z\d])([A-Z])/, '\1_\2')
               .downcase
  end

  def format_for_insert(value_hash)
    if value_hash.nil? || value_hash.strip.empty? || value_hash == []
      'NULL'
    else
      %('#{value_hash.gsub("'",
                           "''").strip.squeeze(' ')}')
    end
  end

  def connect_to_database
    puts 'enter database name (default: postgres)'
    dbname = gets.chomp
    dbname = 'postgres' if dbname.empty?
    puts 'enter user (default: postgres)'
    user = gets.chomp
    user = 'postgres' if user.empty?
    puts 'password (default: postgres)'
    password = gets.chomp
    password = 'postgres' if password.empty?
    puts 'port (default: 5432)'
    port = gets.chomp
    port = '5432' if port.empty?

    PG.connect(dbname: dbname, user: user, password: password, port: port)
  end

  def choose_csv
    csv_in_directory = Dir["#{Dir.getwd}/*.csv"]

    puts 'Choose which csv you want to import to a table by entering the number at the beginning:'

    csv_in_directory.each_with_index do |path, index|
      path.gsub!("#{Dir.getwd}/", '')
      puts "#{index}: #{path}"
    end

    chosen_index = gets.chomp

    csv = csv_in_directory[chosen_index.to_i]

    validate_chosen_index csv, chosen_index

    csv
  end

  def validate_chosen_index(csv, chosen_index)
    if !integer?(chosen_index)
      abort 'You have entered an invalid option.'
    elsif csv.nil?
      abort 'You have entered an invalid option.'
    end
  end

  def choose_table_name
    puts 'Enter table name. (will attempt to use filename if blank). ' \
         'If the tablename is the same, the old table will be dropped.'

    table_name = gets.chomp

    table_name = @csv.gsub('.csv', '') if table_name.empty?

    table_name
  end

  def drop_old_table
    drop_table = "DROP TABLE #{@table_name}"

    begin
      @conn.exec drop_table
    rescue StandardError
      puts 'no existing table to drop'
    end
  end

  def data_types_in_columns
    columns = nil
    data_type_hash = {}
    row_iteration = 0
    CSV.foreach(@csv, converters: :all) do |row|
      if row_iteration.zero?
        columns = row
        columns.each do |header|
          data_type_hash[:"#{header}"] = nil
        end
      end
      if row_iteration.positive?
        value_iteration = 0
        columns.each do |header|
          set_postgres_datatype header, data_type_hash, row[value_iteration]
          value_iteration += 1
        end
      end
      row_iteration += 1
    end

    data_type_hash.each do |key, value|
      data_type_hash[:"#{key}"] = 'TEXT' if value.nil?
    end
    data_type_hash
  end

  def set_postgres_datatype(header, data_type_hash, row)
    if row.instance_of?(String)
      data_type_hash[:"#{header}"] = 'TEXT'
    elsif row.instance_of?(Float) && data_type_hash[:"#{header}"] != 'TEXT'
      data_type_hash[:"#{header}"] = 'DECIMAL'
    elsif row.instance_of?(Integer) && data_type_hash[:"#{header}"].nil?
      data_type_hash[:"#{header}"] = 'BIGINT'
    end
  end

  def create_table
    table_creation = "CREATE TABLE #{@table_name} ("
    @columns.each_with_index  do |(key, _value), index|
      table_creation += ' , ' if index.positive?
      table_creation += "#{convert_camel_case_to_snake_case(key.to_s)} #{@columns[:"#{key}"]}"
    end
    table_creation += ')'

    @conn.exec(table_creation)
  end

  def insert_data_into_table
    insert_string = "INSERT INTO #{@table_name} VALUES ("

    CSV.foreach(@csv).with_index do |row, index|
      next if index.zero?

      values = row.map do |x|
        format_for_insert x
      end.join(',')
      this_insert_string = "#{insert_string}#{values})"

      @conn.exec this_insert_string
    end
  end

  def final_message
    puts "#{@table_name} has been created using #{@csv} in #{@duration}s"
  end
end

CsvToPgTable.new.run
