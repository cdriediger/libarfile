#!/usr/bin/ruby

require './libarfile.rb'
require 'optparse'

def list_files(path)
    files = []
    files_count = 0
    entries = Dir.entries(path)
    entries.delete(".")
    entries.delete("..")
    for entry in entries
        filepath = path + "/" + entry
        if File.file?(filepath)
            filepath.gsub!('//', '/')
            puts(filepath)
            files << filepath
            files_count += 1
        elsif File.directory?(filepath)
            tmp_filelist, tmp_filescount = list_files(filepath)
            files.concat(tmp_filelist)
            files_count += tmp_filescount
        end
    end
    return files, files_count
end

options = {}
jobs = []

optparse = OptionParser.new do |opts|
  opts.on('-A', '--archive ARCHIVE', "Archive File Name") do |archive|
    options[:archive] = archive
  end
  opts.on('-a', '--add-file FILE_PATH', 'Add File') do |file_path|
    jobs << ['add_file', file_path]
  end
  opts.on('--add-folder FOLDER_PATH', 'Add File') do |folder_path|
    jobs << ['add_folder', folder_path]
  end
  opts.on('-d', '--delete-file', 'Delete File') do
    jobs << ['delete']
  end
  opts.on('-e', '--extract DEST_FILE_PATH') do |file_path|
    jobs << ['extract', file_path]
  end
  opts.on('--snapshot-create') do
    jobs << ['snapshot-create']
  end
  opts.on('--snapshot-rollback') do |snapshot_id|
    jobs << ['snapshot-rollback']
  end
  opts.on('--snapshot-delete') do |snapshot_id|
    jobs << ['snapshot-delete']
  end
  opts.on('--create-container CONTAINER_NAME') do |container_name|
    jobs << ['create-container', container_name]
  end
  opts.on('--delete-container CONTAINER_NAME') do |container_name|
    jobs << ['delete-container', container_name]
  end
  opts.on('--container CONTAINER_NAME') do |container_name|
    options[:container_name] = container_name
  end
  opts.on('-f', '--file FILE_ID') do |file_id|
    options[:file_id] = file_id
  end
  opts.on('--files FILE_ID,FILE_ID') do |file_ids|
    options[:file_ids] = file_ids.split(',')
  end
  opts.on('-s', '--snapshot SNAPSHOT_ID') do |snapshot_id|
    options[:snapshot_id] = snapshot_id
  end
  opts.on('--add', 'Add File to Container') do
    jobs << ['add-to-container']
  end
  opts.on('--delete', 'Delete File from Container') do
    jobs << ['delete-from-container']
  end
  opts.on('-l', '--list-files') do
    jobs << ['list-files']
  end
  opts.on('--list-snapshots') do
    jobs << ['list-snapshots']
  end
  opts.on('--list-snapshots-names') do
    jobs << ['list-snapshots-names']
  end
  opts.on('--list-container') do
    jobs << ['list-container']
  end
  opts.on('--list-container-names') do
    jobs << ['list-container-names']
  end
end.parse!

if options[:archive].nil?
  puts '-A, --archive ARCHIVE missing'
  raise OptionParser::MissingArgument
end

unless jobs.empty?
  archive = ArFile.new(options[:archive])
else
  Kernel.exit!
end

#puts "#############"
#puts jobs
#puts "#############"

jobs.each do |job|
  jobname = job[0]
  jobarguments = job[1..-1]
  #puts jobarguments
  #puts '--------------------'
  #puts options
  #puts '--------------------'
  #jobarguments.each {|argument| puts argument.class}
  if jobname == 'add_file'
    file_id = archive.add(jobarguments[0], enable_dedup = true)
    unless options[:container_name].nil?
      archive.add_to_container(options[:container_name], file_id)
    end
  elsif jobname == 'add_folder'
    filelist, files_count = list_files(jobarguments[0])
    for file in filelist
      file_id = archive.add(file, enable_dedup = true)
      unless options[:container_name].nil?
        archive.add_to_container(options[:container_name], file_id)
      end
    end
  elsif jobname == 'delete'
    if options[:file_id].nil? and options[:file_ids].nil? and options[:container_name].nil?
      puts "--file FILE_ID or --container CONTAINER_NAME missing"
      raise OptionParser::MissingArgument
    else
      unless options[:file_id].nil?
        success = archive.delete(options[:file_id])
      end
      unless options[:file_ids].nil?
        options[:file_ids].each do |file_id|
          unless archive.delete(file_id)
            success = false
          end
        end
      end
      unless options[:container_name].nil?
        file_list = archive.files_by_container(options[:container_name])
        file_list.keys.each do |file_id|
          unless archive.delete(file_id)
            success = false
          end
        end
      end
      archive.delete_whitespace if success
    end
  elsif jobname == 'extract'
    if options[:file_id].nil?
      puts "--file FILE_ID missing"
      raise OptionParser::MissingArgument
    else
      archive.extract(options[:file_id], jobarguments[0])
    end
  elsif jobname == 'snapshot-create'
    if options[:file_id].nil?
      archive.create_snapshot
    else
      archive.create_file_snapshot(options[:file_id])
    end
  elsif jobname == 'snapshot-rollback'
    if options[:snapshot_id].nil?
      puts "--snapshot SNAPSHOT_ID missing"
      raise OptionParser::MissingArgument
    else
      archive.rollback_snapshot(options[:snapshot_id])
      archive.delete_whitespace
    end
  elsif jobname == 'snapshot-delete'
    if options[:snapshot_id].nil?
      puts "--snapshot SNAPSHOT_ID missing"
      raise OptionParser::MissingArgument
    else
      archive.delete_snapshot(options[:snapshot_id])
      archive.delete_whitespace
    end
  elsif jobname == 'list-files'
    if options[:container_name].nil?
      file_list = archive.list
      puts "Files in Archive:"
    else
      file_list = archive.files_by_container(options[:container_name])
      puts "Files in Container: #{options[:container_name]}:"
    end
    file_list.each do |file_id, file_data|
      puts "  ID: '#{file_id}'"
      puts "  PATH: #{file_data['Path']}"
      if file_data['container']
        puts "  CONTAINER: #{file_data['container']}"
      end
      puts "---------------"
    end
    puts "Found #{file_list.length} files"
  elsif jobname == 'list-snapshots'
    puts "Snapshot List:"
    archive.list_snapshots.each do |snapshot_id, snapshot_metadata|
      puts "Snapshot ID: #{snapshot_id}"
      puts "CREATED: #{snapshot_metadata['Created']}"
      puts "FILES: "
      snapshot_metadata['Files'].each do |file_id, file_data|
        puts "  File ID: #{file_id}"
        puts "  File PATH: #{file_data['Path']}"
        puts "  ######################"
      end
      puts "---------------"
    end
  elsif jobname == 'list-container'
    puts "Container List:"
    archive.list_container.each do |container_name, file_ids|
      puts "Container: #{container_name}"
      puts "Files:"
      file_ids.each do |file_id|
        puts "  File ID: #{file_id}"
        puts "  File PATH: #{archive.metadata['Files'][file_id]['Path']}"
        puts "  ######################"
      end
      puts "---------------"
    end
  elsif jobname == 'list-container-names'
    puts "Container Name List:"
    puts archive.list_container.keys.map{|name| "  " + name}
  elsif jobname == 'create-container'
    archive.create_container(jobarguments[0])
  elsif jobname == 'delete-container'
    archive.delete_container(jobarguments[0])
  elsif jobname == 'add-to-container'
    if options[:container_name].nil?
      puts "--container CONTAINER_NAME missing"
      raise OptionParser::MissingArgument
    elsif options[:file_id].nil? and options[:file_ids].nil?
      puts "--file FILE_ID missing"
      raise OptionParser::MissingArgument
    else
      unless options[:file_id].nil?
        archive.add_to_container(options[:container_name], options[:file_id])
      end
      unless options[:file_ids].nil?
        options[:file_ids].each do |file_id|
          archive.add_to_container(options[:container_name], file_id)
        end
      end
    end
  elsif jobname == 'delete-from-container'
    if options[:file_id].nil?
      puts "--file FILE_ID missing"
      raise OptionParser::MissingArgument
    else
      archive.delete_from_container(jobarguments[0])
    end
  end
end

archive.close
