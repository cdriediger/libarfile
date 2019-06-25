#!/usr/bin/ruby

require 'securerandom'
require 'digest/md5'
require 'fileutils'
require 'time'
require 'msgpack'
require 'json'
require_relative 'logger2'
require_relative 'ChunkManager2'
require_relative 'MetadataManager'
require_relative 'ArchiveManager'
require_relative 'FileManager'

# TODO:
# * replace local logger class with ruby std logger
#   * build config option for logging level / destination
#   * maybe build two logger instances (STDOUT / logfile)

class Integer

  def to_filesize
    {
      'B'  => 1024,
      'KB' => 1024 * 1024,
      'MB' => 1024 * 1024 * 1024,
      'GB' => 1024 * 1024 * 1024 * 1024,
      'TB' => 1024 * 1024 * 1024 * 1024 * 1024
    }.each_pair { |e, s| return "#{(self.to_f / (s / 1024)).round(2)}#{e}" if self < s }
  end

end

module Usid

  def self.usid
    time = Time.now.to_i.to_s
    random = SecureRandom.hex(3).to_s
    return time + random
  end

end

class ArFile

  attr_reader :Path
  attr_reader :metadata
  attr_accessor :archive

  def initialize(path, log=nil)
    @closed = false
    @closing = false
    @path = File.absolute_path(path)
    if log
      $Log = log
    else
      $Log = Logging.logger(STDOUT)
      $Log.level = :warn
    end
    $Log.info('Initializing ArFile')
    @metadata = MetadataManager.new(@path, read_only=false)
    @archive = ArchiveManager.new(@path, @metadata)
    @metadata.set_archive(@archive)
    if @metadata.new?
      @calculated_part_hashes = true
    else
      @calculated_part_hashes = false
    end
    $print_debug = false
    trap('INT') do
      unless @closing
        @closing = true
        $Log.error('!!ABORTING!!')
        close
        exit
      end
    end
    ObjectSpace.define_finalizer(self, Proc.new do
      unless @closing
        @closing = true
        $Log.info('!!Finished!!')
        close
      end
    end
    )
  end

  def close
    return false if @closed
    @closed = true
    $Log.info('Closing ArFile')
    @metadata.close
    @archive.close
    $Log.debug('Archive Closed')
  end

  def list
    return @metadata['Files']
  end

  def list_snapshots
    return @metadata['Snapshots']
  end

  def list_container
    return @metadata['Container']
  end

  def get_file_info(id)
    return @metadata['Files'][id]
  end

  def files_by_name(filename)
    return @metadata.files_by_name(filename)
  end

  def file_by_path(filepath)
    return @metadata.file_by_path(filepath)
  end

  def files_by_container(containername)
    return @metadata.files_by_container(containername)
  end

  def add(file, enable_dedup = true, chunk_size=10_485_760)
    $Log.info('Adding new File')
    if @archive.closed?
      $Log.error('    Archive is closed')
      return
    end
    if file.is_a?(String)
      path = File.absolute_path(file)
      if File.exist?(path)
        fileobj = FileManager.new(path)
        $Log.info(" Filename: #{path}")
        if File.size?(path)
          if File.size?(path) > 10_485_760 and enable_dedup
            enable_dedup = false
            $Log.info('    Disabled Dedublication. File is to big')
          end
          $Log.info(" Filesize: #{File.size?(path).to_filesize}")
        else
          $Log.info(' Adding Empty File')
        end
      else
        $Log.error("    No such File #{path}")
        return 1
      end
    elsif file.is_a?(File)
      fileobj = file
      path = File.absolute_path(fileobj.path)
      $Log.info(' Fileobject. No name and size present')
    else
      $Log.info(' File need to be a FileObject or a Path')
      return
    end
    if fileobj.directory?
      create_container(file)
      return
    end
    $Log.info("is_ascii?: #{is_ascii?(path)}")
    if not is_ascii?(path) and enable_dedup
      enable_dedup = false
      $Log.info('    Disabled Dedublication. File is not ASCII')
    end
    calculate_part_hashes if enable_dedup and not @calculated_part_hashes
    @metadata.enable_edit
    if @metadata['FileHashes'].keys.include?(fileobj.hash)
      hash = fileobj.hash
      existing_file_id = @metadata['FileHashes'][hash]
      $Log.error("    File already in Archive. ID: #{existing_file_id}")
      if fileobj.path[1..-1] == @metadata['Files'][existing_file_id]['Path']
        fileobj.close
        return @metadata['FileHashes'][hash]
      else
        file_id = @metadata.new_file(fileobj.path[1..-1], 'zlib', fileobj.hash, enable_dedup)
        @metadata['Files'][file_id]['Parts'] = @metadata['Files'][@metadata['FileHashes'][hash]]['Parts']
        return file_id
      end
    end
    $Log.debug("Using chunk size: #{chunk_size} bytes")
    file_id = @metadata.new_file(fileobj.path[1..-1], 'zlib', fileobj.hash, enable_dedup)
    part_id = 0
    enable_dedup = false
    if enable_dedup
      until fileobj.eof?
        chunk_id = @archive.write_part(fileobj.read(chunk_size), dedup = true)
        @metadata.new_part(file_id, part_id, chunk_id)
        part_id += 1
      end
    else
      until fileobj.eof?
        chunk_id = @archive.write_part(fileobj.read(chunk_size), dedup = false)
        @metadata.new_part(file_id, part_id, chunk_id)
        part_id += 1
      end
    end
    if @metadata['Container'].include?(fileobj.dirname)
      add_to_container(fileobj.dirname, file_id)    
    end
    fileobj.close
    @metadata.commit
    $Log.info('Finished adding File')
    return file_id
  end

  def adds(path, data, enable_dedup = true, chunk_size=10_485_760)
    $Log.info('Adding new Data')
    if @archive.closed?
      $Log.error('    Archive is closed')
      return
    end
    calculate_part_hashes if enable_dedup and not @calculated_part_hashes
    @metadata.enable_edit
    hash = Hasher.hash(data)
    if @metadata['FileHashes'].keys.include?(hash)
      existing_file_id = @metadata['FileHashes'][hash]
      $Log.error("    File already in Archive. ID: #{existing_file_id}")
      if path[1..-1] == @metadata['Files'][existing_file_id]['Path']
        return @metadata['FileHashes'][hash]
      else
        file_id = @metadata.new_file(path[1..-1], 'zlib', hash, enable_dedup)
        @metadata['Files'][file_id]['Parts'] = @metadata['Files'][@metadata['FileHashes'][hash]]['Parts']
        return file_id
      end
    end
    $Log.debug("Using chunk size: #{chunk_size} bytes")
    file_id = @metadata.new_file(path[1..-1], 'zlib', hash, enable_dedup)
    part_id = 0
    enable_dedup = false
    chunk_id = @archive.write_part(data, dedup = enable_dedup)
    @metadata.new_part(file_id, part_id, chunk_id)
    @metadata.commit
    $Log.info('Finished adding File')
    return file_id
  end

  def delete(id)
    $Log.info("Deleting File ID: #{id}")
    if @archive.closed?
      $Log.error('Archive is closed')
      return false
    end
    puts @metadata['Files'].keys[0].class
    puts id.class
    unless @metadata['Files'].keys.include?(id)
      $Log.error 'File ID not found'
      return false
    end
    @metadata.enable_edit
    parts = @metadata['Files'][id]['Parts']
    parts.values.each do |chunk_id|
      #@archive.overwrite_chunk(start, length)
      @metadata.chunks.return_chunk(chunk_id)
    end
    @metadata.delete_file(id)
    @metadata.commit
    $Log.info('Finished deleting File')
    return true
  end

  def extract(id, dest=nil)
    $Log.info("Extracting File ID: #{id} to #{dest}")
    if @archive.closed?
      $Log.debug('Archive is closed')
      return false
    end
    $Log.debug('-----------')
    if dest.is_a?(String)
      dest_folder = File.absolute_path(dest.split('/')[0..-2].join('/'))
      if dest == '/'
        dest_folder = '/'
      elsif File.directory?(dest_folder)
        fileobj = FileManager.new(File.absolute_path(dest))
        fileobj.enable_write
      else
        $Log.error("    No such Directory: #{dest_folder}")
        return false
      end
    else
      $Log.error('    No destination given')
      return
    end
    unless @metadata['Files'].keys.include?(id)
      $Log.error "File ID '#{id}' not found"
      puts @metadata['Files']
      return false
    end
    parts = @metadata['Files'][id]['Parts']
    $Log.debug("    Parts: #{parts}")
    orig_path = @metadata['Files'][id]['Path']
    $Log.debug("    Orignal Path: #{orig_path}")
    parts.values.each do |chunk_id|
      $Log.debug("    Reading Chunk: #{chunk_id}")
      data = @archive.read_chunk(chunk_id)
      fileobj.write(data)
    end
    fileobj.close
    $Log.info('Finished extracting')
  end

  def extracts(id)
    $Log.info("Extracting File ID: #{id}")
    if @archive.closed?
      $Log.debug('Archive is closed')
      return false
    end
    unless @metadata['Files'].keys.include?(id)
      $Log.error 'File ID not found'
      return false
    end
    parts = @metadata['Files'][id]['Parts']
    $Log.debug("    Parts: #{parts}")
    data = ''
    parts.values.each do |chunk_id|
      $Log.debug("    Reading Chunk: #{chunk_id}")
      data += @archive.read_chunk(chunk_id)
    end
    $Log.info('Finished extracting')
    return data
  end

  def create_snapshot(snapshot_name)
    $Log.info("Create Snapshot")
    @metadata.enable_edit
    snapshot_id = @metadata.create_snapshot(snapshot_name)
    @metadata.commit
	return snapshot_id
  end

  def create_file_snapshot(file_id)
    $Log.info("Create File Snapshot")
    unless @metadata['Files'].keys.include?(file_id)
      $Log.error 'File ID not found'
      return false
    end
    @metadata.enable_edit
    snapshot_id = @metadata.create_snapshot(snapshot_id = Usid.usid, time = Time.now.to_f, file_id = file_id)
    @metadata.commit
	return snapshot_id
  end
  
  def get_snapshot_id(snapshot_name)
	return @metadata.get_snapshot_by_name(snaphot_name)
  end

  def rollback_snapshot(snapshot_id)
    $Log.info("Rollback Snapshot: #{snapshot_id}")
    @metadata.enable_edit
    result = @metadata.rollback_snapshot(snapshot_id)
    @metadata.commit
  end

  def delete_snapshot(snapshot_id)
    $Log.info("Delete Snapshot: #{snapshot_id}")
    @metadata.enable_edit
    result = @metadata.delete_snapshot(snapshot_id)
    @metadata.commit
  end

  def create_container(container_name)
    $Log.info("Create Container: #{container_name}")
    @metadata.enable_edit
    result = @metadata.create_container(container_name)
    @metadata.commit
  end

  def delete_container(container_name)
    $Log.info("Delete Container #{container_name}")
    @metadata.enable_edit
    result = @metadata.delete_container(container_name)
    @metadata.commit
  end

  def add_to_container(container_name, file_id)
    $Log.info("Add File: #{file_id} to Container: #{container_name}")
    unless @metadata['Files'].include?(file_id)
      $Log.error("File ID #{file_id} #{file_id.class} not found")
      return
    end
    @metadata.enable_edit
    result = @metadata.add_to_container(container_name, file_id)
    @metadata.commit
  end

  def delete_from_container(file_id)
    $Log.info("Delete File: #{file_id} from Container")
    @metadata.enable_edit
    result = @metadata.delete_from_container(file_id)
    @metadata.commit
  end

  def delete_whitespace
    $Log.info("Deleting Whitespace")
    tmparchivefile_path = @path + '.new'
    old_metadata = @metadata
    old_metadata.deactivate
    old_archive = @archive
    new_metadata = MetadataManager.new(tmparchivefile_path, read_only=false)
    new_archive = ArchiveManager.new(tmparchivefile_path, new_metadata)
    new_metadata.set_archive(new_archive)
    new_metadata.enable_edit
    @transfered_files = []
    @transfered_chunks = {}
    old_metadata['Files'].each do |file_id, file_data|
      $Log.debug("Transfer File to new Archive. File_id: #{file_id} | File_data: #{file_data}")
      transfer_file(file_id, file_data, new_metadata, old_archive, new_archive)
      @transfered_files << file_id
    end
    old_metadata['Snapshots'].each do |snaphot_id, snapshot_metadata|
      $Log.debug("Transfer Files of Snapshot: #{snaphot_id}  to new Archive.")
      new_snapshot_metadata = {}
      snapshot_metadata['Files'].each do |file_id, file_data|
        next if @transfered_files.include?(file_id)
        $Log.debug("    Transfer File to new Archive. File_id: #{file_id} | File_data: #{file_data}")
        new_file_id = transfer_file(file_id, file_data, new_metadata, old_archive, new_archive)
        new_snapshot_metadata[file_id] = new_metadata['Files'][file_id]
        new_metadata.delete_file(new_file_id)
        @transfered_files << file_id
      end
      new_metadata.create_snapshot(snaphot_id, snapshot_metadata['Created'], new_snapshot_metadata)
    end
    #old_archive.delete
    @archive = new_archive
    @archive.rename(@path)
    @metadata = new_metadata
    @metadata.commit
    old_metadata = nil
    old_archive = nil
    @transfered_files = nil
    @transfered_chunks = nil
  end

  def transfer_file(file_id, file_data, metadata, src_archive, dst_archive)
    file_id = metadata.new_file(file_data['Path'], 'zlib', file_data['md5'], file_data['dedup'], file_id)
    file_data['Parts'].each_key do |part_id|
      old_chunk_id = file_data['Parts'][part_id]
      part_data = src_archive.read_chunk(old_chunk_id)
      if @transfered_chunks.include?(old_chunk_id)
        new_chunk_id = @transfered_chunks[old_chunk_id]
      else
        new_chunk_id = dst_archive.write_part(part_data, dedup = file_data['dedup'])
        @transfered_chunks[old_chunk_id] = new_chunk_id
      end
      metadata.new_part(file_id, part_id, new_chunk_id)
    end
    return file_id
  end

  def calculate_part_hashes
    $Log.info('Calculating chunk hashes')
    @metadata['Files'].each_key do |file_id|
      @metadata['Files'][file_id]['Parts'].each_pair do |part_id, chunk_id|
        hash = Digest::MD5.new
        hash.update(@archive.read_chunk(chunk_id))
        @metadata.set_part_hash(chunk_id, hash.hexdigest)
      end
    end
    @calculated_part_hashes = true
  end

  def is_ascii?(filepath)
    file_type = filepath.split('.')[-1]
    file_type_whitelist = ['sql']
    return true if file_type_whitelist.include?(file_type)
    if not Gem.win_platform?
        puts "file -be apptype -e encoding -e tokens -e cdf -e compress -e elf -e soft -e tar #{filepath}"
        result = `file -be apptype -e encoding -e tokens -e cdf -e compress -e elf -e soft -e tar #{filepath}`
        return true if result.include?("ASCII")
    end
    return false
  end

  private :transfer_file, :calculate_part_hashes

end
