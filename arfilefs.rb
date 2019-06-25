require 'rfusefs'
require 'pathname'

require_relative 'logger2'
require_relative 'libarfile'

class ArfileFS < FuseFS::FuseDir

  def initialize
    $Log = Logging.logger(STDOUT)
    $Log.level = :info
    $Log.info('Initializing ArFileFS')
    $Log.info("Mountpoint: #{ARGV[0]}")
    $Log.info("Archive: #{ARGV[1]}")
    @archive = ArFile.new(ARGV[1], $Log)
    @files = {}
    @dirs = [] 
    @archive.list.each_pair do |file_id, file_metadata|
      filepath = File.dirname(file_metadata['Path'])
      filepath = '/' if filepath == '.'
      filepath = '/' + filepath unless filepath.start_with?('/')
      filename = File.basename(file_metadata['Path'])
      if filepath[-1] == '/'
        $Log.debug("Add #{filepath + filename} => #{file_id} to @files")
        @files[filepath + filename] = file_id
      else
        $Log.debug("Add #{filepath + '/' + filename} => #{file_id} to @files")
        @files[filepath + '/' + filename] = file_id
      end

      filepath = File.dirname(file_metadata['Path'])
      parent = ''
      filepath.split('/').each do |foldername|
        next if foldername == '.'
        path = parent + '/' + foldername
        $Log.debug("Add #{path} to @dirs")
        @dirs << path unless @dirs.include?(path)
        parent = path
      end
    end
  end

  def contents(path)
    filelist = []
    relative_part = 
    @files.each_pair do |file_path, file_id|
      dirname = File.dirname(file_path)
      basename = File.basename(file_path)
      if path == dirname
        filelist << basename
      end
    end 
    @dirs.each do |dir_path|
      dirname = dir_path.split('/')[-1]
      parent = '/' + dir_path.split('/')[1..-2].join('/')
      filelist << dirname if parent == path
    end 
    return filelist
  end

  def directory?(path)
    @dirs.include?(path)
  end

  def file?(path)
    @files.keys.include?(path)
  end

  def read_file(path)
    return @archive.extracts(@files[path])
  end

  def can_write?(path)
    return true
  end

  def write_to(path, data)
    puts "Overwrite File" 
    if @files.has_key?(path)
      $Log.info("Overwrite File #{path}")
      file_id = @files[path]
      @archive.delete(file_id)
    else
      puts "New File" unless @files.has_key?(path)
    end
    file_id = @archive.adds(path, data)
    @files[path] = file_id
  end

  def can_delete?(path)
    return true
  end

  def delete(path)
    @archive.delete(@files[path])
    @files.delete(path)
  end

  def can_mkdir?(path)
    return true
  end

  def mkdir(path)
    @dirs << path
  end

  def can_rmdir?(path)
    return true
  end

  def rmdir(path)
    @files.each do |file_path, file_id|
      delete(file_path) if file_path.start_with?(path)
    end
    @dirs.delete(path)
  end


end

# Usage: #{$0} mountpoint [mount_options]
FuseFS.main([ARGV[0]]) { |options| ArfileFS.new }
