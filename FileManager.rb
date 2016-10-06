require 'digest'

class FileManager

  attr_reader :path

  class LittleLogger

    def info(msg)
      puts("INFO: #{msg}")
    end

    def error(msg)
      puts("ERROR: #{msg}")
    end

    def fatal_error(msg)
      puts("FATAL_ERROR: #{msg}")
    end

    def debug(msg)
      puts("DEBUG: #{msg}")
    end

  end

  class FileManagerError < IOError
  end

  def initialize(path)
    $Log = LittleLogger.new unless $Log
    @path = File.absolute_path(path)
	$Log.debug("FM: INIT FILEMANAGER #{@path}")
    @readpos = 0
    @writepos = 0
    @mode = 'r'
    unless File.exist?(@path)
      $Log.debug("FM: FILE DOES NOT EXIST. CREATING IT.")
      File.open(@path, 'w').close
    end
    @closed = true
    open
  end

  def open(mode=@mode)
    $Log.debug("FM: OPEN FILE #{path}")
    @mode = mode unless @mode == mode
    fd = IO.sysopen(@path, mode)
    @filehandle = IO.new(fd, mode)
    if Gem.win_platform?
      $Log.debug('FM: RUNNING ON WINDOWS. USING BINMODE')
      @filehandle.binmode
    end
    @closed = false
	return true
  end

  def close
    $Log.debug("FM: CLOSE FILEMANAGER #{path}")
    @filehandle.close
    @closed = true
  end

  def closed?
    return @closed
  end

  def directory?
    return File.directory?(@path)
  end

  def dirname
    return @path if directory?
    return @path.split('/')[0..-2].join('/')
  end

  def enable_write
    $Log.debug("FM: ENABLE WRITE #{path}")
    cur_pos = @filehandle.tell
    close
    open('r+')
    @filehandle.seek(cur_pos)
  end

  def read(length, start=@readpos)
    start=@readpos unless start
    @filehandle.seek(start)
    data = @filehandle.read(length)
    @readpos = @filehandle.tell
	if data
      $Log.debug("FM: READ #{data.length} Bytes from #{path}")
	else
	  $Log.error("FM: READ: Reached end of file.")
	end
    return data
  end

  def read_all
    $Log.debug("FM: READ ALL #{path}")
    cur_pos = @filehandle.tell
    @filehandle.seek(0)
    data = @filehandle.read
    @filehandle.seek(cur_pos)
    return data
  end

  def get_position
    return @filehandle.tell
  end

  def eof?
    return @filehandle.eof?
  end

  def write(data, start=nil)
    raise FileManagerError, 'Not in write mode' if @mode == 'r'
    if start
      @filehandle.seek(start)
    else
      @filehandle.seek(@writepos)
    end
	$Log.debug("FM: Seeked to start: #{@filehandle.tell}")
	start = @filehandle.tell
    length = @filehandle.write(data)
	@writepos = @filehandle.tell
	$Log.debug("FM: SET writepos = #{@filehandle.tell}")
    $Log.debug("FM: WROTE #{length} Bytes to #{path} starting at: #{start}")
    return length
  end

  def get_end_of_file
    $Log.debug("FM: GET END OF FILE #{path}")
    cur_pos = @filehandle.tell
    @filehandle.seek(0, IO::SEEK_END)
    end_of_file = @filehandle.tell
    @filehandle.seek(cur_pos)
    return end_of_file
  end

  def rename(new_path)
    $Log.debug("FM: RENAME #{path} to #{new_path}")
	new_path = File.absolute_path(new_path)
    close
    File.rename(@path, new_path)
    @path = new_path
    open(@mode)
  end

  def hash
    $Log.debug("FM: GET HASH OF FILE #{path}")
    cur_pos = @filehandle.tell
    @filehandle.seek(0)
    hash = Digest::MD5.hexdigest(@filehandle.read)
    @filehandle.seek(cur_pos)
    return hash
  end

  def size?
    @filehandle.fsync    
    return File.size?(@path)
  end    

end
