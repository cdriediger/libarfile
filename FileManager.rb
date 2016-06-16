class FileManager

  attr_reader :path

  def initialize(path)
    $Log.debug("FM: INIT FILEMANAGER #{path}")
    @path = path
    @readpos = 0
    @hash = nil
    @mode = 'r'
    unless File.exist?(@path)
      File.open(@path, 'w').close
    end
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
  end

  def close
    $Log.debug("FM: CLOSE FILEMANAGER #{path}")
    @filehandle.close
    @closed = true
  end

  def closed?
    return @closed
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
    $Log.debug("FM: READ #{data.length} Bytes from #{path}")
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
    if start
      @filehandle.seek(start)
    end
    length = @filehandle.write(data)
    $Log.debug("FM: WROTE #{length} Bytes to #{path}")
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
    close
    File.rename(@path, new_path)
    @path = new_path
    open(@mode)
  end

  def hash
    $Log.debug("FM: GET HASH OF FILE #{path}")
    return @hash if @hash
    cur_pos = @filehandle.tell
    @filehandle.seek(0)
    @hash = Digest::MD5.hexdigest(@filehandle.read)
    @filehandle.seek(cur_pos)
    return @hash
  end

end
