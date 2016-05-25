class Fixnum
  def num_digits
    Math.log10(self).to_i + 1
  end
end

module Compressor

  #require 'lzma'

  def self.compress(data)
    #return LZMA.compress(data)
    #return Zlib::Deflate.deflate(data)
    return data
  end

  def self.restore(comp_data)
    #LZMA.decompress(comp_data)
    #return Zlib::Deflate.deflate(comp_data)
    return comp_data
  end

end

class ArchiveManager

  attr_reader :chunksize
  attr_reader :ReadPos
  attr_reader :end_of_archive
  attr_reader :max_chunk_size

  def initialize(path, metadata)
    @closed = false
    @path = path
    @metadata = metadata
    @readpos = 0
    @chunks = @metadata.chunks
    @max_chunk_size = 10**5 #2**12
    unless File.exist?(@path)
      File.open(@path, 'w').close
    end
    fd = IO.sysopen(@path, 'r+')
    @filehandle = IO.new(fd, 'r+')
    @filehandle.binmode
    @tmpfiles = []
    if @metadata.metadata_start_pos
      @metadata.set_end_of_archive(@metadata.metadata_start_pos)
    else
      @metadata.set_end_of_archive(0)
    end
  end

  def close
    @closed = true
    @filehandle.close
  end

  def get_end_of_file
    last_file_post = @filehandle.tell
    @filehandle.seek(0, IO::SEEK_END)
    end_of_file = @filehandle.tell
    @filehandle.seek(last_file_post)
    return end_of_file
  end

  def reopen
    fd = IO.sysopen(@path, 'r+')
    @filehandle = IO.new(fd)
    @filehandle.binmode
    @closed = false
  end

  def normalize_path(path)
    path.gsub!('//', '/')
    if path[0] == '/'
      path = path[1..-1]
    end
    return path
  end

  def get_bytes(count, byte)
    $Log.debug('AM: GET BYTES')
    if byte.length > 1
      byte = byte[-1]
    end
    return (0...count).map{ byte }.join('')
  end

  def get_part_hash(data)
    $Log.debug('AM: GET PART HASH')
    return Digest::MD5.hexdigest(data)
  end

  def write_part(data, dedup = false, is_superblock = false)
    $Log.debug('AM: WRITE PART')
    if is_superblock
      $Log.debug('   Data is Superblock')
      chunk_start = get_end_of_file
    else
      data = Compressor::compress(data)
      if dedup
        part_hash = get_part_hash(data)
        $Log.debug('   Enabled Dublication')
        if @metadata['PartHashes'].include?(part_hash)
          $Log.debug('   Found Part with same Hash')
          chunk_id = @metadata['PartHashes'][part_hash]
          @metadata.chunks.mark_chunk_as_written(chunk_id)
          chunk_start, chunk_size = @metadata.chunks.get_chunk_by_id(chunk_id)
          $Log.debug("   write_part returns chunk_id: #{chunk_id}")
          return chunk_id
        else
          $Log.debug('   Found NO Part with same part_id')
          chunk_id, chunk_start, chunk_size = @chunks.get_chunk(data.length)
          @metadata.set_part_hash(chunk_id, part_hash)
          $Log.debug("   write_part returns chunk_id: #{chunk_id}")
        end
      else
        $Log.debug('   Disabled Dublication')
        chunk_id, chunk_start, chunk_size = @chunks.get_chunk(data.length)
        $Log.debug("   write_part returns chunk_id: #{chunk_id}, start: #{chunk_start}, length: chunk_length")
      end
    end
    $Log.debug("   Start writing #{data.length} bytes at: #{chunk_start}")
    @filehandle.seek(chunk_start)
    length = @filehandle.write(data)
    $Log.debug("   Wrote #{length} bytes")
    if chunk_start + data.length != @filehandle.tell
      $Log.error("ERROR\n##########")
      $Log.error(data)
      $Log.error("------------")
      $Log.error(data.length)
      $Log.error("############")
      debugfile = File.new(@path + '.debugdata', 'w')
      debugfile.write(data)
      debugfile.close
      $Log.error("Calculated Data Length: #{data.length}")
      $Log.error("Real Data Length: #{@filehandle.tell - chunk_start}")
      $Log.fatal_error("   Calculated #{chunk_start + data.length} and real #{@filehandle.tell} endpoint do not match")
    end
    if length != chunk_size and not is_superblock
      $Log.fatal_error("   Cunk size #{chunk_size} does not fit with length #{length} of Data written!!")
    end
    #@filehandle.flush
    if is_superblock
      return
    else
      @metadata.chunks.mark_chunk_as_written(chunk_id)
      return chunk_id
    end
  end

  def overwrite_chunk(chunk_id, do_not_return_chunk = false)
    $Log.debug('AM: OVERWRITE CHUNK')
    start, length = @metadata.chunks.get_chunk_by_id(chunk_id)
    overwrite(start, length)
    unless do_not_return_chunk
      @chunks.return_chunk(chunk_id)
    end
  end

  def overwrite(start, length)
    $Log.debug('AM: OVERWRITE')
    $Log.debug("Overwriting start: #{start} length:#{length}")
    $Log.debug('Overwriting following Data:')
    $Log.debug(read(length, start))
    $Log.debug('#################')
    @filehandle.seek(start)
    $Log.debug("Data length: #{get_bytes(length, "+").length}")
    @filehandle.write(get_bytes(length, "+"))
  end

  def read(length, start=@readpos)
    $Log.debug('AM: READ')
    @filehandle.seek(start)
    data = @filehandle.read(length)
    @readpos = @filehandle.tell
    return data
  end

  def read_chunk(chunk_id)
    $Log.debug('AM: READ CHUNK')
    chunk_start, chunk_length = @metadata.chunks.get_chunk_by_id(chunk_id)
    $Log.debug("Reading #{chunk_length} bytes beginning at #{chunk_start}")
    return Compressor::restore(read(chunk_length, chunk_start))
    #return read(chunk_length, chunk_start)
  end

  def closed?
    return @closed
  end

  def size
    return File.size(@path)
  end

  def delete
    close
    if File.exists?(@path)
      File.delete(@path)
    end
  end

  def rename(new_path)
    close
    File.rename(@path, new_path)
    @path = new_path
    reopen
  end

end
