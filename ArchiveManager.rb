require_relative 'ZlibCompressor'

class Fixnum
  def num_digits
    Math.log10(self).to_i + 1
  end
end


class ArchiveManager

  attr_reader :chunksize
  attr_reader :ReadPos
  attr_reader :end_of_archive

  def initialize(path, metadata)
    @path = path
    @metadata = metadata
    @filemanager = FileManager.new(@path)
    @filemanager.enable_write
    if @metadata.superblock.found?
      @metadata.set_end_of_archive(@filemanager.get_end_of_file - @metadata.superblock.length)
    end
  end

  def close
    @filemanager.close
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
      #chunk_start = @filemanager.get_end_of_file
      chunk, chunk_start, chunk_length = @metadata.chunks.get_superblock_chunk
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
          chunk_id, chunk_start, chunk_size = @metadata.chunks.get_chunk(data.length)
          @metadata.set_part_hash(chunk_id, part_hash)
          $Log.debug("   write_part returns chunk_id: #{chunk_id}")
        end
      else
        $Log.debug('   Disabled Dublication')
        chunk_id, chunk_start, chunk_size = @metadata.chunks.get_chunk(data.length)
        $Log.debug("   write_part returns chunk_id: #{chunk_id}, start: #{chunk_start}, length: chunk_length")
      end
    end
    $Log.debug("   Start writing #{data.length} bytes at: #{chunk_start}")
    length = @filemanager.write(data, chunk_start)
    $Log.debug("   Wrote #{length} bytes")
    if chunk_start + data.length != @filemanager.get_position
      $Log.error("ERROR\n##########")
      $Log.error(data)
      $Log.error("------------")
      $Log.error(data.length)
      $Log.error("############")
      debugfile = FileManager.new(@path + '.debugdata', 'w')
      debugfile.write(data, 0)
      debugfile.close
      $Log.error("Calculated Data Length: #{data.length}")
      $Log.error("Real Data Length: #{@filemanager.get_position - chunk_start}")
      $Log.fatal_error("   Calculated #{chunk_start + data.length} and real #{@filemanager.get_position} endpoint do not match")
    end
    if length != chunk_size and not is_superblock
      $Log.fatal_error("   Cunk size #{chunk_size} does not fit with length #{length} of Data written!!")
    end
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
      @metadata.chunks.return_chunk(chunk_id)
    end
  end

  def overwrite(start, length)
    $Log.debug('AM: OVERWRITE')
    $Log.debug("Overwriting start: #{start} length:#{length}")
    $Log.debug('Overwriting following Data:')
    $Log.debug(read(length, start))
    $Log.debug('#################')
    $Log.debug("Data length: #{get_bytes(length, "+").length}")
    @filemanager.write(get_bytes(length, "+"), start)
  end

  def read(length, start=nil)
    $Log.debug('AM: READ')
    return @filemanager.read(length, start)
  end

  def read_chunk(chunk_id)
    $Log.debug('AM: READ CHUNK')
    chunk_start, chunk_length = @metadata.chunks.get_chunk_by_id(chunk_id)
    $Log.debug("Reading #{chunk_length} bytes beginning at #{chunk_start}")
    return Compressor::restore(read(chunk_length, chunk_start))
    #return read(chunk_length, chunk_start)
  end

  def closed?
    return @filemanager.closed?
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
    @filemanager.rename(new_path)
  end

end
