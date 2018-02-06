class Chunk < String

  def initialize(start, length, written, locked, id, metadata)
    $Log.debug("C:#{@id} INIT CHUNK")
    super(id)
    @id = id
    if written.kind_of?(TrueClass)
      written = 1
    elsif written.kind_of?(FalseClass)
      written = 0
    end
    if locked.kind_of?(TrueClass)
      $Log.fatal_error('Locked has to be a list of locking snaphot_ids')
    elsif locked.kind_of?(FalseClass)
      locked = []
    else
      locked = locked
    end
    if id == "0"
      $Log.debug("Initializing superblock chunk: #{id} start: #{start} length: #{length} written: #{written} locked: #{locked}")
    else
      $Log.debug("Initializing chunk: #{id} start: #{start} length: #{length} written: #{written} locked: #{locked}")
    end
    @metadata = metadata
    end_of_chunk = start + length
    if end_of_chunk > @metadata['EndOfArchive']
      $Log.debug("   Setting End_of_archive to #{end_of_chunk}")
      @metadata.set_end_of_archive(end_of_chunk)
    else
      $Log.debug("   NOT setting End_of_archive: #{@metadata['EndOfArchive']} next_byte: #{end_of_chunk}")
    end
    if @metadata['Chunks'].has_key?(@id)
      mstart, mlength, mwritten, mlocked = @metadata['Chunks'][@id]
      if start != mstart
        $Log.fatal_error("Chunk #{@id} start: #{start} != mstart: #{mstart}")
      end
      if length != mlength
        $Log.fatal_error("Chunk #{@id} length: #{length} != mlength: #{mlength}")
      end
      if written != mwritten
        $Log.fatal_error("Chunk #{@id} written: #{written} != mwritten: #{mwritten}")
      end
      if locked != mlocked
        $Log.fatal_error("Chunk #{@id} locked: #{locked} != mlocked: #{mlocked}")
      end
    else
      @metadata['Chunks'][@id] = [start, length, written, locked]
    end
    @deleted = false
  end

  def delete
    $Log.debug("C:#{@id} DELETE CHUNK")
    $Log.fatal_error("Cannot delete written Chunk. Written: #{@metadata['Chunks'][@id][2]}") if is_written?
    $Log.fatal_error("Cannot delete locked Chunk. locked: #{@metadata['Chunks'][@id][3]}") if is_locked?
    @metadata['Chunks'].delete(@id)
    @deleted = true
  end

  def is_deleted?
    return @deleted
  end

  def is_written?
    $Log.debug("C:#{@id} IS WRITTEN?")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    written = @metadata['Chunks'][@id][2] > 0
    locked = @metadata['Chunks'][@id][3].empty?
    result = written or not locked
    return result
  end

  def is_locked?
    $Log.debug("C:#{@id} IS LOCKED?")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    return !@metadata['Chunks'][@id][3].empty?
  end

  def start
    $Log.debug("C:#{@id} GET START")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    return @metadata['Chunks'][@id][0]
  end

  def length
    $Log.debug("C:#{@id} GET LENGTH")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    return @metadata['Chunks'][@id][1]
  end

  def written
    $Log.debug("C:#{@id} GET WRITTEN")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    return @metadata['Chunks'][@id][2]
  end

  def locked_by?
    $Log.debug("C:#{@id} GET LOCKED BY?")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    return @metadata['Chunks'][@id][3]
  end

  def set_written
    $Log.debug("C:#{@id} SET WRITTEN")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    @metadata['Chunks'][@id][2] += 1
  end

  def set_deleted
    $Log.debug("C:#{@id} SET DELETE")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    @metadata['Chunks'][@id][2] -= 1 if @metadata['Chunks'][@id][2] > @metadata['Chunks'][@id][3].length
  end

  def lock(snapshot_id)
    $Log.debug("C:#{@id} LOCK")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    @metadata['Chunks'][@id][3] << snapshot_id unless @metadata['Chunks'][@id][3].include?(snapshot_id)
  end

  def unlock(snapshot_id)
    $Log.debug("C:#{@id} UNLOCK")
    $Log.fatal_error("Chunk #{@id} is deleted") if @deleted
    @metadata['Chunks'][@id][3].delete(snapshot_id) if @metadata['Chunks'][@id][3].include?(snapshot_id)
  end

  attr_reader :id

end

class ChunkManager

  attr_reader :writtenChunks
  attr_reader :emptyChunks
  attr_reader :metadataChunks
  attr_reader :chunks

  def initialize(metadata)
    @metadata = metadata
    @writtenChunks = []
    @emptyChunks = []
    @emptyChunks_start = {}
    @emptyChunks_length = {}
	@lockedChunks = {} #{snaphot_id => [chunk_1, chunk_2, chunk_n]}
    @metadataChunks = []
    $Log.debug('Parsing Chunks:')
    @chunks = {}
    unless @metadata.has_key?('Chunks')
      @metadata['Chunks'] = Hash.new
      superblock_chunk = register_chunk("0", 0, 100, 1)
      @chunks[0] = superblock_chunk
      $Log.debug("Creating Superblock chunk: Start: 0 Length 1000")
    end
    $Log.debug("EndOfArchive: #{@metadata['EndOfArchive']}")
    # {CHUNK-ID => [start, length, written, [locking_snapshot_id1, locking_snapshot_idN]]}
    @metadata['Chunks'].each_pair do |chunk_id, chunk_data| # {CHUNK-ID => [start, length, written, locked]}
      chunk = register_chunk(chunk_id, chunk_data[0], chunk_data[1], chunk_data[2], chunk_data[3])
      @chunks[chunk_id] = chunk
      $Log.debug("Parsing Chunk: chunk_start: #{chunk.start}, chunk_length: #{chunk.length}, times_written: #{chunk.written}, locked: #{chunk.is_locked?}")
    end
    $Log.debug("Chunks: #{@chunks}")
  end

  def close
    $Log.debug('CM: CLOSE CHUNKMANAGER')
  end

  def set_metadata_chunk(chunk_start, chunk_end)
    $Log.debug('CM: SET METADATA CHUNK')
    $Log.debug("    chunk_start: #{chunk_start} | chunk_end: #{chunk_end}")
    chunk_id = get_new_chunk_id
    $Log.debug("    chunk_id: #{chunk_id}")
    chunk_length = chunk_end - chunk_start
    $Log.debug("    chunk_length: #{chunk_length}")
    chunk = register_chunk(chunk_id, chunk_start, chunk_length)
    @metadataChunks << chunk
  end

  def get_chunk(data_length)
    $Log.debug("CM: GET CHUNK LENGTH: #{data_length}")
    chunk = find_empty_chunk_with_length_min(data_length)
    if not chunk
      $Log.debug("   No chunk with min length #{data_length} found. Creating new chunk")
      chunk = get_new_chunk(data_length)
      $Log.debug("   Chunk_ID: #{chunk}")
      return [chunk, chunk.start, chunk.length]
    end
    if chunk.length > data_length
      chunk = shorten_empty_chunk(chunk, data_length)
    end
    $Log.debug("   Returning Empty_Chunk #{chunk} Length: #{chunk.length} Start: #{chunk.start}")
    return [chunk, chunk.start, chunk.length]
  end

  def get_metadata_chunk(length)
    $Log.debug('CM: GET METADATA CHUNK')
    chunk, chunk_start, chunk_length = get_chunk(length)
    @metadataChunk = chunk
    mark_chunk_as_written(chunk)
    return [chunk, chunk_start, chunk_length]
  end

  def get_superblock_chunk
    return [@chunks['0'], @chunks['0'].start, @chunks['0'].length]
  end

  def get_chunk_by_id(chunk_id)
    unless @chunks.has_key?(chunk_id)
      $Log.fatal_error("Chunk #{chunk_id} not found")
    end
    return [@chunks[chunk_id].start, @chunks[chunk_id].length]
  end
  
  def get_chunks_locked_by(snapshot_id)
	return @lockedChunks[snapshot_id]
  end

  def return_chunk(chunk_id)
    $Log.debug("CM: RETURN CHUNK #{chunk_id}")
    chunk = @chunks[chunk_id]
    unless chunk.is_written?
      $Log.fatal_error('!!!RETURNING Unwritten CHUNK!!!')
    end
    transform_to_empty_chunk(chunk)
    unless chunk.is_locked?
        chunk = combine_with_previous_chunk_if_empty(chunk)
        chunk = combine_with_next_chunk_if_empty(chunk)
    end
  end

  def lock_chunk(snapshot_id, chunk)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @metadata.is_editable?
    $Log.debug('CM: LOCK CHUNK')
    chunk = @chunks[chunk] unless chunk.kind_of?(Chunk)
	@lockedChunks[snapshot_id] << chunk.id
    chunk.lock(snapshot_id)
  end

  def unlock_chunk(snapshot_id, chunk)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @metadata.is_editable?
    $Log.debug('CM: UNLOCK CHUNK')
    chunk = @chunks[chunk] unless chunk.kind_of?(Chunk)
	@lockedChunks[snapshot_id].delete(chunk.id)
    chunk.unlock(snapshot_id)
  end

  def mark_chunk_as_written(chunk)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @metadata.is_editable?
    $Log.debug('CM: MARK CHUNK AS WRITTEN')
    chunk = @chunks[chunk] unless chunk.kind_of?(Chunk)
    transform_to_written_chunk(chunk)
  end

  def chunk_is_last_chunk?(chunk)
    $Log.debug("CM: CHUNK #{chunk} IS LAST CHUNK?")
    chunk = @chunks[chunk] unless chunk.kind_of?(Chunk)
    chunk_end = chunk.start + chunk.length
    if chunk_end == @metadata['EndOfArchive']
      $Log.debug('   Chunk is last Chunk')
      return true
    elsif chunk_end > @metadata['EndOfArchive']
      $Log.fatal_error('Chunk end after EndOfArchive')
    else
      $Log.debug('   Chunk is NOT last Chunk')
      return false
    end
  end

  def combine_with_next_chunk_if_empty(chunk)
    $Log.debug('CM: COMBINE WITH NEXT CHUNK ID EMPTY')
    chunk = @chunks[chunk] unless chunk.kind_of?(Chunk)
    next_chunk_start = chunk.start + chunk.length
    if @emptyChunks_start.include?(next_chunk_start)
      next_chunk = @emptyChunks_start[next_chunk_start]
      $Log.debug("   Post Found Chunk #{next_chunk}: Start: #{next_chunk.start}, Length: #{next_chunk.length} || Start: #{chunk.start}, Length: #{chunk.length}")
      chunk = combine_chunks(chunk, next_chunk)
    end
    return chunk
  end

  def combine_with_previous_chunk_if_empty(chunk)
    $Log.debug('CM: COMBINE WITH previous CHUNK ID EMPTY')
    chunk = @chunks[chunk] unless chunk.kind_of?(Chunk)
    $Log.debug("Empty Chunks: #{@emptyChunks}")
    @emptyChunks.each do |pre_chunk|
      next if pre_chunk == chunk
      $Log.debug("   Testing Chunk #{pre_chunk}")
      if pre_chunk.start + pre_chunk.length == chunk.start
        $Log.debug("   Pre Found Chunk #{pre_chunk}: Start: #{pre_chunk.start}, Length: #{pre_chunk.length} || Start: #{chunk.start}, Length: #{chunk.length}")
        chunk = combine_chunks(pre_chunk, chunk)
        break
      end
    end
    return chunk
  end

  def combine_chunks(chunk1, chunk2)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @metadata.is_editable?
    $Log.debug('CM: COMBINE CHUNKS')
    chunk1 = @chunks[chunk1] unless chunk1.kind_of?(Chunk)
    chunk1_start = chunk1.start
    chunk1_length = chunk1.length
    chunk2 = @chunks[chunk1] unless chunk2.kind_of?(Chunk)
    chunk2_start = chunk2.length
    chunk2_length = chunk2.length
    $Log.debug("   Combining Chunks: #{chunk1} (#{chunk1.start} | #{chunk1.length}) | #{chunk2} (#{chunk2.start} | #{chunk2.length})")
    $Log.debug("   Deleting chunks #{chunk1} | #{chunk2}")
    unless @emptyChunks.include?(chunk1)
      $Log.error("   chunk1: #{chunk1} not found in @emptyChunks")
      $Log.fatal_error(@emptyChunks)
    end
    @emptyChunks.delete(chunk1)
    @emptyChunks_start.delete(chunk1_start)
    @emptyChunks_length.delete(chunk1_length)
    @chunks.delete(chunk1)
    chunk1.delete
    unless @emptyChunks.include?(chunk2)
      $Log.error("   chunk2: #{chunk2} not fund in @emptyChunks")
      $Log.fatal_error(@emptyChunks)
    end
    @emptyChunks.delete(chunk2)
    @emptyChunks_start.delete(chunk2_start)
    @emptyChunks_length.delete(chunk2_length)
    @chunks.delete(chunk2)
    chunk2.delete
    new_chunk_id = get_new_chunk_id
    new_chunk_length = chunk1_length + chunk2_length
    $Log.debug("   Creating Chunk #{new_chunk_id} Start: #{chunk1_start} Length: #{new_chunk_length}")
    new_chunk = register_chunk(new_chunk_id, chunk1_start, new_chunk_length)
    return new_chunk
  end

  def find_empty_chunk_with_length_min(data_length)
    $Log.debug('CM: FIND EMPTY CHUNK WITH MIN LENGTH')
    matching_chunks = {}
    $Log.debug("   emptyChunks: #{@emptyChunks}")
    @emptyChunks.each do |chunk|
      if chunk.length >= data_length
        matching_chunks[chunk.length] = chunk
      end
    end
    return matching_chunks[matching_chunks.keys.sort[0]]
  end

  def shorten_empty_chunk(chunk, data_length)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @metadata.is_editable?
    $Log.debug("CM: SHORTEN CHUNK: #{chunk} from #{chunk.length} to #{data_length}")
    chunk_one_id = chunk.id
    chunk_one_start = chunk.start
    chunk_one_length = data_length
    chunk_two_id = get_new_chunk_id
    chunk_two_start = chunk.start + data_length
    chunk_two_length = chunk.length - chunk_one_length
    @chunks.delete(chunk)
    @emptyChunks.delete(chunk)
    @emptyChunks_start.delete(chunk.start)
    @emptyChunks_length.delete(chunk.length)
    chunk.delete
    chunk_one = register_chunk(chunk_one_id, chunk_one_start, chunk_one_length)
    chunk_two = register_chunk(chunk_two_id, chunk_two_start, chunk_two_length)
    return chunk_one
  end

  def get_new_chunk(data_length)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @metadata.is_editable?
    $Log.debug('CM: NEW CHUNK')
    chunk_id = get_new_chunk_id
    chunk_start = @metadata['EndOfArchive']
    chunk_length = data_length
    chunk = register_chunk(chunk_id, chunk_start, chunk_length)
    return chunk
  end

  def register_chunk(chunk_id, start, length, written = 0, locked = [])
    $Log.debug('CM: REGISTER CHUNK')
    $Log.debug("   Register Chunk: #{chunk_id}, #{start}, #{length}, #{written}, #{locked}")
    if written.kind_of?(TrueClass)
      written = 1
    elsif written.kind_of?(FalseClass)
      written = 0
    end
    if locked.kind_of?(TrueClass)
      $Log.fatal_error('Locked can not be true')
    elsif locked.kind_of?(FalseClass)
      locked = []
    end
	unless locked.empty?
	  locked.each do |snaphot_id|
	    @lockedChunks[snaphot_id] << chunk_id
	  end
	end
    chunk = Chunk.new(start, length, written, locked, chunk_id, @metadata)
    @chunks[chunk_id] = chunk
    if written == 0
      @emptyChunks << chunk
      @emptyChunks_start[chunk.start] = chunk
      @emptyChunks_length[chunk.length] = chunk
    else
      @writtenChunks << chunk
    end
    return chunk
  end

  def get_new_chunk_id
    $Log.debug('CM: GET NEW CHUNK ID')
    new_chunk_id = @metadata['last_chunk_id'] + 1
    @metadata['last_chunk_id'] = new_chunk_id
    return new_chunk_id.to_s
  end

  def transform_to_empty_chunk(chunk)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @metadata.is_editable?
    $Log.debug('CM: TRANSFORM TO EMPTY CHUNK')
    chunk = @chunks[chunk] unless chunk.kind_of?(Chunk)
    chunk.set_deleted
    $Log.debug("   Chunk #{chunk} is still written.") if chunk.is_written?
    $Log.debug("   Chunk #{chunk} is locked.") if chunk.is_locked?
    unless chunk.is_written? or chunk.is_locked?
      $Log.debug("   Can transform to empty chunk")
      @writtenChunks.delete(chunk)
      @emptyChunks << chunk
      @emptyChunks_start[chunk.start] = chunk
      @emptyChunks_length[chunk.length] = chunk
    else
      $Log.debug("   Can NOT transform to empty chunk. Chunk is still writte or locked")
    end
    $Log.debug("   Chunk #{chunk} is #{chunk.written} times written")
  end

  def transform_to_written_chunk(chunk)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @metadata.is_editable?
    $Log.debug('CM: TRANSFORM TO written CHUNK')
    chunk = @chunks[chunk] unless chunk.kind_of?(Chunk)
    chunk.set_written
    @emptyChunks.delete(chunk)
    @emptyChunks_start.delete(chunk.start)
    @emptyChunks_length.delete(chunk.length)
    @writtenChunks << chunk
    $Log.debug("   Chunk #{chunk} is #{chunk.written} times written")
  end

end
