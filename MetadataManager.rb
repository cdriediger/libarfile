require 'elif'

class MetadataManager < Hash

  attr_accessor :metadata_start_pos
  attr_accessor :metadata_stop_pos
  attr_accessor :writable_data
  attr_accessor :chunks

  def initialize(path)
    $Log.info('Initializing MetadataManager')
    @path = path
    @archivefilename = @path
    @backupfilename = path + '.backup'
    @backupfile = nil
    @metadata_start_pos = nil
    @metadata_stop_pos = nil
    @superblock_start_pos = nil
    @superblock_stop_pos = nil
    @empty_chunks = []
    @changed = false
    @editable = false
    @archive = nil
    @initial_metadata_is_overwritten = false
    @found_init_metadata = false
    @found_init_superblock = false
    if File.exist?(@backupfilename)
      $Log.debug('   Found Metadata Backup')
      self.merge!(restore_metadata_backup)
      commit
    else
      superblock = parse_superblock
      if superblock
        $Log.debug('   Found superblock')
        $Log.debug(superblock)
        @metadata_start_pos = superblock['Start']
        @metadata_stop_pos = superblock['Stop']
      end
      parsed_metadata = parse_metadata
      if parsed_metadata
        $Log.debug('   Found Metadata')
        self.merge!(parsed_metadata)
        $Log.debug(self['Chunks'])
      end
    end
    if self.empty?
      @new = true
    else
      @new = false
    end
    self['FileHashes'] = Hash.new unless self.has_key?('FileHashes') # {MD5-Hash:FILE-ID}
    self['PartHashes'] = Hash.new unless self.has_key?('PartHashes') # {MD5-Hash:CHUNK-ID}
    self['Files'] = Hash.new unless self.has_key?('Files')
    # {FILE-ID:{"Path":PATH,"md5":MD5-HASH,"dedup":bool,"container":containername,"Parts":{PART_ID:CHUNK-ID]}}}
    self['Chunks'] = Hash.new unless self.has_key?('Chunks')
    # {CHUNK-ID => [start, length, written, [locking_snapshot_id1, locking_snapshot_idN]]}
    self['EndOfArchive'] = 0   unless self.has_key?('EndOfArchive')
    self['Snapshots'] = Hash.new unless self.has_key?('Snapshots') # {"created" => Time, "Metadata" => Metadata_json}
    self['Container'] = Hash.new unless self.has_key?('Container') # {'container_name' => [FILE-IDs]}
    self['last_chunk_id'] = 0 unless self.has_key?('last_chunk_id')
    @final_metadata = self.clone
    @chunks = ChunkManager.new(self)
    @chunks.set_metadata_chunk(@metadata_start_pos, @metadata_stop_pos) if @found_init_metadata
    @chunks.set_metadata_chunk(@superblock_start_pos, @superblock_stop_pos) if @found_init_superblock
  end

  def new?
    return @new
  end

  def set_archive(archive)
    $Log.debug('MM: SET ARCHIVE')
    @archive = archive
  end

  def close
    $Log.debug('MM: CLOSE METADATAMAGER')
    $Log.info('   Closing MetadataManager')
    if @backupfile
      @backupfile.close
    end
    if @editable
      $Log.info('   Writing final Metadata to Archive')
      write_to_file
    end
    unless @changed
      $Log.info('   Deleting Metadata Backup')
      if File.exist?(@backupfilename)
        File.delete(@backupfilename)
      end
    else
      $Log.info('   Maybe not all metadata changes are saved. Leaving Backup')
    end
  end

  def deactivate
    @archive = nil
    @changed = false
    @editable = false
    if File.exist?(@backupfilename) and @backupfile
      File.delete(@backupfilename)
    end
  end

  def get_writable_data
    $Log.debug('MM: GET WRITABLE DATA')
    writable_data = @final_metadata.clone
    writable_data['create_time'] = Time.now.to_f
    writable_data.delete('EndOfArchive')
    writable_data.delete('PartHashes')
    return writable_data
  end

  def enable_edit
    $Log.debug('MM: ENABLE EDIT')
    unless @editable
      $Log.debug('    Enable metadata editing')
      overwrite_in_archive
      @editable = true
    else
      $Log.debug('    Editing metadata already enabled')
    end
  end

  def is_editable?
    return @editable
  end

  def commit
    $Log.debug('MM: COMMIT')
    @final_metadata = self.clone
    @changed = false
  end

  def new_file(filepath, comp_method, filehash, dedub, file_id = nil)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @editable
    $Log.debug('MM: NEW FILE')
    @changed = true
    if file_id
      $Log.debug('  Got File_id')
      if self['Files'].keys.include?(file_id)
        $Log.debug('File_id already in use. Taking next one')
        file_id = get_next_file_id
     end
    else
      $Log.debug('  Got NO File_id.')
      file_id = get_next_file_id
    end
    $Log.debug("  Using File_id: #{file_id}")
    write_backup('new_file', file_id)
    filepath.gsub!('//', '/')
    self['FileHashes'][filehash] = file_id
    self['Files'][file_id] = Hash.new
    self['Files'][file_id]['Path'] = filepath
    self['Files'][file_id]['md5'] = filehash
    self['Files'][file_id]['dedup'] = dedub
    self['Files'][file_id]['container'] = false
    self['Files'][file_id]['Parts'] = Hash.new
    return file_id
  end

  def get_next_file_id
    #return (self['Files'].keys.sort[-1].to_i + 1).to_s
    return Usid.usid
  end

  def delete_file(id)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @editable
    $Log.debug('MM: DELETE FILE')
    @changed = true
    write_backup('delete_file', id)
    self['Files'].delete(id)
    self['FileHashes'].each do |hash, file_id|
      self['FileHashes'].delete(hash) if file_id == id
    end
  end

  def new_part(file_id, part_id, chunk_id)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @editable
    $Log.debug('MM: NEW PART')
    @changed = true
    self['Files'][file_id]['Parts'][part_id] = chunk_id
  end

  def set_part_hash(chunk_id, part_hash)
    $Log.debug('MM: SET PART HASH')
    self['PartHashes'][part_hash] = chunk_id
  end

  def create_snapshot(snapshot_id = Usid.usid, time = Time.now.to_f, files = self['Files'].clone)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @editable
    $Log.debug('MM: CREATE SNAPSHOT')
    @changed = true
    write_backup('create_snapshot', snapshot_id)
    self['Snapshots'][snapshot_id] = {'Created' => time, 'Files' => files}
    @chunks.writtenChunks.each do |chunk_id|
      @chunks.lock_chunk(snapshot_id, chunk_id)
    end
    return snapshot_id
  end

  def rollback_snapshot(snapshot_id)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @editable
    $Log.debug('MM: ROLLBACK SNAPSHOT')
    @changed = true
    write_backup('rollback_snapshot', snapshot_id)
    snapshot_files = self['Snapshots'][snapshot_id]['Files']
    self['Files'] = snapshot_files
  end

  def delete_snapshot(snapshot_id)
    $Log.fatal_error('!!Metadata NOT Editable!!') unless @editable
    $Log.debug('MM: DELETE SNAPSHOT')
    @changed = true
    write_backup('delete_snapshot', snapshot_id)
    chunklist = @chunks.lockedChunks[snapshot_id].clone
    chunklist.each do |chunk_id|
      @chunks.unlock_chunk(snapshot_id, chunk_id)
    end
    self['Snapshots'].delete(snapshot_id)
  end

  def create_container(container_name)
    self['Container'][container_name] = []
  end

  def delete_container(container_name)
    self['Container'].delete(container_name)
  end

  def add_to_container(container_name, file_id)
    create_container(container_name) unless self['Container'].keys.include?(container_name)
    if self['Files'][file_id]['container']
      self['Container'][self['Files'][file_id]['container']].delete(file_id)
    end
    self['Container'][container_name] << file_id
    self['Files'][file_id]['container'] = container_name
  end

  def delete_from_container(file_id)
    if self['Files'].has_key?(file_id)
      if self['Files'][file_id]['container']
        self['Container'][self['Files'][file_id]['container']].delete(file_id)
        self['Files'][file_id]['container'] = false
      end
    end
  end

  def set_end_of_archive(end_of_archive)
    $Log.debug('MM: SET END OF ARCHIVE')
    $Log.debug("   Set end of archive to: #{end_of_archive}")
    self['EndOfArchive'] = end_of_archive
  end

  def files_by_name(filename)
    $Log.debug('MM: FILES BY NAME')
    files = []
    self['Files'].each do |file_id, file_data|
      path = file_data('Path')
      files << [file_id, path] if path.split('/')[-1].include?(filename)
    end
    return files
  end

  def file_by_path(filepath)
    $Log.debug('MM: FILES BY PATH')
    filepath = normalize_path(filepath)
    self['Files'].each do |file_id, file_data|
      return file_data('Path') if file_data('Path') == filepath
    end
  end

  def files_by_container(containername)
    files = {}
    if self['Container'].has_key?(containername)
      self['Container'][containername].each do |file_id|
        files[file_id] = self['Files'][file_id]
      end
    end
    return files
  end

  def normalize_path(path)
    path.gsub!('//', '/')
    if path[0] == '/'
      path = path[1..-1]
    end
    return path
  end

  def write_to_file(archive = @archive)
    $Log.debug('MM: WRITE TO FILE NEW')
    $Log.debug("   Appending Metadata to #{@path}")
    metadata = get_writable_data
    if metadata
      begin
        metadata_final = metadata.to_msgpack
        $Log.debug("############")
        $Log.debug(metadata_final)
        $Log.debug("############")
      rescue RangeError
        $Log.fatal_error(JSON.generate(self))
      end
      metadata_hash = Digest::MD5.hexdigest(metadata_final)
      chunk_id = archive.write_part(metadata_final, debup = false)
      $Log.debug("MM: Wrote Metadata at Chunk #{chunk_id}")
      start, size = @chunks.get_chunk_by_id(chunk_id)
      @metadata_start_pos = start
      @metadata_stop_pos = start + size
      superblock = {'Hash' => metadata_hash,
                      'Start' => @metadata_start_pos,
                      'Stop' => @metadata_stop_pos}.to_msgpack
      superblock.insert(0, %Q< \n>)
      $Log.debug("MM: Superblock: #{superblock}")
      archive.write_part(superblock, debup = false, is_superblock = true)
      $Log.debug("MM: Wrote Superblock at End of Archive")
    end
  end

  def write_backup(function_name, object_id)
    $Log.debug('MM: WRITE METADATA BACKUP')
    $Log.debug("function_name: #{function_name}, object_id: #{object_id}")
    metadata = self.clone
    metadata['current_task'] = [function_name, object_id]
    metadata['create_time'] = Time.now.to_f
    begin
      metadata_msgpack = metadata.to_msgpack
    rescue RangeError, NoMethodError
      $Log.fatal_error(JSON.generate(metadata))
    end
    @backupfile = File.open(@backupfilename, 'w') unless @backupfile
    @backupfile.seek(0)
    @backupfile.write(metadata_msgpack)
  end

  def restore_metadata_backup
    $Log.info('MM: RESTORE METADATA BACKUP')
    backupfile = File.open(@backupfilename, 'r')
    metadata_backup = MessagePack.unpack(backupfile.read)
    metadata_archive = parse_metadata
    if metadata_archive
      if metadata_backup['create_time'] > metadata_archive['create_time']
        $Log.info('MM: Backup Metadata is newer. Using Backup Metadata')
        metadata = metadata_backup
      else
        $Log.info('MM: Archive Metadata is newer. Using Archive Metadata')
        metadata = metadata_archive
      end
    else
      $Log.info('MM: No Archive Metadata found. Using Backup Metadata')
      metadata = metadata_backup
    end
    if metadata.include?('current_task')
      current_task, current_task_data = metadata['current_task']
      $Log.info("current_task: #{current_task}, current_task_data: #{current_task_data}")
      metadata.delete('current_task')
      if current_task == "new_file"
        $Log.info("Deleting uncomplete added file. File_ID: #{current_task_data}")
        metadata['FileHashes'].each do |filehash, file_id|
          if file_id == current_task_data
            metadata['FileHashes'][filehash].delete(filehash)
          end
        end
        if metadata['Files'].include?(current_task_data)
          metadata['Files'][current_task_data]['Parts'].each do |part_id, chunk_id|
            metadata['Chunks'][chunk_id][2] == false
          end
          metadata['Files'].delete(current_task_data)
        end
      elsif current_task == 'create_snapshot'
        metadata['Snapshots'].delete(current_task_data)
        metadata['Chunks'].each_key do |chunk_id|
          if metadata['Chunks'][chunk_id][3].include?(current_task_data)
            metadata['Chunks'][chunk_id][3].delete(current_task_data)
          end
        end
      elsif current_task == 'rollback_snapshot'

      elsif current_task == 'delete_snapshot'

      elsif current_task == 'delete_file'
        if metadata['Files'].include?(current_task_data)
          metadata['Files'].delete(current_task_data)
        end
        metadata['FileHashes'].each do |hash, file_id|
          if file_id == current_task_data
            metadata['FileHashes'].delete(hash)
          end
        end
      end
    else
      $Log.info('Could not find current_task')
    end
    $Log.info('Restored Metadata:')
    $Log.info(JSON.pretty_generate(metadata))
    return metadata
  end

  def parse_metadata
    $Log.debug('MM: PARSE METADATA NEW')
    $Log.debug('   Parsing Metadata..')
    begin
      metadatafileobj = File.open(@archivefilename)
    rescue Errno::ENOENT
      return false
    end
    metadatafileobj.seek(@metadata_start_pos)
    metadata_length = @metadata_stop_pos - @metadata_start_pos
    metadata_msp = Compressor.restore(metadatafileobj.read(metadata_length))
    metadata = MessagePack.unpack(metadata_msp)
    $Log.debug(metadata)
    return metadata
  end

  def parse_metadata_old
    $Log.debug('MM: PARSE METADATA NEW')
    $Log.debug('   Parsing Metadata..')
    begin
      metadatafileobj = Elif.open(@archivefilename)
    rescue Errno::ENOENT
      return false
    end
    start_magicword = %Q<//START_METADATA:>
    end_magicword = %Q<:END_METADATA\\ \n>
    found_end = false
    @metadata_lines = []
    metadatafileobj.each_line do | line |
      if line.end_with?(end_magicword)
        @metadata_lines << line
        found_end = true
        if line.start_with?(start_magicword)
          @found_init_metadata = true
          break
        end
      elsif line.start_with?(start_magicword) and found_end
        @metadata_lines << line
        @found_init_metadata = true
        break
      elsif found_end
        @metadata_lines << line
      end
    end
    metadatafileobj.close
    @metadata_msp = @metadata_lines.reverse.join("")[17..-17]
    if @found_init_metadata
      $Log.debug('   Found Metadata')
      $Log.debug(@metadata_msp)
      $Log.debug(@metadata_msp.class)
      metadata = MessagePack.unpack(@metadata_msp)
    end
    return metadata
  end

  def parse_superblock
    $Log.debug('MM: PARSE SUPERBLOCK')
    $Log.debug('   Parsing Superblock..')
    begin
      metadatafileobj = Elif.open(@archivefilename)
    rescue Errno::ENOENT
      return false
    end
    superblock_msp = metadatafileobj.readline
    $Log.debug('   Found Superblock')
    $Log.debug(superblock_msp)
    superblock = MessagePack.unpack(superblock_msp)
    return superblock
  end

  def overwrite_initial_metadata(archive = @archive)
    $Log.debug('MM: OVERWRITE INITIAL METADATA')
    if not @initial_metadata_is_overwritten and archive and @found_init_metadata
      if @metadata_start_pos and @metadata_stop_pos
        start = @metadata_start_pos
        length = (@metadata_stop_pos - start) + 18
        $Log.debug("   Overwriting Initial Metadata: Start: #{start}, length: #{length}")
        archive.overwrite(start, length)
        @initial_metadata_is_overwritten = true
      else
        $Log.debug('   No initial Metadata')
      end
    end
  end

  def overwrite_in_archive(archive = @archive)
    $Log.debug('MM: OVERWRITE IN ARCHIVE')
    unless self.chunks.metadataChunks.empty?
      self.chunks.metadataChunks.each do |chunk_id|
        $Log.debug("   Overwriting Metadata Chunk: #{chunk_id}")
        archive.overwrite_chunk(chunk_id, do_not_return_chunk = true)
      end
    else
      $Log.debug('   No Metadata Chunk found')
    end
  end

end