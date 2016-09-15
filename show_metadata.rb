#!/usr/bin/env ruby

require 'json'
require 'msgpack'
require './logger.rb'
require './ArchiveManager.rb'
require './MetadataManager.rb'
require './ChunkManager2.rb'

@path = File.absolute_path(ARGV[0])
$Log = Log.new(@path)
$Log.quiet!
metadata = MetadataManager.new(@path)
puts JSON.pretty_generate(metadata)
