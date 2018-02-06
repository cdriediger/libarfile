#!/usr/bin/env ruby

require 'json'
require 'msgpack'
require_relative 'logger.rb'
require_relative 'ArchiveManager.rb'
require_relative 'MetadataManager.rb'
require_relative 'ChunkManager2.rb'

@path = File.absolute_path(ARGV[0])
$Log = Log.new(@path)
#$Log.quiet!
metadata = MetadataManager.new(@path)
puts JSON.pretty_generate(metadata.superblock)
puts "##############################"
puts JSON.pretty_generate(metadata)
