module Compressor

  require 'lzma'

  def self.compress(data)
    return LZMA.compress(data)
  end

  def self.restore(comp_data)
    LZMA.decompress(comp_data)
  end

end