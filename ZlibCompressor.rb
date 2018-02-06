module Compressor

  require 'zlib'

  def self.compress(data)
    return Zlib::Deflate.deflate(data)
  end

  def self.restore(comp_data)
    Zlib::Inflate.inflate(comp_data)
  end

end