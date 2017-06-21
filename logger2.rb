require 'logging'

class Logging::Logger

  def fatal_error(msg)
    error(msg)
    Kernel.exit!
  end

end
