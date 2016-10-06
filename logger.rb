require 'logger'

class Log

  def initialize(path)
    if path
      logfiles = Dir[path + ".debug.*"]
      logfiles.map! {|path| /\d{1,3}/.match(path).to_s.to_i}
      if logfiles.empty?
        @Logfile = path + ".debug.1"
      else
        @Logfile = path + ".debug." + (logfiles.sort[-1] + 1).to_s
      end
      @logger = Logger.new(@Logfile)
    else
      @logger = nil
    end
  end

  def quiet!
    @quiet = true
  end

  def level=(loglevel)
    @logger.level = loglevel if @logger
  end

  def debug(msg)
    return if @quiet
    @logger.debug(msg) if @logger
#    if $print_debug
#    puts(msg)
#    end
  end

  def info(msg)
    return if @quiet
    @logger.info(msg) if @logger
    puts(msg)
  end

  def error(msg)
    return if @quiet
    @logger.error(msg) if @logger
    puts(msg)
  end

  def fatal_error(msg)
    return if @quiet
    begin
      error("FATAL ERROR: " + msg)
      raise ArfileError, msg
    rescue => e
      puts "=============================="
      puts e.backtrace
      puts "=============================="
    end
    Kernel.exit!
  end
end

class ArfileError < StandardError

end
