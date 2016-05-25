require 'logger'

class Log

  def initialize(path)
    logfiles = Dir[path + ".debug.*"]
    logfiles.map! {|path| /\d{1,3}/.match(path).to_s.to_i}
    if logfiles.empty?
      @Logfile = path + ".debug.1"
    else
      @Logfile = path + ".debug." + (logfiles.sort[-1] + 1).to_s
    end
    puts "Logfile: #{@Logfile}"
    @logger = Logger.new(@Logfile)
  end

  def level=(loglevel)
    @logger.level = loglevel
  end

  def debug(msg)
    @logger.debug(msg)
#    if $print_debug
#    puts(msg)
#    end
  end

  def info(msg)
    @logger.info(msg)
    puts(msg)
  end

  def error(msg)
    @logger.error(msg)
    puts(msg)
  end

  def fatal_error(msg)
    begin
      error("FATAL ERROR: " + msg)
      raise ArfileError, msg
    rescue => e
      puts "=============================="
      puts e.message
      puts "=============================="
      puts e.backtrace
      puts "=============================="
    end
    Kernel.exit!
  end
end

class ArfileError < StandardError

end
