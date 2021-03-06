require 'logger'

class Log

  def initialize(path)
    if path
      logfiles = Dir[path + "*.log"]
      logfiles.map! {|path| /\d{1,3}/.match(path).to_s.to_i}
      if logfiles.empty?
        @Logfile = path + ".1.log"
      else
        @Logfile = path + "." + (logfiles.sort[-1] + 1).to_s + ".log"
      end
      @logger = Logger.new(@Logfile)
    else
      @logger = nil
    end
	@quiet = false
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
    puts(msg)
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
