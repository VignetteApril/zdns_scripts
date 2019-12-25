#!/usr/local/bin/ruby
require 'json'
require 'socket'
require 'httparty'
require 'log4r'
require 'ipaddress'
require 'set'
require 'mysql2'
require 'singleton'

##################### 使用说明 （版本：2018-06-29）####################
# 1）本脚本提供和深澜、城市热点的对接功能，监听 61440 的UDP端口，产生的日志为
#    /usr/local/acl_monitor000001.log，启动后的进程名称为: zdns:cms_aclmonitor
# 2）使用本脚本前，需要按照如下参数配置说明来配置参数，以便正确运行。
# 3）本脚本只需要部署到网管，执行后变为守护进程，在后台执行。下次执行时，如果
#    发现已经启动，则忽略本次执行，如果想每次执行都执行的是重启操作，可以在执行
#    时，添加参数 '-k' 如: "/usr/bin/AclMonitor.rb -k"。
# 4）进程如果现网部署时，需要持续运行，并防止退出，则需要增加监控：
#    1>> 修改AclMonitor.rb 为可执行权限。
#    2>> 把脚本拷贝到目录/usr/bin/
#    3>> 执行脚本后，会自动把脚本添加入监控
#    4>> 打开/usr/bin/zdns_monitor 脚本，检查是否最后一行有且仅有刚新增的一行监控代码
##################### 配置参数 ########################################
#1）用户名/密码，当前台密码变更为非默认的密码时，这里需要同步变更。
USER_NAME = "admin"
PASS_WORD = "admin"
#2）是否开启调试日志，取值范围（true/false），正常运行时不需要开启。
LOG_LEVEL_DEBUG = false
#3）acl的刷新间隔，输入范围(10 ~ 300)
ACL_REFRESH_INTERVAL = 10
#4) 针对长春光华学院查数据库的需求{INTERN_PACKAGE_NAME1 => acl_name1, INTERN_PACKAGE_NAME2 => acl_name}
INTERNET_PACKAGE_NAME_ACL_NAME_MAP = { '电信单带宽30M12月' => '电信单带宽', '学生20M2年' => '学生20M', '移动单带宽50M1月' => '移动单带宽','学生20M3年' => '学生20M','移动单带宽50M5月' => '移动单带宽','办公专用' => '办公专用','补偿20M' => '学生20M','电信单带宽6M1月'=> '电信单带宽','学生10M12个月'=> '电信单带宽','联通单带宽20M1月'=> '联通单带宽','联通单带宽10M5月'=> '联通单带宽' }
#5) 当RUN_MODE 为4时链接目标数据库的账号密码和host以及数据库
MYSQL_HOST       = '10.1.107.15'
MYSQL_USER_NAME  = 'root'
MYSQL_PASSWORD   = '1234'
MYSQL_DATABASE   = 'acl'
MYSQL_TABLE_NAME = 'online_user'
################################################################
PROCESS_NAME = "zdns:cms_aclmonitor"
class DDIFormatter < Log4r::Formatter
    def format(event)
        buffer = Time.now.strftime("%Y-%m-%d %H:%M:%S ")
        buffer += "#{Log4r::LNAMES[event.level]}: "
        buffer += "#{event.data}\n"
    end
end

class Log
    MODULE_NAME = "acl_monitor"
    @@logger = nil
    def self.get_log_instance
        return @@logger unless @@logger.nil?
        config = {
            "filename" => "/usr/local/#{MODULE_NAME}.log",
            "maxsize" => 30 * 1024 * 1024,
            "max_backups" => 4,
            "trunc" => false,
            "formatter" => DDIFormatter
        }
        @@logger = Log4r::Logger.new(MODULE_NAME)
        @@logger.outputters = Log4r::RollingFileOutputter.new(MODULE_NAME, config)
        @@logger.level = LOG_LEVEL_DEBUG ? Log4r::DEBUG : Log4r::INFO
        @@logger
    end
    def self.debug(data); get_log_instance.debug(data); end
    def self.info(data); get_log_instance.info(data); end
    def self.error(data); get_log_instance.error(data); end
end

class Command
    def self.send_cmd(method, url, args)
        if url.start_with?("https://")
            HTTParty::Basement.default_options.update(verify: false)
            HTTParty::Basement.basic_auth(USER_NAME, PASS_WORD)
        end
        args["current_user"] = "admin"
        body = {:body => args.to_json, :timeout => 300,
                :headers => {'Content-Type' => 'application/json', 'Accept' => 'application/json'}}
        cmd = HTTParty.send(method.downcase, url, body)
        if cmd.code.to_i == 401
            Log.error "user name or password error!!!"
        elsif cmd.code == 200
            return JSON.load(cmd.body.to_s)
        else
            Log.error "#{method} #{url} {...}; Return Code: #{cmd.code}"
        end
    rescue Exception => e
        Log.error e.to_s
    end
end

class Mysql
    include Singleton
    attr_accessor :client

    def connect_to_database
        if @client.nil?
            @client = Mysql2::Client.new(host: MYSQL_HOST, username: MYSQL_USER_NAME, password: MYSQL_PASSWORD )
            @client.select_db(MYSQL_DATABASE)
            @client 
        else
          @client
        end
    rescue Exception => ex
        Log.error "#{ex.to_s} Mysql Connected Failed"
    end 

    # 关闭数据库链接的方法
    # 并将@@client置为nil
    def close_connection
        unless @client.nil?
            @client.close
            @client = nil
        end
    rescue Exception => e
        Log.error e.to_s
    end
    
    # 组织好从数据查询的acls数据
    # { intern_package_name1 => [ip1, ip2, ip3], intern_package_name2 => [ip1, ip2, ip4] } 
    def query_acls
        client = connect_to_database
        results = client.query("SELECT USER_IPV4,USER_IPV6,INTERNET_PACKAGE_NAME FROM #{MYSQL_TABLE_NAME}")
        acls = {}
        results.each do |res|
            user_group = res['INTERNET_PACKAGE_NAME']          
            next unless INTERNET_PACKAGE_NAME_ACL_NAME_MAP.has_key? user_group
            acl_name   = INTERNET_PACKAGE_NAME_ACL_NAME_MAP[user_group]
            user_ipv4  = res['USER_IPV4']
            user_ipv6  = res['USER_IPV6']

            acls[acl_name] ||= []

            if IPAddress.valid_ipv4?(user_ipv4)
                user_ipv4 = IPAddress(user_ipv4).octets.join('.')
                acls[acl_name] << user_ipv4
            else
                Log.info "#{user_ipv4} is not valid! this ip will not send to server" if !user_ipv4.nil? && !user_ipv4.empty?
            end

            if IPAddress.valid_ipv6?(user_ipv6)
                user_ipv6 = IPAddress(user_ipv6).compressed
                acls[acl_name] << user_ipv6
            else
                Log.info "#{user_ipv6} is not valid! this ip will not send to server" if !user_ipv6.nil? && !user_ipv6.empty?
            end
        end
        acls
    rescue Exception => ex
        Log.error "#{ex.to_s} Mysql Query Failed"
        nil
    ensure
        close_connection
    end
    
end

class Acl_Monitor
    # 上次查询的数据
    @@last_query_result = {}

    def send_changed_acls(changed_acls, current_acls)
        changed_acls.each { |acl_name, acl_ips| changed_acls[acl_name] = acl_ips.uniq.join(';') }
        modify_infos = changed_acls
        url = "https://127.0.0.1:20120/acls"
        # 获取到系统内的acl资源
        current_system_acls = Command.send_cmd("GET", url, {})["resources"]

        if !current_acls.empty?
            current_acl_keys = current_system_acls.map { |acl| acl["name"] }
            changed_acl_keys = modify_infos.keys
            both_have_keys = current_acl_keys & changed_acl_keys
            temp_modify_infos = {}
            both_have_keys.each { |key| temp_modify_infos[key] = modify_infos[key] }
            modify_infos = temp_modify_infos
        end

        params = {"modify_infos" => modify_infos}
        10.times do |i|
            start_time = Time.now.to_i
            ret = Command.send_cmd("PUT", url, params)
                        
            # 如果更新失败则不更新上次查询的数据，并记录失败的日志
            # 如果更新成功更新上次上次查询的数据，并记录成功更新的日志
            if ret.nil?
                Log.error "send changed acls failed, try again(#{i})!"
                sleep 10
            else
                Log.info "PUT #{url} {\"modify_infos\" => #{modify_infos.keys}}, return: #{ret}, take times: [#{Time.now.to_i - start_time}s]" if ret
                @@last_query_result = current_acls 
                return
            end

        end
        Log.error "send changed acls failed"
    rescue Exception => e
        Log.error "send_changed_acls failed! : #{e.to_s}"
    end

    def start
        Log.info "start acl monitor..."
        loop do
            begin
                # 从客户的mysql数据库获取到acls数据
                current_acls = Mysql.instance.query_acls
                next unless current_acls.is_a? Hash 
                if current_acls == @@last_query_result
                    Log.info 'Current Data do not changed'  
                else
                    # 对比当前系统内的acls的数据和数据中查询的acls数据，将有变更的数据更新
                    changed_acl_log = ''
                    changed_acls = {}
                    if @@last_query_result.empty?
                        changed_acls = current_acls.dup
                    else
                        @@last_query_result.each do |acl_name, acl_ips|
                            current_acl_ips = current_acls[acl_name]
                            # 对比数据库中的acl数据和系统中的acl数据中的ip
                            # 如果当前的acl_name下的ip并没有改变则查看下一个acl
                            next if current_acl_ips == acl_ips
                            # 当上次查询的acl_name 在 这次查询中没有出现，则说明该acl_name在数据库已经被全部删除
                            current_acl_ips ||= []
                            # 给更改的acls增加值
                            changed_acls[acl_name] = current_acl_ips
                            # 需要增加的acl ip 
                            add_acl_ips = current_acl_ips - acl_ips
                            # 需要删除的acl ip
                            remove_acl_ips = acl_ips - current_acl_ips
                            # 增加更新日志
                            add_acl_ips.each { |ip| changed_acl_log << "add ip [#{ip}] to acl [#{acl_name}]; " } unless add_acl_ips.empty?
                            # ip减少更新的日志
                            remove_acl_ips.each { |ip| changed_acl_log << "delete ip [#{ip}] from acl [#{acl_name}]; " } unless remove_acl_ips.empty? 
                        end

                        extra_keys = current_acls.keys - @@last_query_result.keys
                        if !extra_keys.empty?
                            extra_keys.each do |acl_name| 
                                changed_acls[acl_name] = current_acls[acl_name]
                                current_acls[acl_name].each { |ip| changed_acl_log << "add ip [#{ip}] to acl [#{acl_name}]; " }
                            end
                        end
                    end
                    
                    if changed_acls.empty?
                        Log.info 'Nothing changed!'
                    else
                        send_changed_acls changed_acls, current_acls
                        Log.info 'Already sent acls to system'
                        Log.info changed_acl_log
                    end

                end
            rescue Exception => ex
                Log.error ex.to_s
            end
            sleep ACL_REFRESH_INTERVAL   
        end
    end
    
    def self.is_self_exist?
        result = `ps -ef | grep "#{PROCESS_NAME}" | grep -v grep`
        result.to_s.split("\n").each do |line|
            process = line.split(" ", 8)
            return true if process[7].start_with?(PROCESS_NAME)
        end
        false
    rescue Exception => e
        false
    end
end

if $0 == __FILE__

    if "-k" == ARGV[0].to_s.downcase
        pid = `ps ax | grep 'zdns:cms_aclmonitor' | grep -v grep | awk '{print $1}'`.to_s.strip
        Log.info pid
        if $?.to_i == 0 && pid =~ /^\d+$/
            Log.info "Kill acl monitor first, current pid[#{pid}]"
            `kill -9 #{pid}` 
        end
    end
    Process.exit! if Acl_Monitor.is_self_exist?
    acl_monitor = `cat /usr/bin/zdns_monitor | grep '/usr/bin/AclMonitor.rb'`.strip
    unless acl_monitor.start_with?("/usr/bin/AclMonitor.rb")
        Log.info "===== Add acl to zdns_monitor ====="
        `echo '/usr/bin/AclMonitor.rb' >> /usr/bin/zdns_monitor`
    end
    Log.info "===== Begin start acl monitor ====="
    pid = fork do
        $0 = PROCESS_NAME
        Acl_Monitor.new.start
    end
    Process.detach(pid)

end
