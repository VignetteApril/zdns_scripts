require 'fileutils'
require "net/ftp"
require "net/sftp"
require 'date'
require 'socket'
require 'log4r'
require 'net/http'
require 'uri'
require 'timeout'

##################### 使用说明 （版本：2019-12-25）####################
# 1) 本脚本提供通过传入的时间参数过滤日志的功能，keepalive 和 secure-* 因为
#    日志中没有年份信息，不支持跨年的过滤
# 2) 本脚本将会部署在各个节点，日志采集完成后，将日志打包为zip格式，然后上传到
#    华为sftp服务器，上传完成后会删除本地的zip包以及临时文件。并发送请求给华为
#    的服务器并告知上传结果
# 3) zip包的名称为zdns_{node_id}_{start_date}_{end_date}.zip
# 4) 本脚本会产生自己的日志在/usr/local/upload_to_huawei_logs000001.log
# 5) 进程名称为 zdns:upload_to_huawei
# 6) 目标日志列表见 UploadJob::LOGS
##################### 调用说明 ########################################
# 调用命令
# ruby aquire_log.rb 10.1.107.164 22 root /backups/ 20190122T100506 20191230T140506
# 参数说明:
# 1) 10.1.107.164 		为华为的sftp server ip
# 2) 22 				为华为的sftp server 端口
# 3) root 				为华为sftp server 用户
# 4) /backups/ 			为华为sftp server的目标路径
# 5) 20190122T100506 	为日志过滤开始的时间戳
# 6) 20191230T140506 	为日志结束的时间戳
##################### Sftp server ####################################
# 1) 关于sftp server的配置，需要事先在目标的sftp server配置好当前server的ssh
#    key，否则上传不成功
######################################################################

PROCESS_NAME = 'zdns:upload_to_huawei'
RUBY_STYLE_FORMAT = <<END_OF_SCRIPT
#!/usr/bin/gawk -v start_date=1447034117 -v end_date=1447034118 -f

{
	 temp_date = $1;
	 temp_time = $2;
     gsub(/-|\\//, " ", temp_date);
     gsub(":", " ", temp_time);
     current_unix_time = mktime( temp_date " " temp_time );
     if (current_unix_time >= start_date && current_unix_time <= end_date) {
     	print;
    }
}
END_OF_SCRIPT

WEB_ACCESS_STYLE_FORMAT = <<END_OF_SCRIPT
#!/usr/bin/gawk -v start_date=1447034117 -v end_date=1447034118 -f

{
	 temp_date = $6;
	 gsub("+08:00", "", temp_date); 
	 gsub(/-|T|:|\\[|\\]/, " ", temp_date);

     current_unix_time = mktime( temp_date );
     if (current_unix_time >= start_date && current_unix_time <= end_date) {
     	print;
    }
}
END_OF_SCRIPT

BIND_STYLE_FORMAT = <<END_OF_SCRIPT
#!/usr/bin/gawk -v start_date=1447034117 -v end_date=1447034118 -f
BEGIN {
	 m=split("Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec",d,"|");
	 for(o=1;o<=m;o++){
     	months[d[o]]=sprintf("%02d",o);
     }
}

{
	 temp_date = $1;
	 temp_time = $2;

	 split(temp_date, date_arr, "-")
	 split(temp_time, time_arr, ":")

     current_unix_time = mktime( date_arr[3] " " months[date_arr[2]] " " date_arr[1] " " time_arr[1] " " time_arr[2] " " time_arr[3]);
     if (current_unix_time >= start_date && current_unix_time <= end_date) {
     	print;
    }
}
END_OF_SCRIPT

KEEPALIVE_STYLE_FORMAT = <<END_OF_SCRIPT
#!/usr/bin/gawk -v start_date=1447034117 -v end_date=1447034118 -f
BEGIN {
	 m=split("Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec",d,"|");
	 for(o=1;o<=m;o++){
     	months[d[o]]=sprintf("%02d",o);
     }
}

{
	 month = $1;
	 day = $2;
	 year = strftime("%Y");
	 temp_time = $3;
	 split(temp_time, time_arr, ":");
     current_unix_time = mktime( year " " months[month] " " day " " time_arr[1] " " time_arr[2] " " time_arr[3] );
     if (current_unix_time >= start_date && current_unix_time <= end_date) {
     	print;
    }
}
END_OF_SCRIPT

# sftp script
SFTP_SHELL_UPLOAD_FILE = <<-CONF
#!/usr/bin/expect
set timeout 480
set host [lindex $argv 0]
set port [lindex $argv 1]
set username [lindex $argv 2]
set rsa_path [lindex $argv 3]
set src_file [lindex $argv 4]
set dest_file [lindex $argv 5]
spawn sftp -oIdentityFile=$rsa_path -oPort=$port $username@$host
expect {
    "sftp>" {send "\\n"}
}
expect "sftp>"
send "put $src_file $dest_file\\n"
expect "sftp>"
send "quit\\n"
expect eof
CONF

class DDIFormatter < Log4r::Formatter
    def format(event)
        buffer = Time.now.strftime("%Y-%m-%d %H:%M:%S ")
        buffer += "#{Log4r::LNAMES[event.level]}: "
        buffer += "#{event.data}\n"
    end
end

class Log
    MODULE_NAME = "upload_to_huawei_logs"
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
        @@logger.level = Log4r::INFO
        @@logger
    end
    def self.debug(data); get_log_instance.debug(data); end
    def self.info(data); get_log_instance.info(data); end
    def self.error(data); get_log_instance.error(data); end
end

class UploadJob
	SCRIPT_PATH = '/usr/local/huawei_aquire_logs.awk'
	TARGET_LOG_PATH = '/usr/local/logs_data'
	TARGET_LOG_TAR = '/usr/local/logs_data.zip'
	SFTP_PRIVATE_KEY = '/root/.ssh/id_rsa'
	SFTP_BIN_PATH = '/usr/bin/huawei_sftp'
	MAX_FILE_SIZE = 5 * 2**20
	MAX_LINE_CHECK = 50
	HUAWEI_INFO_URL = '/api/v1.0/3rd-conf/conf-backup-result'

	# 日志过滤列表
	LOGS = [
		{
			name: 'grid',
			path: '/usr/local',
			find_name: 'grid0*.log',
			target_name: 'grid_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'dns',
			path: '/usr/local',
			find_name: 'dns0*.log',
			target_name: 'dns_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'clouddns',
			path: '/usr/local',
			find_name: 'clouddns0*.log',
			target_name: 'clouddns_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'system',
			path: '/usr/local',
			find_name: 'system0*.log',
			target_name: 'system_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'node',
			path: '/usr/local',
			find_name: 'node0*.log',
			target_name: 'node_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'agent',
			path: '/usr/local',
			find_name: 'agent0*.log',
			target_name: 'agent_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'zva',
			path: '/usr/local',
			find_name: 'zva0*.log',
			target_name: 'zva_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'monitor_core',
			path: '/usr/local',
			find_name: 'monitor_core0*.log',
			target_name: 'monitor_core_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'monitor_main',
			path: '/usr/local',
			find_name: 'monitor_main0*.log',
			target_name: 'monitor_main_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{ 
			name: 'web_access',
			path: '/usr/local',
			find_name: 'web.access.log',
			target_name: 'web.access_target.log',
			log_type: 'WEB_ACCESS_STYLE_FORMAT',
		},
		{
			name: 'web_error',
			path: '/usr/local',
			find_name: 'web.error.log',
			target_name: 'web.error_target.log',
			log_type: 'RUBY_STYLE_FORMAT',
		},
		{
			name: 'web',
			path: '/usr/local',
			find_name: 'web0*.log',
			target_name: 'web_target.log',
			log_type: 'RUBY_STYLE_FORMAT',
		},
		{
			name: 'rsync',
			path: '/usr/local/rsync',
			find_name: 'rsync.log',
			target_name: 'rsync_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'zddi',
			path: '/usr/local',
			find_name: 'zddi.log',
			target_name: 'zddi_target.log',
			log_type: 'ZDDI_STYLE_FORMAT'
		},
		{
			name: 'dmesg',
			path: '/usr/local',
			find_name: 'dmesg.log',
			target_name: 'dmesg_target.log',
			log_type: 'DMESG_STYLE_FORMAT'
		},
		{
			name: 'upgrade',
			path: '/usr/local',
			find_name: 'upgrade.log',
			target_name: 'upgrade_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'upgrade_upgrade',
			path: '/usr/local/upgrade',
			find_name: 'upgrade.log',
			target_name: 'upgrade_upgrade_target.log',
			log_type: 'UPGRADE_STYLE_FORMAT'
		},
		{
			name: 'upgrade_manager',
			path: '/usr/local',
			find_name: 'upgrade_manager.log',
			target_name: 'upgrade_manager_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'zdns_alarm_node_msg',
			path: '/usr/local',
			find_name: 'zdns_alarm_node_msg0*.log',
			target_name: 'zdns_alarm_node_msg_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'general_log',
			path: '/usr/local/zddi/dns/log',
			find_name: 'general_log',
			target_name: 'general_log_target.log',
			log_type: 'BIND_STYLE_FORMAT'
		},
		{
			name: 'resolver',
			path: '/usr/local/zddi/dns/log',
			find_name: 'resolver.log.',
			target_name: 'resolver_log_target.log',
			log_type: 'BIND_STYLE_FORMAT'
		},
		{
			name: 'sa',
			path: '/var/log/sa',
			find_name: 'sa*',
			target_name: 'sa_target.tar.gz',
			log_type: 'SA_STYLE_FORMAT'
		},
		{
			name: 'keepalive',
			path: '/usr/local',
			find_name: 'keepalive.log',
			target_name: 'keepalive_target.log',
			log_type: 'KEEPALIVE_STYLE_FORMAT'
		},
		{
			name: 'secure',
			path: '/var/log',
			find_name: 'secure-*',
			target_name: 'secure_target',
			log_type: 'KEEPALIVE_STYLE_FORMAT'
		},
		{
			name: 'monitor_cpu_mem',
			path: '/usr/local',
			find_name: 'monitor_cpu_mem.log',
			target_name: 'monitor_cpu_mem_target.log',
			log_type: 'MONITOR_STYLE_FORMAT'
		},
		{
			name: 'zdns_monitor_snmp',
			path: '/usr/local',
			find_name: 'zdns_monitor_snmp0*.log',
			target_name: 'zdns_monitor_snmp_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'add',
			path: '/usr/local/zddi/dns/log',
			find_name: 'add.log.',
			target_name: 'add_target.log.',
			log_type: 'BIND_STYLE_FORMAT'
		},
		{
			name: 'cloudprobe',
			path: '/usr/local',
			find_name: 'cloudprobe0*.log',
			target_name: 'cloudprobe_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'probe',
			path: '/usr/local',
			find_name: 'probe0*.log',
			target_name: 'probe_target.log',
			log_type: 'RUBY_STYLE_FORMAT'
		},
		{
			name: 'probed',
			path: '/usr/local',
			find_name: 'probed.log*',
			target_name: 'probed_target.log',
			log_type: 'BIND_STYLE_FORMAT'
		}
	]

	# @params start_date Timestamp 日志起始时间
	# @params end_date   Timestamp 日志结束时间
	# @params logs 		 Array     指定要取的日志
	def start ftp_ip, ftp_port, ftp_user, backup_path, start_date, end_date, node_ip, logs=[]
	    Log.info "====================Starting...====================="

	    end_date = end_date + '2359' if end_date.length <= 8 # 如果结束日期中没有包含时间则设置时间为一天得结束
		unix_start_date = DateTime.parse(start_date + ' +0800').to_time.to_i
		unix_end_date   = DateTime.parse(end_date + ' +0800').to_time.to_i
		return if unix_start_date > unix_end_date # 时间校验
		zip_name 		= "zdns_#{node_ip}_#{start_date}_#{end_date}.zip"
		target_tar_path = File.join(backup_path, zip_name)
		reload_target_log_path

		LOGS.each do |log|
			log_find_path = File.join(log[:path], log[:find_name])
			target_log_path = File.join(TARGET_LOG_PATH, log[:target_name])

			case log[:log_type]
			when 'ZDDI_STYLE_FORMAT'
				parse_zddi_log log_find_path, target_log_path
			when 'DMESG_STYLE_FORMAT'
				generate_dmesg_log target_log_path
			when 'UPGRADE_STYLE_FORMAT'
				parse_upgrade_log log_find_path, target_log_path
			when 'SA_STYLE_FORMAT'
				parse_sa_log log[:path], target_log_path, start_date, end_date
			when 'MONITOR_STYLE_FORMAT'
				parse_monitor_cpu_men_log log_find_path, target_log_path, unix_start_date, unix_end_date
			else
				reload_script_file
				current_log_paths = Dir.glob(log_find_path)
				script = Object.const_get(log[:log_type])
				File.open(SCRIPT_PATH, "w+"){ |f| f.write(script) }
				current_log_paths.each do |log_path|
					# next if check_if_can_skip_log log_path, log[:log_type], unix_start_date, unix_end_date
					cmd = "gawk -v start_date=#{unix_start_date} -v end_date=#{unix_end_date} -f #{SCRIPT_PATH} #{log_path} > #{target_log_path}"
					system(cmd)
				end
			end
		end

		# 压缩文件
		compress_log_files
		upload_rs = false
		if check_file_size
			Log.info "File uploading..."
			upload_rs = upload_file_through_sftp ftp_ip, ftp_port, ftp_user, target_tar_path
			Log.info "File upload work was done with #{upload_rs}."
		else
			Log.info "Current file size larger than 5M, Upload file failed!"
		end
		send_upload_result_to_huawei upload_rs
	rescue => e
		Log.error "!!!!!!!!!!!!!!!!!!!!!!!!!!! #{e.to_s} !!!!!!!!!!!!!!!!!!!!!!!!!!!"
	ensure
		Log.info "Deleting temp files...: #{TARGET_LOG_PATH}, #{TARGET_LOG_TAR}, #{SFTP_BIN_PATH}, #{SCRIPT_PATH}"
		FileUtils.rm_r TARGET_LOG_PATH, force: true if Dir.exist?(TARGET_LOG_PATH)
		FileUtils.rm_r TARGET_LOG_TAR, force: true if File.exist?(TARGET_LOG_TAR)
		FileUtils.rm_r SFTP_BIN_PATH, force: true if File.exist?(SFTP_BIN_PATH)
		FileUtils.rm_r SCRIPT_PATH, force: true if File.exist?(SCRIPT_PATH)

		Log.info "=================Upload work end...================="
	end

	def reload_target_log_path
		FileUtils.rm_r TARGET_LOG_PATH, force: true if Dir.exist?(TARGET_LOG_PATH)
		FileUtils.mkdir_p TARGET_LOG_PATH
	end

	def reload_script_file
		FileUtils.rm_r SCRIPT_PATH, force: true if File.exist?(SCRIPT_PATH)
		FileUtils.touch SCRIPT_PATH
	end

	def compress_log_files
		Log.info 'Compressing file...'
		cmd = "zip -r #{TARGET_LOG_TAR} #{TARGET_LOG_PATH}"
		system(cmd)
	end

	def upload_file_through_sftp ftp_ip, ftp_port, ftp_user, target_tar_path
		reload_sftp_bin_file
		add_know_hosts ftp_ip

		File.open(SFTP_BIN_PATH, "w"){|f| f << "#{SFTP_SHELL_UPLOAD_FILE}"}

		cmd = "#{SFTP_BIN_PATH} #{ftp_ip} #{ftp_port} #{ftp_user} #{SFTP_PRIVATE_KEY} #{TARGET_LOG_TAR} #{target_tar_path} 2>&1"
		
		begin
		  Timeout.timeout(15) do
  			rs = system(cmd)
		  end
		rescue Timeout::Error
			Log.info "Upload command time out!!"
			rs = false
		end
		rs
	end

	def reload_sftp_bin_file
		FileUtils.rm_r SFTP_BIN_PATH, force: true if File.exist?(SFTP_BIN_PATH)
		FileUtils.touch SFTP_BIN_PATH
		`chmod u+rwx #{SFTP_BIN_PATH}`
	end

	def add_know_hosts ftp_ip
		`ssh-keygen -R #{ftp_ip}`
		`ssh-keyscan #{ftp_ip} >> ~/.ssh/known_hosts`
	end

	# 主动给华为发送请求，告知上传是否成功
	def send_upload_result_to_huawei upload_flag
		msg = upload_flag ? 'success' : 'failed'
		Log.info "Sending upload #{msg} message to huawei server"
		# Net::HTTP.post URI(HUAWEI_INFO_URL),
	 #              	   { "version" => "V001R017C00", "vm_type" => "controller", "status" => "#{msg}" }.to_json,
	 #               	   "Content-Type" => "application/json"
	end

	# 检查要上传的文件的大小，如果大于5M的话则告知华为上传失败
	# 如果小于5M则继续上传
	def check_file_size
		ret_bool = false
		if File.exist?(TARGET_LOG_TAR)
			currnet_log_size = File.size(TARGET_LOG_TAR)
			if currnet_log_size <= MAX_FILE_SIZE
				ret_bool = true
			end
		end
		ret_bool
	end

	# 用于检查日志是否可以跳过过滤
	def check_if_can_skip_log log_file, log_type, unix_start_date, unix_end_date
		ret_bool = false
		top_line_num = 0
		bottom_line_num = 0
		first_line_date = 0
		last_line_date  = 0

		while first_line_date == 0
			top_line_num += 1
			# 防止死循环
			break if top_line_num == MAX_LINE_CHECK

			first_line = `head -#{top_line_num} #{log_file} | tail -1`
			first_line_date = extract_time_from_line first_line, log_type
		end

		while last_line_date == 0
			bottom_line_num += 1
			# 防止死循环
			break if bottom_line_num == MAX_LINE_CHECK

			last_line = `tail -#{bottom_line_num} #{log_file} | head -1`
			last_line_date = extract_time_from_line last_line, log_type
		end

		if (last_line_date < unix_start_date || first_line_date > unix_end_date) && (last_line_date != 0 && first_line_date != 0)
			ret_bool = true
		end

		ret_bool
	end

	def extract_time_from_line line, log_type
		case log_type
		when 'RUBY_STYLE_FORMAT', 'BIND_STYLE_FORMAT'
			date_string = line.split(' ').first(2).join(' ')
			DateTime.parse(date_string + ' +0800').to_time.to_i
		when 'WEB_ACCESS_STYLE_FORMAT'
			date_string = line.split(' ')[5].gsub(/\[|\]/, '')
			DateTime.parse(date_string + ' +0800').to_time.to_i
		when 'KEEPALIVE_STYLE_FORMAT'
			date_string = "#{Date.today.year} #{line.split(' ').first(3).join(' ')}"
			DateTime.parse(date_string + ' +0800').to_time.to_i
		end
	rescue => e
		return 0
	end

	def parse_zddi_log log_file, target_file
		cmd = "tail -n 1000 #{log_file} >> #{target_file}"
		system(cmd)
	end

	def generate_dmesg_log target_file
		cmd = "dmesg >> #{target_file}"
		system(cmd)
	end

	def parse_upgrade_log log_file, target_file
		cmd = "cp #{log_file} #{target_file}"
		system(cmd)
	end

	def parse_sa_log log_path, target_log_path, start_date, end_date
		start_date = DateTime.parse(start_date + ' +0800')
		end_date = DateTime.parse(end_date + ' +0800')
		start_day = start_date.month != end_date.month ? 1 : start_date.day
		end_day = end_date.day
		file_name_arr = []

		(start_day..end_day).each do |day|
			file_name = File.join(log_path, "sa#{sprintf('%02d', day)}")
			file_name_arr << file_name if File.exist?(file_name)
		end

		if !file_name_arr.empty?
			cmd = "zip -r #{target_log_path} #{file_name_arr.join(' ')}"
			system(cmd)
		end
	end

	def parse_monitor_cpu_men_log log_path, target_log_path, unix_start_date, unix_end_date
		return unless File.exist? log_path
		insert_flag = true
		lines = ""
		IO.foreach(log_path) do |line|
			if line.include?('====')
				date_string = line.split(' ').first(2).join(' ')
				current_unix_time = DateTime.parse(date_string + ' +0800').to_time.to_i

				if current_unix_time >= unix_start_date && current_unix_time <= unix_end_date
					insert_flag = true
				else
					insert_flag = false
				end
			end
			lines << line if insert_flag
		end

		File.open(target_log_path, "w+"){ |f| f.write(lines) }
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
	sftp_ip 	= ARGV[0] # sftp server ip address
	sftp_port 	= ARGV[1] # sftp server port
	sftp_user 	= ARGV[2] # sftp server user
	sftp_path 	= ARGV[3] # sftp upload to path
	start_date 	= ARGV[4] # filter log the start date
	end_date 	= ARGV[5] # filter log the end date
	ip = Socket.ip_address_list.detect{|intf| intf.ipv4_private?}
	client_ip 	= ip.ip_address

	if ARGV.size != 6
        puts 'The script only accept 6 arguments' 
        exit 1 
    end
	
	Process.exit! if UploadJob.is_self_exist?

    pid = fork do
        $0 = PROCESS_NAME
        UploadJob.new.start sftp_ip, sftp_port, sftp_user, sftp_path, start_date, end_date, client_ip
    end
end
