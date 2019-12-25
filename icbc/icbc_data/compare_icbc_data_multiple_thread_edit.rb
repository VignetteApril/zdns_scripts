# coding: utf-8
#!/usr/local/bin/ruby -Ku
require 'spreadsheet'
require 'thwait'

# IP_ADDRESS = '10.1.115.220'
IP_ADDRESS = '84.37.97.110'
THREAD_NUM = 5

class DataSheet
    attr_reader :book
    # 表头的格式
    ICBC_AREA_FORMAT    = Spreadsheet::Format.new horizontal_align: :center, pattern_fg_color: :yellow, pattern: 1
    NO_ICBC_AREA_FORMAT = Spreadsheet::Format.new horizontal_align: :center, pattern_fg_color: :lime, pattern: 1
    FORWARD_AREA_FORMAT = Spreadsheet::Format.new horizontal_align: :center, pattern_fg_color: :aqua, pattern: 1
    ACL_AREA_FORMAT     = Spreadsheet::Format.new horizontal_align: :center, pattern_fg_color: :gray, pattern: 1
    # 文字居中的格式
    TEXT_CENTER_FORMAT  = Spreadsheet::Format.new horizontal_align: :center
    
    # 区名
    ICBC_AREA    = 'ICBC区'
    NO_ICBC_AREA = '非ICBC区'
    FORWARD_AREA = '转发区'
    ACL_AREA     = 'ACL信息'

    # 区的列名
    ICBC_FILEDS = %w( 视图 域名 TTL 记录类型 记录值 )
    NO_ICBC_FILEDS = %w( 视图 权威区 域名 TTL 记录类型 记录值 )
    FORWARD_FILEDS = %w( 转发域名 视图 转发服务器 转发方式 )
    ACL_FILEDS = %w( 视图 优先级 访问控制列表 网络信息 )

    def initialize old_file_name, new_file_name
        @old_file_name = old_file_name
        @new_file_name = new_file_name
        # 创建表格
        @book = Spreadsheet::Workbook.new
        # 创建三个工作簿
        sheet_create = @book.create_worksheet(name: '新增')
        sheet_delete = @book.create_worksheet(name: '删除')
        sheet_update = @book.create_worksheet(name: '修改')

        create_table_head sheet_create
        create_table_head sheet_delete
        create_table_head_for_update sheet_update, old_file_name, new_file_name
    end

    def create_table_head sheet
        sheet.merge_cells(0,0,0,18)
        sheet[0,0] = "#{@new_file_name}的数据较#{@old_file_name}#{sheet.name}信息如下"
        # 为三个工作簿创建表头
        # 合并单元格
        sheet.merge_cells(1,0,1,4)
        sheet.merge_cells(1,5,1,10)
        sheet.merge_cells(1,11,1,14)
        sheet.merge_cells(1,15,1,18)
        # 设置表头的单元格样式
        sheet.row(1).set_format(0, ICBC_AREA_FORMAT)
        sheet.row(1).set_format(5, NO_ICBC_AREA_FORMAT)
        sheet.row(1).set_format(11, FORWARD_AREA_FORMAT)
        sheet.row(1).set_format(15, ACL_AREA_FORMAT)
        # 设置表格的值
        sheet[1,0] = ICBC_AREA
        sheet[1,5] = NO_ICBC_AREA
        sheet[1,11] = FORWARD_AREA
        sheet[1,15] = ACL_AREA
        # 设置列名的值
        (ICBC_FILEDS + NO_ICBC_FILEDS + FORWARD_FILEDS + ACL_FILEDS).each_with_index do |field, index|
            sheet[2, index] = field
        end
    end
    
    # 为工作簿[修改]创建一个表头
    def create_table_head_for_update sheet, old_file_name, new_file_name
        sheet.merge_cells(0,0,0,37)
        sheet[0,0] = "#{@new_file_name}的数据较#{@old_file_name}#{sheet.name}信息如下"
        # 设置第一行的表头
        # 合并单元格
        sheet.merge_cells(1,0,1,9)
        sheet.merge_cells(1,10,1,21)
        sheet.merge_cells(1,22,1,29)
        sheet.merge_cells(1,30,1,37)
        # 设置表头的单元格样式
        sheet.row(1).set_format(0, ICBC_AREA_FORMAT)
        sheet.row(1).set_format(10, NO_ICBC_AREA_FORMAT)
        sheet.row(1).set_format(22, FORWARD_AREA_FORMAT)
        sheet.row(1).set_format(30, ACL_AREA_FORMAT)
         # 设置表格的值
        sheet[1,0] = ICBC_AREA
        sheet[1,10] = NO_ICBC_AREA
        sheet[1,22] = FORWARD_AREA
        sheet[1,30] = ACL_AREA
        
        # 设置第二行的表头
        sheet.merge_cells(2,0,2,4)
        sheet.merge_cells(2,5,2,9)
        sheet.merge_cells(2,10,2,15)
        sheet.merge_cells(2,16,2,21)
        sheet.merge_cells(2,22,2,25)
        sheet.merge_cells(2,26,2,29)
        sheet.merge_cells(2,30,2,33)
        sheet.merge_cells(2,34,2,37)

        # 设置第二行表头的值
        column_nums = [0, 5, 10, 16, 22, 26, 30, 34]
        column_nums.each_with_index do |num, index|
            second_table_head = index % 2 == 0 ? "变更前#{old_file_name}" : "变更后#{new_file_name}"
            sheet[2,num] = second_table_head
            sheet.row(2).set_format(num, TEXT_CENTER_FORMAT)
        end
        # 设置第三行表头的值
        (ICBC_FILEDS * 2 + NO_ICBC_FILEDS * 2 + FORWARD_FILEDS * 2 + ACL_FILEDS * 2).each_with_index do |field, index|
            sheet[3,index] = field
        end
    end
end

module CompareData
    # 取到
    def self.get_db_path file_path, filename
        path = '/tmp/' + filename
        `mkdir #{path}`
        puts "tar vxf #{file_path} -C #{path}"
        `tar vxf #{file_path} -C #{path}`
        path = path + "/" + "inactive" + "/" + IP_ADDRESS
        pathname = `find #{path} -name \"clouddns.db\"`
        path = pathname.strip!
        # 复制10个数据库，用于后期的多线程操作
        THREAD_NUM.times.each do |index|
            cp_path = path + "_#{index}"
            puts "cp -f #{path} #{cp_path}"
            `cp -f #{path} #{cp_path}`
        end
        path
    end

    def self.start_compare old_db_path, new_db_path, book
        while File.exist?(old_db_path) && File.exist?(new_db_path) do
            puts "analysis changed tables ......"
            changed_tables_data_strings = `sqldiff #{old_db_path} #{new_db_path}`
            temp_array = changed_tables_data_strings.gsub('\\n', '').gsub("\"", "").split("\n").map{ |data| data.split(' ') }
            changed_table = []
            temp_array.each do |sql_arr|
                next if sql_arr.empty?
                sql_head = sql_arr[0]
                case sql_head
                when 'UPDATE'
                    changed_table << sql_arr[1]
                when 'DELETE'
                    changed_table << sql_arr[2]
                when 'INSERT'
                    changed_table << sql_arr[2].split("(")[0]
                end
            end

            if changed_table.empty?
                puts "nothing changed!"
                return
            else
                puts "analysis complete!"
                changed_table.uniq!
                puts changed_table
            end

            self.compare_create_data old_db_path, new_db_path, book, changed_table
            self.compare_delete_data old_db_path, new_db_path, book, changed_table
            self.compare_update_data old_db_path, new_db_path, book, changed_table

            break # 只做一次循环
        end

    end
    
    def self.compare_create_data old_db_path, new_db_path, book, changed_table
        puts '=================start prepare create compare data...=================s'
        total_data = self.compare_data old_db_path, new_db_path, changed_table
        puts '=================screate compare data prepare complete! start write data to doc !=================s'
        self.write_data_to_excel total_data, book, '新增'
    end

    def self.compare_delete_data old_db_path, new_db_path, book, changed_table
        puts '=================start prepare delete compare data...=================s'
        total_data = self.compare_data new_db_path, old_db_path, changed_table
        puts '=================delete compare data prepare complete! start write data to doc !=================s'
        self.write_data_to_excel total_data, book, '删除'
    end

    def self.compare_update_data old_db_path, new_db_path, book, changed_table
        puts '=================start prepare update compare data...=================s'
        total_data = self.compare_data old_db_path, new_db_path, changed_table, true
        puts '=================update compare data prepare complete! start write data to doc !=================s'
        self.write_data_to_excel total_data, book, '修改'
    end

    def self.write_data_to_excel total_data, book, sheet_name
        sheet = book.worksheet sheet_name
        start_row = sheet_name == '修改' ? 4 : 3
        total_data.each do |area_name, data|
            case area_name
            when 'ACL信息'
                start_col = sheet_name == '修改' ? 30 : 15
            when '转发区'
                start_col = sheet_name == '修改' ? 22 : 11
            when 'ICBC区'
                start_col = 0
            when '非ICBC区'
                start_col = sheet_name == '修改' ? 10 : 5
            end

            data.each_with_index do |row_data, row_index|
                row_data.each_with_index do |col_data, col_index|
                    sheet[row_index + start_row, start_col + col_index] = col_data
                end
            end
        end
    end

    # 对比出增或者删，并获取表格所需要的数据
    # 增：参数按照旧在前新在后, 新表比旧表中的数据则为新增的数据
    # 删：参数按照新在前旧在后，旧表比新表多的数据则为删除的数据
    def self.compare_data old_db_path, new_db_path, changed_table, is_update = false
        old_table_names_string = `sqlite3 #{old_db_path} .tables`
        new_table_names_string = `sqlite3 #{new_db_path} .tables`
        old_table_names = self.parse_table_name_data old_table_names_string
        new_table_names = self.parse_table_name_data new_table_names_string
       
        # 新表表明的数组 - 旧表表明的数组 = 新增的表的表名
        # 该表中的所有数据都属于新增的wook sheet中的
        more_table_names = is_update ? [] : new_table_names - old_table_names
        compare_tables = changed_table        
        total_data = { 'ACL信息' => [], '转发区' => [], 'ICBC区' => [], '非ICBC区' => [] }
        
        # for test limit the tables
        # old_table_names = ['ah_view_auth_aeauh.icbc', 'acl_table', 'forwardStub_zone_table', 'ah_view_auth_ah.icbc', 'ah_view_auth_bebru.icbc', 'ah_view_auth_icbc']
        # more_table_names = []

        # old_table_names.each do |table_name|
        #     classified_table_name = self.classifiy_table_name table_name
        #     data = self.compare_create_for_table table_name, old_db_path, new_db_path, classified_table_name, is_update
        #     next if data.nil?
        #     total_data[classified_table_name] += data
        # end
        
        # 把表格分为十批执行
        threads = []
        compare_tables.each_slice(THREAD_NUM).each do |batch_datas|
            threads << Thread.new {
                batch_datas.each_with_index do |table_name, index|
                    classified_table_name = self.classifiy_table_name table_name
                    data = self.compare_create_for_table table_name, old_db_path + "_#{index}", new_db_path + "_#{index}", classified_table_name, is_update
                    next if data.nil?
                    total_data[classified_table_name] += data
                end
            }
        end
        # 等到所有的线程执行完成
        ThreadsWait.all_waits(*threads)

        more_table_names.each do |table_name|
            classified_table_name = self.classifiy_table_name table_name

            if classified_table_name.include? 'ICBC'
                data_string = `sqlite3 #{new_db_path} "select a.id, a.view, a.name, a.ttl, a.rdata from #{table_name};"`
                data = self.parse_data data_string, table_name
                next if data.nil?
                total_data[classified_table_name] += data                
            else
                next
            end

        end
        # 返回所有的数据
        total_data
    end
    
    def self.compare_create_for_table table_name, old_db_path, new_db_path, classified_table_name, is_update = false
        puts "comparing #{table_name}..."
    
        case classified_table_name
        when 'ACL信息'
            join_table_sql = "(select aa.name as view_name, aa.priority, bb.name, bb.networks from new_db.view aa inner join new_db.acl_table bb on aa.acl_names = bb.name)"            
            if is_update
                data_string = `sqlite3 #{old_db_path} "attach '#{new_db_path}' AS new_db; select a.view_name, a.priority, b.name, b.networks, a.view_name, a.priority, a.name, a.networks from #{join_table_sql} a inner join '#{table_name}' b on a.name = b.name where a.networks <> b.networks;"`      
            else
                 data_string = `sqlite3 #{old_db_path} "attach '#{new_db_path}' AS new_db; select a.view_name, a.priority, a.name, a.networks from #{join_table_sql} a left join '#{table_name}' b on a.name = b.name where b.name is null;"`               
            end
        when '转发区'
            if is_update
                data_string = `sqlite3 #{old_db_path} "attach '#{new_db_path}' AS new_db; select b.name, b.view, b.servers, b.forward_style, a.name, a.view, a.servers, a.forward_style from new_db.'#{table_name}' a inner join '#{table_name}' b on a.id = b.id where a.servers <> b.servers or a.forward_style <> b.forward_style;"`
               
            else
                data_string = `sqlite3 #{old_db_path} "attach '#{new_db_path}' AS new_db; select a.name, a.view, a.servers, a.forward_style from new_db.'#{table_name}' a left join '#{table_name}' b on a.id = b.id where b.id is null;"`
            end
        when /ICBC区|非ICBC区/
            if is_update
                data_string = `sqlite3 #{old_db_path} "attach '#{new_db_path}' AS new_db; select b.view, b.name, b.ttl, b.type, b.rdata, a.view, a.name, a.ttl, a.type, a.rdata from new_db.'#{table_name}' a inner join '#{table_name}' b on a.id = b.id where (a.ttl <> b.ttl or a.rdata <> b.rdata) and (b.type <> 'SOA' or a.type <> 'SOA');"`
            else
                data_string = `sqlite3 #{old_db_path} "attach '#{new_db_path}' AS new_db; select a.view, a.name, a.ttl, a.type, a.rdata from new_db.'#{table_name}' a left join '#{table_name}' b on a.id = b.id where b.id is null;"`
            end
        else
            return nil
        end

        puts "#{table_name} comparing complete!"

        data = self.parse_data data_string, table_name, is_update
        
        data
    end
        
    # 解析sqlite shell 命令返回的普通数据
    def self.parse_data data_string, table_name, is_update
        return nil if data_string.empty?
        classifiy_table_name = self.classifiy_table_name table_name
        data = data_string.split("\n").map do |data| 
            temp_data = data.split("|")
            if table_name.include? 'icbc'
                view_name = table_name.split('_')[0..1].join('_')
                temp_data[0] = view_name
                temp_data[5] = view_name if is_update
                if classifiy_table_name == '非ICBC区'
                    qw_area = table_name.split('_auth_')[-1]
                    temp_data.insert(1, qw_area)
                    temp_data.insert(7, qw_area) if is_update
                end
            end
            temp_data
        end
        data
    end

    # 解析sqlite shell 命令返回的表名的数据
    def self.parse_table_name_data data_string
        return nil if data_string.empty?
        data = data_string.split("\n").map { |table_names| table_names.split(' ') }.flatten
        data
    end
    
    # 根据表名的名字判断该表的类别
    # 表的类别如下
    # ICBC区
    # 非ICBC区
    # 转发区
    # ACL信息
    # 如果都匹配不上的则返回nil
    def self.classifiy_table_name table_name
        case table_name
        when 'acl_table'
            'ACL信息'
        when 'forwardStub_zone_table'
            '转发区'
        when /auth_icbc$/
            'ICBC区'
        else
            if (table_name.include? '_auth_') && (table_name.include? '.icbc')
                '非ICBC区'
            else
                nil
            end
        end
    end
end

if $0 == __FILE__ 
    old_file_path = ARGV[0]
    new_file_path = ARGV[1]
    
    # 参数文件检查
    if ARGV.size != 2 
        puts 'The script only accept two file path argument' 
        exit 1 
    end
    if !File.exist? old_file_path then 
        puts "#{old_file_path} is not exist!" 
        exit 1 
    end
    if !File.exist? new_file_path then 
        puts "#{new_file_path} is not exist!" 
        exit 1 
    end
    
    begin
        # 生成表格
        old_file_name = File.basename(old_file_path, File.extname(old_file_path))
        new_file_name = File.basename(new_file_path, File.extname(new_file_path))
        book = DataSheet.new(old_file_name, new_file_name).book
        # 得到db的路径
        old_db_path = CompareData.get_db_path old_file_path, old_file_name
        new_db_path = CompareData.get_db_path new_file_path, new_file_name
        
        CompareData.start_compare old_db_path, new_db_path, book
                
        puts 'generating doc...'
        book.write ("/tmp/analysis_#{old_file_name}.xls")
        puts "generating doc complete! go find in /tmp/analysis_#{old_file_name}.xls"
    rescue Exception => e
        puts e.to_s
        exit 1
    ensure
        temp_old_path = '/tmp/' + old_file_name
        temp_new_path = '/tmp/' + new_file_name
        # 删除临时文件
        puts "start delete temp files #{temp_new_path}..."
        puts "start delete temp files#{temp_old_path}..."
        
        `rm -rf #{temp_new_path}`
        `rm -rf #{temp_old_path}`
    end
end
