#!/usr/bin/ruby 

require 'open-uri'
require 'dbi'
require 'mysql2'
require 'securerandom'
require 'date'
require 'net/http'
require 'zipruby'

## YEAR VARIABLE
fec_year = "16"
fec_full_year = "20" + fec_year

# DB VARS
dbname = "XXXXX"
dbhost = "localhost"
dbport = "3306"
dbuser = "XXXXXXX"
dbpass = "XXXXXXXX"
mysql_login_path = "XXXXXXX"  # --login-path option new for MySQL 5.6 

client = Mysql2::Client.new(
  :host => dbhost, 
  :port => dbport, 
  :database => dbname,
  :username => dbuser, 
  :password => dbpass)

tables = [ "cm", "cn", "ccl", "oth", "pas2", "indiv", "oppexp" ]
fields = [ "CMTE_ID,CMTE_NM,TRES_NM,CMTE_ST1,CMTE_ST2,CMTE_CITY,CMTE_ST,CMTE_ZIP,CMTE_DSGN,CMTE_TP,CMTE_PTY_AFFILIATION,CMTE_FILING_FREQ,ORG_TP,CONNECTED_ORG_NM,CAND_ID",
"CAND_ID,CAND_NAME,CAND_PTY_AFFILIATION,CAND_ELECTION_YR,CAND_OFFICE_ST,CAND_OFFICE,CAND_OFFICE_DISTRICT,CAND_ICI,CAND_STATUS,CAND_PCC,CAND_ST1,CAND_ST2,CAND_CITY,CAND_ST,CAND_ZIP",
"CAND_ID,CAND_ELECTION_YR,FEC_ELECTION_YR,CMTE_ID,CMTE_TP,CMTE_DSGN,LINKAGE_ID",
"CMTE_ID,AMNDT_IND,RPT_TP,TRANSACTION_PGI,IMAGE_NUM,TRANSACTION_TP,ENTITY_TP,NAME,CITY,STATE,ZIP_CODE,EMPLOYER,OCCUPATION,@var1,TRANSACTION_AMT,OTHER_ID,TRAN_ID,FILE_NUM,MEMO_CD,MEMO_TEXT,SUB_ID",
"CMTE_ID,AMNDT_IND,RPT_TP,TRANSACTION_PGI,IMAGE_NUM,TRANSACTION_TP,ENTITY_TP,NAME,CITY,STATE,ZIP_CODE,EMPLOYER,OCCUPATION,@var1,TRANSACTION_AMT,OTHER_ID,CAND_ID,TRAN_ID,FILE_NUM,MEMO_CD,MEMO_TEXT,SUB_ID",
"CMTE_ID,AMNDT_IND,RPT_TP,TRANSACTION_PGI,IMAGE_NUM,TRANSACTION_TP,ENTITY_TP,NAME,CITY,STATE,ZIP_CODE,EMPLOYER,OCCUPATION,@var1,TRANSACTION_AMT,OTHER_ID,TRAN_ID,FILE_NUM,MEMO_CD,MEMO_TEXT,SUB_ID",
"CMTE_ID,AMNDT_IND,RPT_YR,RPT_TP,IMAGE_NUM,LINE_NUM,FORM_TP_CD,SCHED_TP_CD,NAME,CITY,STATE,ZIP_CODE,@var1,TRANSACTION_AMT,TRANSACTION_PGI,PURPOSE,CATEGORY,CATEGORY_DESC,MEMO_CD,MEMO_TEXT,ENTITY_TP,SUB_ID,FILE_NUM,TRAN_ID,BACK_REF_TRAN_ID"
 ]
transformations = [ "", "", "", ", transaction_dt = STR_TO_DATE(replace(@var1, '/', ''), '%m%d%Y')", ", transaction_dt = STR_TO_DATE(replace(@var1, '/', ''), '%m%d%Y')", ", transaction_dt = STR_TO_DATE(replace(@var1, '/', ''), '%m%d%Y')", ", transaction_dt = STR_TO_DATE(replace(@var1, '/', ''), '%m%d%Y')" ]
 
counter = 0

tables.each do|t|

	if (counter >= 0) 

		url = "ftp://ftp.fec.gov/FEC/" + fec_full_year + "/" + t + fec_year + ".zip"	
		file_temp_suffix =  SecureRandom.hex.to_s
		file_save_path = "/tmp/" + file_temp_suffix + ".fecftp"	

		zipbytes = open(url).read
		Zip::Archive.open_buffer(zipbytes) do |zf|
			zf.fopen(zf.get_name(0)) do |f|
			  unzipped = f.read
			  unzipped = unzipped.gsub("'", "\\\\'")
				File.open(file_save_path, 'w') { |file| 
					file.write(unzipped) }
			end
		end	
		
		# Make sure we're not importing a blank file
		line_count = File.foreach(file_save_path).inject(0) {|c, line| c+1}
		
		load_command = "/usr/bin/mysql --login-path=#{mysql_login_path} --host=#{dbhost} --database=#{dbname} -e \"LOAD DATA LOCAL INFILE '#{file_save_path}' INTO TABLE #{t} FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (#{fields[counter]}) SET fec_period = #{fec_full_year} #{transformations[counter]};\""

		# delete old data, import new data
		if (line_count > 0)
			begin
				sql_delete = "DELETE FROM #{t} WHERE fec_period = '#{fec_full_year}'"
				res = client.query(sql_delete)	
				system(load_command)				
			rescue Exception => e  
				puts e
			end	
		end
		
		# delete temp file
		File.delete(file_save_path)
	end
	counter = counter + 1
end

