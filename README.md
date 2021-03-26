# mdsyncer
###异构模型&amp;数据同步工具

###现支持：Oracle \ MySQL \ PostgreSQL间的模型转换和数据同步

####数据同步封装DataX 实现自动化配置
####备注：DataX是阿里的一款开源ETL工具

##操作
* 第一步： <br />
    配置源库和目标库信息，配置文件相对路径$MDSYNCER_HOME/src/main/mdsyncer/conf/dbParams/dbconf.xml
  #### 格式如下
  
  ```
   <auth id="ORACLE_172.21.86.201">
        <dbtype>ORACLE</dbtype>
        <host>172.21.86.201</host>
        <port>1521</port>
        <user>scott</user>
        <passwd>UPscGj8PuIBzZEvuJqdiMz/u0mbrxwVQh06nTFIM3Esg0tD2B0iADAa6Zf0MGescBtPzpbVdoRolX7+Ce3+D6A==</passwd>
        <dbname>ORCL</dbname>
    </auth>
  ```
  #### 参数说明

  * **auth_id**
  	* 描述：数据库配置信息的认证名称，格式建议：数据库类型_服务器IP(_用户名) <br />
  	* 注意：特别当存在同一个服务器数据库类型一致，用户名不一致的建议添加用户名以便区分
  	  
  * **dbtype**	   
    * 描述：数据库类型，格式大写，例如 ORACLE\MYSQL\POSTGRESQL等 <br />

  * **host**	   
    * 描述：数据库服务器IP地址<br />

  * **port**	   
    * 描述：数据库端口<br />
    
  * **user**	   
    * 描述：数据库用户名<br />
    
  * **passwd**	   
    * 描述：数据库名用户密码，需要进行加密操作，通过调用mdtool中的Crypter.encrypt方法进行32位的密码加密<br />
    * 加密： python mdtool -ep "未加密的字符串信息" or  ./mdtool -ep "未加密的字符串信息"
    * 解密： python mdtool -dp "加密的字符串信息"  or  ./mdtool -ep "加密的字符串信息"
    
  * **dbname**	   
    * 描述：数据库服务名，根据不同类型的数据库填写，ORACLE数据库填写SID或者SERVICE_NAME，MYSQL/POSTGRESQL填写DATABASE<br />
  
  #### 注意
  * 配置文件中认证名称为MGR_172.21.86.205的数据库配置信息，作为管理库，需要保留切不可变动
  ```
  <auth id="MGR_172.21.86.205">
        <dbtype>MYSQL</dbtype>
        <host>172.21.86.205</host>
        <port>3306</port>
        <user>root</user>
        <passwd>UPscGj8PuIBzZEvuJqdiMz/u0mbrxwVQh06nTFIM3Esg0tD2B0iADAa6Zf0MGescBtPzpbVdoRolX7+Ce3+D6A==</passwd>
        <dbname>dbsyncer</dbname>
    </auth>
  ```  
  
* 第二步： <br />
    模型同步，只支持ORACLE/MYSQL/POSTGRESQL异构模型同步
    1. 模型同步方法介绍<br />
    2. 配置需要同步的表信息，配置文件相对路径$MDSYNCER_HOME/src/main/mdsyncer/conf/sample_tab.lst<br />
    #### 模型同步参数说明
    1.  modelsyncer 支持单表，少量表参数模式的模型同步操作 <br />
    * **dbsrc**	   
        * 描述：模型需要转换的数据库信息  <br />
        * 必填：是 <br />
    * **dbtag**	   
        * 描述：模型转换后的数据库信息  <br />
        * 必填：是 <br />
    * **dbmgr**	   
        * 描述：mdsyncer工具管理数据库信息  <br /> 
        * 必填：是 <br />
    * **flag**	   
        * 描述：表对象、约束、索引等可选，以逗号为分隔符 格式：table,constraint,index  <br />
        * 必填：否 <br />
        * 默认值：同步模型包括：表、约束以及索引对象 <br />
    * **tables_in**	   
        * 描述：按表对象导出，以逗号为分隔符 格式：tab1,tab2,tab3  <br />
        * 必填：否 <br />
        * 默认值：全表 <br />
    * **dbin**	   
        * 描述：是否直接加载到DB: Y生成模型文件且加载到DB;N生成模型文件不加载到DB <br />
        * 必填：否 <br />
        * 默认值：N <br />
  
    2.  modelsyncer_tabdb 支持单表和少量表参数；支持表数量较多时的模型同步操作 <br />
    * **dbsrc**	   
        * 描述：模型需要转换的数据库信息  <br />
        * 必填：是 <br />
    * **dbtag**	   
        * 描述：模型转换后的数据库信息  <br />
        * 必填：是 <br />
    * **dbmgr**	   
        * 描述：mdsyncer工具管理数据库信息  <br /> 
        * 必填：是 <br />
    * **flag**	   
        * 描述：表对象、约束、索引等可选，以逗号为分隔符 格式：table,constraint,index  <br />
        * 必填：否 <br />
        * 默认值：同步模型包括：表、约束以及索引对象 <br />
    * **dbin**	   
        * 描述：是否直接加载到DB: Y生成模型文件且加载到DB;N生成模型文件不加载到DB <br />
        * 必填：否 <br />
        * 默认值：N <br />
    * **tabfile_flag**	   
        * 描述：是否读取文件方式批量导出表对象: Y读取文件，N不用读取  <br />
        * 必填：否 <br />
        * 默认值：N <br />   
    * **tabfile**	   
        * 描述：需要同步的表配置文件名称,配合tabfile_flag参数一起使用  <br />
        * 必填：否 <br />
        
    #### 模型同步注意
        1. 需要同步的表数量较少（限定10以内），不通过配置文件，可以直接通过参数调用实现<br />
        调用方式：python modelsyncer.py --dbsrc ORACLE_172.21.86.201 --dbtag MYSQL_172.21.86.205 --dbmgr MGR_172.21.86.205  --tables_in rme_card,rme_port --dbin N

        2. 配置文件的方式实现；<br />
        调用方式：python modelsyncer_tabdb.py --dbsrc ORACLE_172.21.86.201 --dbtag MYSQL_172.21.86.205 --dbmgr MGR_172.21.86.205 --dbin N --tabfile_flag Y --tabfile sample_tab.lst

        ps:外键问题，如果模型直接同步到目标数据库，外键同模型一道完成，对后续的数据同步存在影响，故建议模型同步不直接加载到数据库，同时手动加载模型的时候，将外键语句保留，当数据同步完成后，在执行外键操作。
    
    #### 模型同步结果
        1. 表对象SQL语句 $MDSYNCER_HOME/src/main/mdsyncer/result/tables_generator<br />
        2. 约束对象SQL语句 $MDSYNCER_HOME/src/main/mdsyncer/result/constraints_generator<br />
        3. 索引对象SQL语句 $MDSYNCER_HOME/src/main/mdsyncer/result/indexes_generator<br />
 
    
* 第三步： <br />
    数据同步，支持ORACLE/MYSQL/POSTGRESQL间的数据同步
    1. 模型同步方法介绍<br />
    2. 配置同模型同步配置<br />
    #### 数据同步参数说明
    * **dbsrc**	   
        * 描述：模型需要转换的数据库信息  <br />
        * 必填：是 <br />
    * **dbtag**	   
        * 描述：模型转换后的数据库信息  <br />
        * 必填：是 <br />   
    * **tabfile**	   
        * 描述：需要同步的表配置文件名称  <br />
        * 必填：否 <br />
    * **dbin**	   
        * 描述：是否直接加载到DB: Y生成datax配置文件且加载到目标DB;N只生成datax配置的json文件；默认不直接加载 <br />
        * 必填：否 <br />
        * 默认值：N <br />
        
     #### 调用方式
     python datasyncer.py --dbsrc ORACLE_10.45.59.246 --dbtag POSTGRESQL_172.21.86.201 --tabfile sample_tab.lst
     
    #### 数据同步结果
        1. json配置文件目录 $MDSYNCER_HOME/src/main/mdsyncer/job
        2. 同步日志信息 $DATAX_HOME/log/yyyy-mm-dd
        
* 第四步： <br />
    数据同步完成后的日志校验
    * 调用方式 $MDSYNCER_HOME/src/main/script/checklog.sh <br />
        * 参数yyyy-mm-dd <br />
        * 默认值：当天日期

* 第五步： <br />
    存在外键的模型，数据同步完成后需要执行外键SQL语句


### 注意
为了避免操作的复杂性，通过shell进行封装，简单化处理。
* 调用方式
    执行$MDSYNCER_HOME/src/main/script目录下的脚本
    sh mdsyncer_job.sh dbsrc dbtag dbmgr tabfile
    #### 数据同步参数说明
    * **dbsrc**	   
        * 描述：模型需要转换的数据库信息  <br />
        * 必填：是 <br />
    * **dbtag**	   
        * 描述：模型转换后的数据库信息  <br />
        * 必填：是 <br />   
    * **dbmgr**	   
        * 描述：mdsyncer工具管理数据库信息  <br /> 
        * 必填：是 <br />
    * **tabfile**	   
        * 描述：需要同步的表配置文件名称  <br />
        * 必填：是 <br />  
    
* 举例说明：sh mdsyncer_job.sh ORACLE_172.21.86.201 POSTGRESQL_172.21.86.201 MGR_172.21.86.205 sample_tab.lst
    
    