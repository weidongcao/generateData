#!/bin/bash
# 功能说明:
# 导入数据到ClickHouse
# 并实现以下功能:
#   1. 启动Java程序生成数据
#   2. 可以传参指定ClickHouse及程序相关配置
#   2. 打印日志到同名.log文件下
#   3. 导入数据到ClickHouse
#   4. 多线程导入数据以提高导入速度
#
# 导入数据到ClickHouse,逻辑如下:
# 1. while循环判断导入程序是否需要结束, 程序结束的判断标准是:
#       1. Java生成数据的线程是否结束
#       2. 数据目录下是否还有数据文件
# 2. 通过find命令获取数据目录下指定时间段的数据文件,循环导入所有文件
#       1. 读取1个线程令牌以创建1个子线程,如果没有令牌等待
#       2. 执行导入命令
#       3. 判断是否导入成功
#           1. 成功 --> 删除数据文件
#           2. 失败 --> 提示错误
# 4. 导入所有find出来的文件后,等待所有子线程执行完毕
# 5. 再次判断Java线程是否结束及数据目录下是否还有数据文件
# 6. 进行下一次循环
#
# 7. 关闭线程流及令牌
# 8. 删除数据目录
#
# 命令行传参
# -h|--host:ClickHouse-Server的IP
# -p|--port:ClickHouse的端口
# -u|--username:ClickHouse的用户名
# -P|--password:ClickHouse的密码
# -t|--tablename:表名
# -d|--data_path:DNS解析日志的路径
# -m|--minutes:导入修改时间从当前时间向前推多少分钟的数据
# -n|--thread_num:启动多少个线程跑任务
# --help:使用帮助
#
# 例子：
# sh mantual_import.sh --host=192.168.6.126 --port=9800 --thread_num=3
# sh mantual_import.sh -h 192.168.6.126 -p 9800 -n 3
# 两个命令的意思是一样的，
# 意为： 指定ClickHouse主机为192.168.6.126，指定端口号为9800，指定最多同时执行3个线程

############################# 打印变量值,调试时打开 ############################
#set -e
#set -x

function get_timestamp(){
    echo $(($(date +%s%N) / 1000000))
}

# 打印日志
function logger(){
    echo "$(date "+%Y-%m-%d %H:%M:%S") [INFO] ${1}"
    # 写入日志文件, 日志文件为与脚本名相同, 后缀为.log
    echo "$(date "+%Y-%m-%d %H:%M:%S") [INFO] ${1}" >> "${0/sh/log}"
}

################################## 初始化变量 ##################################
# 当前路径
current_path="$(cd "$( dirname "$0"  )" && pwd)"
# 脚本主线程PID作为数据生成目录
main_pid="$$"
# 数据目录(随机生成)
data_path="${current_path}/${main_pid}"

# ClickHouse-Server所在服务器
host="192.168.1.231"
# ClickHouse端口
port="9000"
# ClickHouse用户名
username="data_engine"
# ClickHouse密码
password="DLKUc4M.ZwD%Q7"
# DNS解析日志导入的数据库表
tablename="src.src_dns_logs_cache"
# 数据入库频率（分钟）
minutes_ago=-3
# 最大线程数
thread_num=3
# 导入的数据量(万)
data_count=1800
# 1个数据文件中的数据量(万)
fileDataCount=50
# Java生成数据的程序main方法
java_main_class="com.yamu.AppSpecifyDataCount"

# 获取当前时间毫秒
#将current转换为时间戳，精确到秒
#将current转换为时间戳，精确到毫秒

################################## 解析命令行传的参数 ##################################
# getopt指定命令行可以给脚本传哪些参数
# -a 为短选项(不需要值) ,这里没有定义
# -o 为短选项(需要值), 这里定义了-h, -p, -u, -P等等
# --long 为长选项需要值, 这里定义了--host, --port, --username, --password等等,
ARGS=$(getopt -a -o h:p:u:P:t:m:d:n:c:a: --long host:,port:,username:,password:,tablename:,minutes:,data_path:,thread_num:,data_count:,file_data:,app:,help -- "$@")

# 重排选项
if [ $? != 0 ];then
    echo "Terminating..."
    exit 1
fi


eval set -- "${ARGS}"


# 解析命令行参数
while :
do
    case "$1" in
        -h|--host)
            host=${2}
            shift
            ;;
        -p|--port)
            port=${2}
            shift
            ;;
        -u|--username)
            username=${2}
            shift
            ;;
        -P|--password)
            password=${2}
            shift
            ;;
        -t|--tablename)
            #tablename=${2:-"src.wedotest"}
            tablename=${2}
            shift
            ;;
        -m|--minutes)
            minutes_ago=${2}
            shift
            ;;
        -n|--thread_num)
            thread_num=${2}
            shift
            ;;
        --file_data)
            fileDataCount=${2}
            shift
            ;;
        -c|--data_count)
            data_count=${2}
            shift
            ;;
        -d|--data_path)
            data_path=${2}
            shift
            ;;
        -a|--app)
            java_main_class=${2}
            shift
            ;;
        --help)
            usage
            ;;
        --)
            shift
            break
            ;;
        *)
            logger "thread-main $1"
            logger "thread-main Internal error!"
            exit 1
            ;;
    esac
        shift
done
logger "thread-main ClickHouse host: ${host}"
logger "thread-main ClickHouse port: ${port}"
logger "thread-main ClickHouse username: ${username}"
logger "thread-main ClickHouse password: *********"
logger "thread-main ClickHouse tablename: ${tablename}"
logger "thread-main ClickHouse import data from ${minutes_ago} minutes"
logger "thread-main thread_num : ${thread_num}"
logger "thread-main data path: ${data_path}"


############################## 启动Java程序生成数据 ##############################
# 判断数据目录是否存在
if [[ ! -d "{data_path}" ]];then
    logger "thread-main data path does not exist, I will create it"
    mkdir -p "${data_path}"
fi


# 执行后台任务时需要加上nohup，不然控制台断开的话会异常结束
# 执行前台任务的时候最好不要加nohup，不然控制台不会打印Java程序日志
#nohup java  -Xms2g -Xmx4g -classpath "${current_path}"/demo-1.0-SNAPSHOT-jar-with-dependencies.jar ${java_main_class} "${data_path}" ${fileDataCount} "${data_count}"  &
java  -Xms2g -Xmx4g -classpath "${current_path}/generageData-1.0-SNAPSHOT-jar-with-dependencies.jar" ${java_main_class} "${data_path}" ${fileDataCount} "${data_count}"  &
logger "thread-main start generate data Java program: ${java_main_class}, pid: $!"

################################## 初始化多线程 ##################################
temp_fifofile="$$.fifo"
mkfifo $temp_fifofile

# 将fd6指向fifo类型
# 使文件描述符为非阻塞式
exec 6<>$temp_fifofile
rm $temp_fifofile

#根据线程总数量设置令牌个数
for ((i = 0;i < thread_num;i++));do
    echo
done >&6
logger "thread-main multiple thread init finish, thread number: ${thread_num}"


############################### 数据导入ClickHouse ###############################
# 先统计数据目录下最近一段时间是否有数据文件
file_num=$(find "${data_path}" -name "*.gz" -cmin "${minutes_ago}" 2> /dev/null | wc -l)

# 判断Java生成数据程序是否结束(排除grep进程)
java_thread=$(ps aux | grep "${java_main_class}" | grep -v grep)

# 导入数据到ClickHouse,逻辑如下:
# 1. while循环判断导入程序是否需要结束, 程序结束的判断标准是:
#       1. Java生成数据的线程是否结束
#       2. 数据目录下是否还有数据文件
# 2. 通过find命令获取数据目录下指定时间段的数据文件,循环导入所有文件
#       1. 读取1个线程令牌以创建1个子线程,如果没有令牌等待
#       2. 执行导入命令
#       3. 判断是否导入成功
#           1. 成功 --> 删除数据文件
#           2. 失败 --> 提示错误
# 4. 导入所有find出来的文件后,等待所有子线程执行完毕
# 5. 再次判断Java线程是否结束及数据目录下是否还有数据文件
# 6. 进行下一次循环
#
# 7. 关闭线程流及令牌
# 8. 删除数据目录
import_start_time=0
while [[ -n "${java_thread}" ]] || [[ $file_num -gt 0 ]]
do
    if [[ ${file_num} -ne 0 ]]; then
        logger "thread-main --> ${data_path} find ${file_num} files last $((0 - minutes_ago)) minutes, I will import them"
    else
        logger "thread-main --> ${data_path} find ${file_num} files last $((0 - minutes_ago)) minutes, I will sleep 2 seconds"
        if [[ $import_start_time -eq 0 ]]; then
            import_start_time=$(get_timestamp)
        fi
    fi

    # 创建数组, 存储所有执行导入的子线程
    subprocess_pids=()

    # 获取数据目录下最近多少分钟修改过的数据文件
    for file in $(find "${data_path}" -name "*.gz" -cmin "${minutes_ago}" | sort) ; do

        # 获取1个令牌
        # 一个read -u6命令执行一次，就从fd6中减去一个回车符，然后向下执行，
        # fd6中没有回车符的时候，就停在这了，从而实现了线程数量控制
        read -u6

        # 创建子线程执行导入
        {
            # 线程号
            thread_id="thread-$(printf '%04d' $((RANDOM % 10000)))"

            # 文件大小,如果没有达到指定大小说明文件还没有写完
            file_size=$(ls -l ${file} | awk '{ print $5 }')

            # 判断数据文件是否存在及是不是写入完毕(大小不为0)
            if [[ -s "${file}" ]] && [[ ${file_size} -gt 10000000 ]]; then

                # 导入ClickHouse命令模板
                import_cmd="zcat ${file} | clickhouse-client -u ${username} --password ${password} --format_csv_delimiter='|' --date_time_input_format=best_effort --query='INSERT INTO ${tablename}(src_ip, domain_name, parse_timestamp, a_record, rcode, qtype, cname, aaaa_record, business_ip) FORMAT CSV' -h ${host} --port=${port} --max_partitions_per_insert_block=4096 >> ${0/sh/log} 2>&1"

                # 该线程导入开始时间，用以记录线程执行时间
                start_time=$(get_timestamp)

                # 执行导入命令
                eval "${import_cmd}"
                #sleep 3

                # 判断是否导入成功，导入成功则删除数据文件,不成功仅提示错误
                if [ $? -eq 0 ]; then
                    # 记录导入命令执行时间
                    import_time=$(get_timestamp)
                    logger "${thread_id} import success, execution time:  $(printf '%5d' $((import_time - start_time))) millisecond, data file: ${file##*/}"

                    # 删除数据文件命令模板
                    rm_cmd="rm -f ${file} "

                    # 删除导入成功的数据文件
                    eval "${rm_cmd}"

                    # 记录删除命令执行时间
                    mv_time=$(get_timestamp)
                    # logger "${thread_id} delete file: time consume: $((mv_time - import_time)), cmd: ${rm_cmd}"
                else
                    # 导入失败，提示错误
                    logger "${thread_id} DNS日志导入ClickHouse失败,命令:${import_cmd}"
                    sleep 10
                    #exit -1
                fi
            else
                # 提示日志文件还没有写完，
                logger "${thread_id} skip import, data file not write complete: file size: ${file_size}, file name: ${file##*/}"
            fi

            # 当进程结束以后返还令牌
            echo "" >&6
        } &

        # 记录子线程pid
        subprocess_pids[${#subprocess_pids[*]}]=$!
    done
    logger "thread-main finish import find files ..."

    # 判断数组中是否有执行导入的子线程
    if [[ ${#subprocess_pids[*]} -ne 0 ]]; then
        # 如果有的话等待所有子线程完成
        logger "thread-main wait for subprocess finish..."
        for pid in ${subprocess_pids[*]}; do
            wait ${pid}
        done
    else
        # 如果没有的话sleep 2秒,防止等待过程中打印过多的日志
        logger "thread-main no subprocess running sleep 2 second"
        sleep 2
    fi

    # 再次判断Java程序是否结束
    java_thread=$(ps aux | grep "${java_main_class}" | grep -v grep)

    # 获取新生成的日志文件数量
    file_num=$(find "${data_path}" -name "*.gz" -cmin "${minutes_ago}" 2> /dev/null | wc -l)
    unset subprocess_pids
done
import_end_time=$(get_timestamp)
echo "import time comsume: $(( import_end_time - import_start_time ))"

# 等待所有子线程结束
wait

# 删除线程流及令牌
exec 6>&-

# 删除数据目录
rm -rf "${data_path}"
logger "thread-main finish, I will delete data directory: ${data_path}"

logger "thread-main --> over"

