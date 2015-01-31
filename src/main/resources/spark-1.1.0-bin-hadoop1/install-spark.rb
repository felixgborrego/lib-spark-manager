#!/usr/bin/ruby                                                                                                                                                                                                                      
 
require 'json'
require 'emr/common'
require 'digest'
require 'socket'
 
def run(cmd)
  if ! system(cmd) then
    raise "Command failed: #{cmd}"
  end
end
 
def sudo(cmd)
  run("sudo #{cmd}")
end
 
def println(*args)
  print *args
  puts
end
 
def local_ip
  orig, Socket.do_not_reverse_lookup = Socket.do_not_reverse_lookup, true  # turn off reverse DNS resolution temporarily
  UDPSocket.open do |s|
    s.connect '64.233.187.99', 1
    s.addr.last
  end
  ensure
  Socket.do_not_reverse_lookup = orig
end
 
job_flow = Emr::JsonInfoFile.new('job-flow')
instance_info = Emr::JsonInfoFile.new('instance')
 
@hadoop_home="/home/hadoop"
@hadoop_apps="/home/hadoop/.versions"
 
@base_url="https://s3.amazonaws.com/gilt-rtd/artifacts"
@spark_version="1.1.0"
@scala_version="2.10.4"
@hadoop="hadoop1"
@local_dir= `mount`.split(' ').grep(/mnt/)[0] << "/spark/"
@hadoop_version= job_flow['hadoopVersion']
@is_master = instance_info['isMaster'].to_s == 'true'
@master_dns=job_flow['masterPrivateDnsName']
@master_ip=@is_master ? local_ip : `host #{@master_dns}`.scan(/[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/)[0]

def download_from_s3
  println "downloading spark from #{@base_url}/emr/spark/spark-#{@spark_version}-bin-#{@hadoop}.tgz"
  sudo "curl -L --silent --show-error --fail --connect-timeout 60 --max-time 720 --retry 5 -O  #{@base_url}/spark/spark-#{@spark_version}-bin-#{@hadoop}.tgz"
  println "downloading scala from #{@base_url}/scala/scala-#{@scala_version}.tgz"
  sudo "curl -L --silent --show-error --fail --connect-timeout 60 --max-time 720 --retry 5 -O  #{@base_url}/scala/scala-#{@scala_version}.tgz"
end
 
def untar_all
  sudo "tar xzf  spark-#{@spark_version}-bin-#{@hadoop}.tgz -C #{@hadoop_apps} && rm -f spark-#{@spark_version}-bin-#{@hadoop}.tgz"
  sudo "tar xzf  scala-#{@scala_version}.tgz -C #{@hadoop_apps} && rm -f scala-#{@scala_version}.tgz"
end
 
def create_symlinks
  sudo "ln -sf #{@hadoop_apps}/spark-#{@spark_version}-bin-#{@hadoop} #{@hadoop_home}/spark"
end
 
def write_to_bashrc
  println "Setting SCALA_HOME to: #{@hadoop_apps}/scala-#{@scala_version}"
  File.open('/home/hadoop/.bashrc','a') do |file_w|
  file_w.write("export SCALA_HOME=#{@hadoop_apps}/scala-#{@scala_version}")
  end
end
 
def mk_local_dir
  `mkdir #{@local_dir}`
end

def create_spark_env
  lzo_jar=Dir.glob("#{@hadoop_apps}/#{@hadoop_version}/share/**/hadoop-*lzo.jar")[0]
  if lzo_jar.nil?
    then
      lzo_jar=Dir.glob("#{@hadoop_apps}/#{@hadoop_version}/share/**/hadoop-*lzo*.jar")[0]
  end    
  if lzo_jar.nil?
    println "lzo not found inside #{@hadoop_apps}/#{@hadoop_version}/share/"
  end
  File.open('/tmp/spark-env.sh','w') do |file_w|
    file_w.write("export SPARK_MASTER_IP=#{@master_ip}\n")
    file_w.write("export SCALA_HOME=#{@hadoop_apps}/scala-#{@scala_version}\n")
    file_w.write("export SPARK_LOCAL_DIRS=#{@local_dir}\n")
    file_w.write("export SPARK_CLASSPATH=\"#{@hadoop_apps}/scala-#{@scala_version}/lib/scala-library.jar:#{@hadoop_apps}/scala-#{@scala_version}/lib/scala-reflect.jar:/usr/share/aws/emr/emr-fs/lib/*:/usr/share/aws/emr/lib/*:#{@hadoop_home}/share/hadoop/common/lib/*:#{lzo_jar}\"\n")
    file_w.write("export SPARK_DAEMON_JAVA_OPTS=\"-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps\"\n")
    file_w.write("export SPARK_LOG_DIR=\"/mnt/var/log/hadoop/spark/logs\"\n")
    file_w.write("export SPARK_WORKER_DIR=\"/mnt/var/log/hadoop/spark/work\"\n")
  end
  sudo "mv /tmp/spark-env.sh #{@hadoop_home}/spark/conf/spark-env.sh"
end
 
def create_spark_defaults
  File.open('/tmp/spark-defaults.conf','w') do |file_w|
    file_w.write("spark.master spark://#{@master_ip}:7077\n")
    file_w.write("spark.executor.memory 1g\n")
    file_w.write("spark.ui.port 8080\n")
    file_w.write("spark.ui.retainedStages 1000\n")
    file_w.write("spark.ui.killEnabled true\n")
  end
  sudo "mv /tmp/spark-defaults.conf #{@hadoop_home}/spark/conf/spark-defaults.conf"
 
end
  
def copy_files_to_spark
  core_site_xml=Dir.glob("#{@hadoop_home}/conf/**/core-site.xml")[0]
  sudo "cp #{core_site_xml} #{@hadoop_home}/spark/conf/"
end
 
def test_connection_with_master
  attempt=0
  until (system("nc -z #{@master_ip} 7077"))
    attempt += 1
    if attempt < 20
      then
        sleep(5)
    else
      break
    end
  end
  if attempt == 20
    then
      return false
  else
    return true
  end
end
 
println "Master DNS: #{@master_dns}"
println "Master IP: #{@master_ip}"

download_from_s3
untar_all
create_symlinks
mk_local_dir
create_spark_env
create_spark_defaults
copy_files_to_spark
  
if @is_master then
  println "Starting master using script at #{@hadoop_home}/spark/sbin/start-master.sh"
  sudo "#{@hadoop_home}/spark/sbin/start-master.sh"
else 
  if test_connection_with_master
    then
      sudo "#{@hadoop_home}/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://#{@master_ip}:7077 &"
    else 
      raise RuntimeError, 'Worker not able to connect to master'
  end
end